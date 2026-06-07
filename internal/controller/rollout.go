/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// Proactive rolling restart (ADR 0004), Replication topology.
//
// With spec/operator opt-in the StatefulSet uses updateStrategy: OnDelete and
// the operator drives the rollout itself: roll the replicas first (a replica
// restart needs no failover), then for the primary promote an up-to-date
// replica BEFORE deleting the old primary pod — so the unavailability window on
// a config rollout shrinks to ~0 instead of the reactive ~15-20s.
//
// Staleness is keyed off the StatefulSet's revision, exactly as the STS
// controller decides it for a RollingUpdate: a pod is "stale" when its
// `controller-revision-hash` label differs from the STS's
// `.status.updateRevision`. That captures ANY pod-template change (config-hash,
// the restart token, image, resources) — not just config — so opting into
// OnDelete never silently swallows a restart request. No separate cursor is
// needed and the loop is naturally resumable: each reconcile recomputes the next
// step from the live pods.

// proactiveRolloutEnabled reports whether a cluster opted into ADR 0004
// proactive rolling restart via the `valkey.wellcake.io/proactive-rollout: "true"`
// annotation.
func proactiveRolloutEnabled(vc *cachev1beta1.ValkeyCluster) bool {
	v, err := strconv.ParseBool(vc.Annotations[proactiveRolloutAnnotation])
	return err == nil && v
}

type rolloutPod struct {
	name     string
	ordinal  int32
	revision string // controller-revision-hash label
	ready    bool
}

type rolloutKind int

const (
	rolloutNone        rolloutKind = iota // every pod is on the desired revision
	rolloutWait                           // a roll is in flight (a pod not Ready) — wait
	rolloutRollReplica                    // delete this stale replica to recreate it on the new revision
	rolloutPromote                        // only the primary is stale — promote a replica, then delete it
)

type rolloutAction struct {
	kind rolloutKind
	pod  string // target pod name (rollReplica: the replica to delete; promote: the stale primary)
}

// nextRolloutStep is the pure decision: given the live pods, the desired STS
// revision and the current primary, what is the next rollout action? Replicas
// roll first, the primary last (and only via promote). A pod that is not Ready
// means a previous step is still settling — wait. Extracted for unit testing.
func nextRolloutStep(pods []rolloutPod, desiredRevision, primary string) rolloutAction {
	if len(pods) == 0 || desiredRevision == "" {
		return rolloutAction{kind: rolloutNone}
	}
	var stale []rolloutPod
	allReady := true
	for _, p := range pods {
		if !p.ready {
			allReady = false
		}
		if p.revision != desiredRevision {
			stale = append(stale, p)
		}
	}
	if len(stale) == 0 {
		return rolloutAction{kind: rolloutNone}
	}
	// Never roll another pod while the set is still converging — one at a time.
	if !allReady {
		return rolloutAction{kind: rolloutWait}
	}
	// Roll a stale REPLICA first (lowest ordinal among non-primary stale pods).
	sort.Slice(stale, func(i, j int) bool { return stale[i].ordinal < stale[j].ordinal })
	for _, p := range stale {
		if p.name != primary {
			return rolloutAction{kind: rolloutRollReplica, pod: p.name}
		}
	}
	// Only the primary is left stale → promote a replica, then delete it.
	return rolloutAction{kind: rolloutPromote, pod: primary}
}

// driveReplicationRollout advances the operator-driven rollout by one step.
// Returns the pod that should be considered primary after this call and whether
// a rollout is in progress (caller should requeue soon and skip reactive
// failover while true, so the two don't fight over the primary).
func (r *ValkeyClusterReconciler) driveReplicationRollout(
	ctx context.Context, vc *cachev1beta1.ValkeyCluster, password string, survey []podRole,
) (primary string, inProgress bool, err error) {
	log := logf.FromContext(ctx)
	primary = vc.Status.Primary

	// Re-Get the StatefulSet for the freshest .status.updateRevision: the STS
	// controller recomputes it asynchronously after we update the template, so the
	// copy ensureStatefulSet returned may still carry the previous revision.
	var sts appsv1.StatefulSet
	if err := r.Get(ctx, client.ObjectKey{Namespace: vc.Namespace, Name: statefulSetName(vc)}, &sts); err != nil {
		return primary, false, err
	}
	desiredRevision := sts.Status.UpdateRevision
	if desiredRevision == "" {
		return primary, false, nil
	}

	pods, err := r.listRolloutPods(ctx, vc)
	if err != nil {
		return primary, false, err
	}
	// Wait until the full set exists before reasoning about a rollout.
	if int32(len(pods)) < totalReplicas(vc) {
		return primary, true, nil
	}

	action := nextRolloutStep(pods, desiredRevision, primary)
	switch action.kind {
	case rolloutNone:
		return primary, false, nil
	case rolloutWait:
		return primary, true, nil
	case rolloutRollReplica:
		log.Info("proactive rollout: rolling replica", "name", vc.Name, "pod", action.pod, "revision", desiredRevision)
		if err := r.deletePodByName(ctx, vc, action.pod); err != nil {
			return primary, true, err
		}
		return primary, true, nil
	case rolloutPromote:
		// Proactive failover: promote the most up-to-date reachable replica that
		// is already on the new revision, THEN delete the old primary pod.
		port := valkeyPort
		if tlsEnabled(vc) {
			port = valkeyTLSPort
		}
		target := bestRolloutTarget(survey, pods, desiredRevision, primary)
		if target == nil {
			// No suitable replica yet (still syncing) — wait.
			return primary, true, nil
		}
		log.Info("proactive rollout: promoting replica before restarting primary",
			"name", vc.Name, "oldPrimary", primary, "newPrimary", podName(vc, target.ordinal))
		newPrimary := r.promoteReplica(ctx, vc, password, survey, target, port, log)
		if err := r.deletePodByName(ctx, vc, action.pod); err != nil {
			return newPrimary, true, err
		}
		return newPrimary, true, nil
	}
	return primary, false, nil
}

// bestRolloutTarget picks the replica to promote when restarting the primary:
// a reachable, up-to-date (already on the new revision) replica, by highest offset.
func bestRolloutTarget(survey []podRole, pods []rolloutPod, desiredRevision, primary string) *podRole {
	freshReady := map[int32]bool{}
	for _, p := range pods {
		if p.revision == desiredRevision && p.ready && p.name != primary {
			freshReady[p.ordinal] = true
		}
	}
	var best *podRole
	for i := range survey {
		s := survey[i]
		if !s.reachable || s.role != roleSlave || !freshReady[s.ordinal] {
			continue
		}
		if best == nil || s.offset > best.offset {
			b := s
			best = &b
		}
	}
	return best
}

func (r *ValkeyClusterReconciler) listRolloutPods(ctx context.Context, vc *cachev1beta1.ValkeyCluster) ([]rolloutPod, error) {
	var list corev1.PodList
	if err := r.List(ctx, &list, client.InNamespace(vc.Namespace),
		client.MatchingLabels{instanceLabel: vc.Name}); err != nil {
		return nil, err
	}
	sts := statefulSetName(vc)
	out := make([]rolloutPod, 0, len(list.Items))
	for i := range list.Items {
		p := &list.Items[i]
		if p.DeletionTimestamp != nil {
			continue
		}
		ord, ok := ordinalFromPodName(p.Name, sts)
		if !ok {
			continue
		}
		out = append(out, rolloutPod{
			name:     p.Name,
			ordinal:  ord,
			revision: p.Labels[appsv1.ControllerRevisionHashLabelKey],
			ready:    podIsReady(p),
		})
	}
	return out, nil
}

func (r *ValkeyClusterReconciler) deletePodByName(ctx context.Context, vc *cachev1beta1.ValkeyCluster, name string) error {
	pod := &corev1.Pod{}
	pod.Namespace = vc.Namespace
	pod.Name = name
	if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func ordinalFromPodName(name, sts string) (int32, bool) {
	suffix := strings.TrimPrefix(name, sts+"-")
	if suffix == name {
		return 0, false
	}
	n, err := strconv.ParseInt(suffix, 10, 32)
	if err != nil {
		return 0, false
	}
	return int32(n), true
}

func podIsReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// --- Proactive rolling restart (ADR 0004), Cluster topology ---
//
// Same staleness signal (STS revision) but rolled shard-aware: a shard's
// replicas roll first (gossip rejoins, no slot owner touched), then the shard's
// master is handed over via CLUSTER FAILOVER to an already-fresh replica BEFORE
// the old master pod is restarted. CLUSTER FAILOVER is a no-data-loss handover
// (master pauses writes, replica catches up, then takes the slots), so a config
// rollout of a sharded HA cluster keeps every slot continuously served. One node
// is disrupted at a time across the whole cluster.

// clusterShard is the minimal per-shard view the cluster rollout reasons over,
// projected from surveyCluster's ShardStatus (primary pod + replica pods).
type clusterShard struct {
	primary  string
	replicas []string
}

type clusterRolloutKind int

const (
	clusterRolloutNone        clusterRolloutKind = iota // every pod on the desired revision
	clusterRolloutWait                                  // a roll/failover is settling — wait
	clusterRolloutRollReplica                           // delete this stale replica pod
	clusterRolloutFailover                              // CLUSTER FAILOVER on this fresh replica (promote it)
	clusterRolloutRollMaster                            // 0-replica shard: delete the stale master directly
)

type clusterRolloutAction struct {
	kind clusterRolloutKind
	pod  string
}

// nextClusterRolloutStep is the pure decision for the Cluster rollout. Replicas
// (across all shards) roll first; once a shard's replicas are all fresh, its
// stale master is failed over to a fresh replica (which demotes the old master
// to a replica — picked up as a stale replica on the next pass and deleted). A
// shard with no replicas at all falls back to deleting the master directly
// (brief slot unavailability — there's nothing to fail over to). Extracted for
// unit testing.
func nextClusterRolloutStep(pods []rolloutPod, shards []clusterShard, desiredRevision string) clusterRolloutAction {
	if len(pods) == 0 || desiredRevision == "" || len(shards) == 0 {
		return clusterRolloutAction{kind: clusterRolloutNone}
	}
	rev := make(map[string]string, len(pods))
	ready := make(map[string]bool, len(pods))
	anyStale := false
	for _, p := range pods {
		rev[p.name] = p.revision
		ready[p.name] = p.ready
		if p.revision != desiredRevision {
			anyStale = true
		}
	}
	if !anyStale {
		return clusterRolloutAction{kind: clusterRolloutNone}
	}
	// One disruption at a time: never act while the set is still converging.
	for _, p := range pods {
		if !p.ready {
			return clusterRolloutAction{kind: clusterRolloutWait}
		}
	}
	// 1) Roll a stale REPLICA first (lowest pod name for determinism).
	var staleReplicas []string
	for _, s := range shards {
		for _, rp := range s.replicas {
			if rev[rp] != desiredRevision {
				staleReplicas = append(staleReplicas, rp)
			}
		}
	}
	if len(staleReplicas) > 0 {
		sort.Strings(staleReplicas)
		return clusterRolloutAction{kind: clusterRolloutRollReplica, pod: staleReplicas[0]}
	}
	// 2) All replicas fresh → hand over stale masters. Deterministic shard order.
	ordered := append([]clusterShard(nil), shards...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].primary < ordered[j].primary })
	for _, s := range ordered {
		if rev[s.primary] == desiredRevision {
			continue // master already fresh
		}
		// Failover to a fresh, ready replica of THIS shard.
		for _, rp := range s.replicas {
			if rev[rp] == desiredRevision && ready[rp] {
				return clusterRolloutAction{kind: clusterRolloutFailover, pod: rp}
			}
		}
		if len(s.replicas) == 0 {
			// Nothing to fail over to — restart the master directly.
			return clusterRolloutAction{kind: clusterRolloutRollMaster, pod: s.primary}
		}
		// Has replicas but none fresh/ready yet — wait for them.
		return clusterRolloutAction{kind: clusterRolloutWait}
	}
	return clusterRolloutAction{kind: clusterRolloutNone}
}

// driveClusterRollout advances the Cluster rollout by one step. Returns whether
// a rollout is in progress (caller requeues quickly while true).
func (r *ValkeyClusterReconciler) driveClusterRollout(
	ctx context.Context, vc *cachev1beta1.ValkeyCluster, password string,
) (inProgress bool, err error) {
	log := logf.FromContext(ctx)

	var sts appsv1.StatefulSet
	if err := r.Get(ctx, client.ObjectKey{Namespace: vc.Namespace, Name: statefulSetName(vc)}, &sts); err != nil {
		return false, err
	}
	desiredRevision := sts.Status.UpdateRevision
	if desiredRevision == "" {
		return false, nil
	}
	pods, err := r.listRolloutPods(ctx, vc)
	if err != nil {
		return false, err
	}
	if int32(len(pods)) < totalReplicas(vc) {
		return true, nil
	}
	anyStale := false
	for _, p := range pods {
		if p.revision != desiredRevision {
			anyStale = true
			break
		}
	}
	if !anyStale {
		return false, nil
	}

	shardStatuses := r.surveyCluster(ctx, vc, password)
	if len(shardStatuses) == 0 {
		return true, nil // membership unknown — wait
	}
	shards := make([]clusterShard, 0, len(shardStatuses))
	for _, s := range shardStatuses {
		shards = append(shards, clusterShard{primary: s.Primary, replicas: s.Replicas})
	}

	action := nextClusterRolloutStep(pods, shards, desiredRevision)
	switch action.kind {
	case clusterRolloutNone:
		return false, nil
	case clusterRolloutWait:
		return true, nil
	case clusterRolloutRollReplica, clusterRolloutRollMaster:
		log.Info("proactive cluster rollout: rolling pod", "name", vc.Name, "pod", action.pod,
			"role", rolloutRoleLabel(action.kind), "revision", desiredRevision)
		if err := r.deletePodByName(ctx, vc, action.pod); err != nil {
			return true, err
		}
		return true, nil
	case clusterRolloutFailover:
		log.Info("proactive cluster rollout: CLUSTER FAILOVER on fresh replica before restarting master",
			"name", vc.Name, "replica", action.pod, "revision", desiredRevision)
		if err := r.clusterFailoverOnPod(ctx, vc, password, action.pod); err != nil {
			// A failover hiccup is transient — requeue and retry.
			log.Error(err, "CLUSTER FAILOVER failed", "pod", action.pod)
		}
		return true, nil
	}
	return false, nil
}

func rolloutRoleLabel(k clusterRolloutKind) string {
	if k == clusterRolloutRollMaster {
		return roleMaster
	}
	return "replica"
}

// clusterFailoverOnPod dials a specific replica pod and issues CLUSTER FAILOVER,
// promoting it to master for its shard (graceful, no data loss).
func (r *ValkeyClusterReconciler) clusterFailoverOnPod(
	ctx context.Context, vc *cachev1beta1.ValkeyCluster, password, pod string,
) error {
	port := valkeyPort
	if tlsEnabled(vc) {
		port = valkeyTLSPort
	}
	c := dialReplClient(ctx, podFQDN(vc, pod), port, password, tlsEnabled(vc), loadMTLSClientCert(ctx, r, vc), 3*time.Second)
	if c == nil {
		return fmt.Errorf("dial %s for CLUSTER FAILOVER failed", pod)
	}
	defer c.close()
	return c.clusterFailover(ctx)
}
