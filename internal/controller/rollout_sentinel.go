/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// Proactive rolling restart (ADR 0004), Sentinel topology.
//
// Two planes are rolled in order, one pod at a time, under updateStrategy
// OnDelete:
//
//  1. Data plane: stale replicas first (a replica restart needs no failover),
//     then — once only the master is stale and every replica is fresh — hand the
//     master over (sentinelHandover): pause its writes, wait for the replicas to
//     catch up, run `SENTINEL FAILOVER <name>` and let the Sentinel quorum
//     promote one of the (now already-fresh) replicas, then point the old master
//     at the promoted one before lifting the pause. The old master is picked up
//     on the next pass as a stale replica and rolled like any other.
//     Crucially the operator does NOT promote a replica itself here (as it does
//     for Replication): in Sentinel topology the Sentinels own failover, and a
//     direct promotion would race their election. The only REPLICAOF it sends
//     goes to the old master, once a replica reports itself promoted.
//  2. Sentinel plane: the Sentinel pods are stateless monitors — roll the stale
//     ones one at a time; the "one disruption, wait for Ready" loop keeps the
//     quorum (replicas >= 3) up throughout.
//
// Staleness is the same STS-revision signal the Replication/Cluster rollouts
// use, computed separately for each plane's StatefulSet. The loop is stateless
// and resumable: every reconcile recomputes the next step from the live pods, so
// re-issuing SENTINEL FAILOVER while one is already in flight (a harmless
// "-INPROG" error) is fine.

// driveSentinelRollout advances the Sentinel-topology rollout by one step.
// Returns whether a rollout is in progress (caller requeues quickly while true).
func (r *ValkeyClusterReconciler) driveSentinelRollout(
	ctx context.Context, vc *cachev1beta1.ValkeyCluster, password string,
) (inProgress bool, err error) {
	log := logf.FromContext(ctx)

	// --- Plane 1: data pods ---
	var dataSTS appsv1.StatefulSet
	if err := r.Get(ctx, client.ObjectKey{Namespace: vc.Namespace, Name: statefulSetName(vc)}, &dataSTS); err != nil {
		return false, err
	}
	dataRev := dataSTS.Status.UpdateRevision
	if dataRev == "" {
		return false, nil
	}
	dataPods, err := r.listRolloutPods(ctx, vc)
	if err != nil {
		return false, err
	}
	if int32(len(dataPods)) < vc.Spec.Replicas {
		return true, nil // set still forming
	}
	if anyStale(dataPods, dataRev) {
		// The current master is whichever data pod INFO reports as role:master.
		// Until we can see it, wait rather than guess — a wrong "primary" would
		// make nextRolloutStep treat the real master as a stale replica and delete
		// it directly, defeating the whole point.
		survey := r.surveyReplication(ctx, vc, password)
		master := sentinelDataMaster(vc, survey)
		if master == "" {
			return true, nil
		}
		action := nextRolloutStep(dataPods, dataRev, master)
		switch action.kind {
		case rolloutWait:
			return true, nil
		case rolloutRollReplica:
			log.Info("proactive sentinel rollout: rolling data replica",
				"name", vc.Name, "pod", action.pod, "revision", dataRev)
			if err := r.deletePodByName(ctx, vc, action.pod); err != nil {
				return true, err
			}
			return true, nil
		case rolloutPromote:
			// Only the master is stale and every replica is fresh+ready: hand it
			// over. The Sentinels promote a fresh replica; the old master is
			// demoted and rolled as a stale replica on the next pass.
			log.Info("proactive sentinel rollout: handing the master over before restarting it",
				"name", vc.Name, "master", action.pod, "revision", dataRev)
			if err := r.sentinelHandover(ctx, vc, password, survey, action.pod); err != nil {
				// A failover already in flight, replicas that did not catch up, or a
				// transient dial error — requeue and re-evaluate. We never delete the
				// master directly.
				log.Info("proactive sentinel rollout: handover not done (will retry)",
					"name", vc.Name, "master", action.pod, "err", err.Error())
			}
			return true, nil
		case rolloutNone:
			// data plane converged — fall through to the Sentinel plane
		}
	}

	// --- Plane 2: sentinel pods ---
	var senSTS appsv1.StatefulSet
	if err := r.Get(ctx, client.ObjectKey{Namespace: vc.Namespace, Name: sentinelStatefulSetName(vc)}, &senSTS); err != nil {
		return false, err
	}
	senRev := senSTS.Status.UpdateRevision
	if senRev == "" {
		return false, nil
	}
	senPods, err := r.listSentinelRolloutPods(ctx, vc)
	if err != nil {
		return false, err
	}
	if int32(len(senPods)) < vc.Spec.Sentinel.Replicas {
		return true, nil
	}
	// Sentinels have no primary — nextRolloutStep with primary="" only ever
	// returns roll-replica/wait/none, rolling one stale Sentinel at a time.
	action := nextRolloutStep(senPods, senRev, "")
	switch action.kind {
	case rolloutWait:
		return true, nil
	case rolloutRollReplica:
		log.Info("proactive sentinel rollout: rolling sentinel pod",
			"name", vc.Name, "pod", action.pod, "revision", senRev)
		if err := r.deletePodByName(ctx, vc, action.pod); err != nil {
			return true, err
		}
		return true, nil
	}
	return false, nil
}

// anyStale reports whether any pod is off the desired STS revision.
func anyStale(pods []rolloutPod, desiredRevision string) bool {
	for _, p := range pods {
		if p.revision != desiredRevision {
			return true
		}
	}
	return false
}

// sentinelDataMaster returns the pod name of the reachable data node that INFO
// reports as the master, or "" if none is currently visible.
func sentinelDataMaster(vc *cachev1beta1.ValkeyCluster, survey []podRole) string {
	for i := range survey {
		s := survey[i]
		if s.reachable && s.role == roleMaster {
			return podName(vc, s.ordinal)
		}
	}
	return ""
}

// sentinelStatefulSetName is the name shared by the Sentinel StatefulSet and its
// headless Service.
func sentinelStatefulSetName(vc *cachev1beta1.ValkeyCluster) string {
	return vc.Name + "-sentinel"
}

// sentinelPodFQDN is the in-cluster DNS name of a Sentinel pod, behind the
// Sentinel headless Service.
func sentinelPodFQDN(vc *cachev1beta1.ValkeyCluster, ord int32) string {
	sn := sentinelStatefulSetName(vc)
	return fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local", sn, ord, sn, vc.Namespace)
}

// listSentinelRolloutPods returns the rollout view of the Sentinel StatefulSet's
// pods (component=sentinel), mirroring listRolloutPods for the data plane.
func (r *ValkeyClusterReconciler) listSentinelRolloutPods(ctx context.Context, vc *cachev1beta1.ValkeyCluster) ([]rolloutPod, error) {
	var list corev1.PodList
	if err := r.List(ctx, &list, client.InNamespace(vc.Namespace),
		client.MatchingLabels{instanceLabel: vc.Name, componentLabel: componentSentinel}); err != nil {
		return nil, err
	}
	sts := sentinelStatefulSetName(vc)
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

// issueSentinelFailover dials a reachable Sentinel and asks it to fail the
// monitored master over to a replica. Any Sentinel can accept the command — it
// propagates through the quorum — so we try pods in order and return the first
// one's verdict (including the harmless "already in progress" error).
func (r *ValkeyClusterReconciler) issueSentinelFailover(ctx context.Context, vc *cachev1beta1.ValkeyCluster, password string) error {
	port := sentinelListenPort(vc)
	var lastErr error
	for i := int32(0); i < vc.Spec.Sentinel.Replicas; i++ {
		host := sentinelPodFQDN(vc, i)
		c := dialReplClient(ctx, host, port, password, tlsEnabled(vc), loadMTLSClientCert(ctx, r, vc), 3*time.Second)
		if c == nil {
			lastErr = fmt.Errorf("dial sentinel %s failed", host)
			continue
		}
		err := c.sentinelFailover(ctx, sentinelMasterName)
		c.close()
		return err // reached a Sentinel; its verdict is authoritative
	}
	return lastErr
}

// handoverTimings bounds the master handover of the proactive Sentinel rollout.
// catchUp and promote run one after the other within pause, so the writes are
// held for at most pause, even if the operator stops halfway.
type handoverTimings struct {
	pause   time.Duration // CLIENT PAUSE timeout on the old master
	catchUp time.Duration // wait for the replicas to reach the master's offset
	promote time.Duration // wait for a replica to report role:master
	poll    time.Duration
}

var sentinelHandoverTimings = handoverTimings{
	pause:   15 * time.Second,
	catchUp: 5 * time.Second,
	promote: 8 * time.Second,
	poll:    100 * time.Millisecond,
}

// sentinelHandover hands the master over to a replica without losing the
// writes it acknowledged. SENTINEL FAILOVER alone does not stop them: the
// master keeps taking writes until the Sentinels demote it, seconds after the
// promotion, and the promoted replica may lack the last ones it had. The
// handover pauses the master's writes, waits for every replica to reach its
// offset, runs SENTINEL FAILOVER, waits for a replica to report itself
// promoted, points the old master at it and lifts the pause: the held writes
// then fail with READONLY instead of landing on the old master.
func (r *ValkeyClusterReconciler) sentinelHandover(ctx context.Context, vc *cachev1beta1.ValkeyCluster, password string, survey []podRole, master string) error {
	port := valkeyPort
	if tlsEnabled(vc) {
		port = valkeyTLSPort
	}
	cert := loadMTLSClientCert(ctx, r, vc)
	dial := func(host string) *replClient {
		return dialReplClient(ctx, host, port, password, tlsEnabled(vc), cert, 2*time.Second)
	}
	var masterHost string
	var replicaHosts []string
	for _, s := range survey {
		switch {
		case podName(vc, s.ordinal) == master:
			masterHost = s.host
		case s.reachable:
			replicaHosts = append(replicaHosts, s.host)
		}
	}
	failover := func() error { return r.issueSentinelFailover(ctx, vc, password) }
	return handOverMaster(ctx, dial, failover, masterHost, replicaHosts, port, sentinelHandoverTimings)
}

// handOverMaster runs the handover described on sentinelHandover. dial and
// failover are parameters so tests can run it against in-process servers.
func handOverMaster(ctx context.Context, dial func(host string) *replClient, failover func() error,
	masterHost string, replicaHosts []string, port int32, t handoverTimings) (err error) {
	if masterHost == "" || len(replicaHosts) == 0 {
		return errors.New("no master or no replica to hand over to")
	}
	mc := dial(masterHost)
	if mc == nil {
		return fmt.Errorf("dial master %s failed", masterHost)
	}
	defer mc.close()
	if err := mc.clientPauseWrite(ctx, t.pause); err != nil {
		return fmt.Errorf("CLIENT PAUSE: %w", err)
	}
	// Lift the pause on every path, and only after the REPLICAOF below when
	// there is one.
	defer func() {
		if uerr := mc.clientUnpause(context.WithoutCancel(ctx)); uerr != nil && err == nil {
			err = fmt.Errorf("CLIENT UNPAUSE: %w", uerr)
		}
	}()

	if err := wait.PollUntilContextTimeout(ctx, t.poll, t.catchUp, true, func(ctx context.Context) (bool, error) {
		info, err := mc.info(ctx)
		return err == nil && replicasCaughtUp(info), nil
	}); err != nil {
		return fmt.Errorf("the replicas did not catch up with the master: %w", err)
	}
	// INPROG: a failover is already running. The pause also holds the
	// Sentinels' PUBLISH on the master, so with a short down-after-milliseconds
	// they may start one themselves; the replicas are caught up either way, so
	// wait for its promotion as for ours.
	if err := failover(); err != nil && !strings.HasPrefix(err.Error(), "INPROG") {
		return fmt.Errorf("SENTINEL FAILOVER: %w", err)
	}

	replicas := make(map[string]*replClient, len(replicaHosts))
	defer func() {
		for _, c := range replicas {
			c.close()
		}
	}()
	var promoted string
	if err := wait.PollUntilContextTimeout(ctx, t.poll, t.promote, true, func(ctx context.Context) (bool, error) {
		for _, h := range replicaHosts {
			c := replicas[h]
			if c == nil {
				if c = dial(h); c == nil {
					continue
				}
				replicas[h] = c
			}
			if info, err := c.info(ctx); err == nil && info["role"] == roleMaster {
				promoted = h
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		return fmt.Errorf("no replica was promoted: %w", err)
	}
	if err := mc.replicaOf(ctx, promoted, port); err != nil {
		return fmt.Errorf("REPLICAOF %s on the old master: %w", promoted, err)
	}
	return nil
}

// replicasCaughtUp reports whether a master's INFO replication lists at least
// one replica and every replica is online at the master's offset.
func replicasCaughtUp(info map[string]string) bool {
	masterOffset, err := strconv.ParseInt(info["master_repl_offset"], 10, 64)
	if err != nil {
		return false
	}
	n, err := strconv.Atoi(info["connected_slaves"])
	if err != nil || n == 0 {
		return false
	}
	for i := range n {
		fields := map[string]string{}
		for kv := range strings.SplitSeq(info["slave"+strconv.Itoa(i)], ",") {
			if k, v, ok := strings.Cut(kv, "="); ok {
				fields[k] = v
			}
		}
		offset, err := strconv.ParseInt(fields["offset"], 10, 64)
		if err != nil || fields["state"] != "online" || offset < masterOffset {
			return false
		}
	}
	return true
}
