/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"context"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// quorumRecoveryDownAfter is how long the operator observes a Cluster stuck in
// cluster_state:fail — with the surviving primaries below a voting quorum and a
// recoverable, fenced primary majority — before it drives recovery. It is
// deliberately longer than the Replication failover debounce: gossip promotes
// replicas on its own wherever a master quorum still exists, and the operator
// must not race that. It only steps in for the one state gossip cannot fix — a
// lost majority — which stays stuck indefinitely, so a generous window costs
// nothing but rules out a transient blip masquerading as quorum loss.
const quorumRecoveryDownAfter = 45 * time.Second

// quorumTakeoverAnnotation force-enables (or force-disables) automatic quorum
// recovery for a cluster, overriding the profile default. Cache clusters recover
// automatically; Durable clusters default OFF, because a forced CLUSTER FAILOVER
// TAKEOVER can promote a replica that Valkey's replica-validity factor would
// refuse, silently dropping acknowledged writes — a trade-off a Durable operator
// must accept explicitly.
const quorumTakeoverAnnotation = "valkey.wellcake.io/quorum-takeover"

// quorumTakeoverTarget is one shard the operator will recover: the dead primary
// to fence and the surviving replica to promote in its place.
type quorumTakeoverTarget struct {
	primaryPod string
	primaryID  string
	replicaPod string
}

// quorumRecoveryEnabled reports whether automatic quorum recovery is allowed for
// this cluster. Cache: yes by default (availability-first). Durable: only when
// explicitly opted in via the annotation, because a forced takeover can drop
// acknowledged writes the Durable profile promises to keep. An explicit
// annotation value always wins (it can also disable recovery for a Cache cluster).
func quorumRecoveryEnabled(vc *cachev1beta1.ValkeyCluster) bool {
	if v, err := strconv.ParseBool(vc.Annotations[quorumTakeoverAnnotation]); err == nil {
		return v
	}
	return vc.Spec.Profile != cachev1beta1.ProfileDurable
}

// maybeRecoverClusterQuorum detects and recovers a Cluster that has lost the
// majority of its primaries — the one failure mode gossip cannot self-heal,
// because authorizing a replica failover needs a master quorum that no longer
// exists. Such a cluster sits in cluster_state:fail indefinitely with unserved
// slots; the surviving replicas of the dead primaries wait for votes that can
// never come.
//
// The operator breaks the deadlock with two things gossip lacks: a view of pod
// and node liveness through the k8s API, and a direct out-of-band connection to
// each node. It intervenes ONLY when all of the following hold, so it never races
// a failover gossip could still perform and never overrules a primary that might
// still be serving:
//
//   - the surviving primaries are below a voting quorum (healthy*2 <= total) —
//     gossip provably cannot promote on its own;
//   - the cluster has been stuck this way for quorumRecoveryDownAfter;
//   - each dead primary is BOTH k8s-fenced (pod/node confirmed not serving via
//     the k8s API) AND not observably serving on its data path (unreachable, or
//     reachable but reporting cluster_state:fail). A primary reachable AND
//     reporting cluster_state:ok is genuinely serving and is never taken over.
//
// Write split-brain safety does NOT rest on proving the old primary's process is
// dead — no k8s operator can do that without STONITH, and on a control-plane
// partition a NotReady/absent-node pod may still be running. It rests on Valkey
// Cluster's own guarantee: a primary partitioned from the majority of masters for
// longer than cluster-node-timeout stops accepting writes (returns CLUSTERDOWN),
// because a replica on the majority side may have been promoted. quorumRecovery
// DownAfter (45s) is an order of magnitude above the operator's node-timeout
// (5s), so by the time a takeover fires, a still-running-but-isolated old primary
// has already stopped serving writes. The k8s fence and the data-path check are
// defence in depth on top of that — they decide WHEN to intervene and refuse to
// overrule a primary that is demonstrably still serving. (A minority primary may
// still answer stale READS until it rejoins; the Cache profile, the only one that
// recovers automatically, accepts that AP trade-off — Durable is opt-in.)
//
// Returns handled=true (with a fast requeue) while recovery is pending or in
// flight; handled=false when the cluster is healthy, recovery is disabled, or
// nothing is safely recoverable and normal reconciliation should continue.
func (r *ValkeyClusterReconciler) maybeRecoverClusterQuorum(
	ctx context.Context, vc *cachev1beta1.ValkeyCluster, password string,
) (ctrl.Result, bool, error) {
	log := logf.FromContext(ctx)
	if !vc.Status.ClusterInitialized {
		return ctrl.Result{}, false, nil
	}
	if !quorumRecoveryEnabled(vc) {
		// Disabled (Durable without opt-in, or explicitly turned off) — don't
		// leave a stale debounce marker behind if it was toggled mid-incident.
		r.clearQuorumDown(ctx, vc)
		return ctrl.Result{}, false, nil
	}

	// Dial ANY reachable pod — pod-0 may itself be one of the dead primaries, so
	// unlike surveyCluster we walk every pod until one answers.
	c := r.dialAnyClusterPod(ctx, vc, password)
	if c == nil {
		// Nothing reachable: a total outage, or the operator is partitioned from
		// the data plane. Either way we can neither confirm state nor fence
		// safely — do nothing and let the normal requeue retry.
		return ctrl.Result{}, false, nil
	}
	defer c.close()

	info, err := c.clusterInfoMap(ctx)
	if err != nil {
		return ctrl.Result{}, false, nil
	}
	if info["cluster_state"] == "ok" {
		// Healthy — gossip is coping or has already recovered. This is the ONLY
		// place the debounce marker is cleared, so transient probe errors below
		// never reset the clock.
		r.clearQuorumDown(ctx, vc)
		return ctrl.Result{}, false, nil
	}

	raw, err := c.clusterNodes(ctx)
	if err != nil {
		return ctrl.Result{}, false, nil
	}
	nodes := parseClusterNodes(raw)

	// Majority gate: intervene only when the surviving primaries are NOT a voting
	// quorum, so gossip provably cannot promote on its own. With a quorum intact,
	// gossip will heal a genuine primary loss (or deliberately refuse under the
	// Durable validity factor), and the operator must not race or overrule it —
	// so a mere cluster_state:fail with a minority of primaries down is left alone.
	healthy, total := countMasters(nodes)
	if total == 0 || healthy*2 > total {
		return ctrl.Result{}, false, nil
	}

	targets := r.recoverableShards(ctx, vc, password, nodes)
	if len(targets) == 0 {
		// Stuck below quorum but nothing SAFELY recoverable right now (a shard
		// that lost every pod needs a restore; a "down" primary still reachable
		// and serving must not be overruled; or a transient probe error). Do not
		// clear the marker — only cluster_state:ok does that — so an API blip
		// cannot reset the debounce clock. Take no action.
		return ctrl.Result{}, false, nil
	}

	// Debounce: act only after the cluster has been stuck below quorum for the
	// whole window.
	if !r.quorumDownLongEnough(ctx, vc, log) {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, true, nil
	}

	// Note: the marker is intentionally NOT cleared here. Clearing happens only on
	// the observed cluster_state:ok above — so if a recovery attempt fails, the
	// next pass (debounce already elapsed) retries immediately instead of
	// re-waiting the whole window.
	for _, t := range targets {
		r.recoverShard(ctx, vc, password, c.port, t, log)
	}

	// Requeue quickly to observe recovery: slots re-served, cluster_state:ok, the
	// fenced primaries re-created by their StatefulSet rejoin as replicas.
	return ctrl.Result{RequeueAfter: 5 * time.Second}, true, nil
}

// recoverShard fences one dead primary and promotes its surviving replica. It
// re-confirms the primary is not serving RIGHT BEFORE acting (a fresh split-brain
// guard, since the debounce window may have changed things), then fences the pod
// and issues the takeover. Fencing (deleting the pod) precedes the takeover as
// defence in depth: the primary is already proven not-serving, but deleting it
// first also lets the StatefulSet re-create it — it rejoins as a replica of the
// new primary through its retained data PVC (same node ID, it sees the higher
// configEpoch and demotes). Per-shard errors are logged and the next reconcile
// retries; one bad shard does not block the others.
func (r *ValkeyClusterReconciler) recoverShard(
	ctx context.Context, vc *cachev1beta1.ValkeyCluster, password string, port int32,
	t quorumTakeoverTarget, log logr.Logger,
) {
	// Fresh guards, re-evaluated right before acting because the debounce window
	// may have changed things: the primary must STILL be k8s-fenced AND still not
	// serving. Re-checking the fence closes the window where the pod returned to
	// Running+Ready on a Ready node during the wait.
	fenced, err := r.primaryFencedByK8s(ctx, vc, t.primaryPod)
	if err != nil || !fenced {
		log.Info("quorum recovery: primary no longer fenced by k8s — aborting takeover",
			"name", vc.Name, "primary", t.primaryPod)
		return
	}
	if r.primaryStillServing(ctx, vc, password, t.primaryPod) {
		log.Info("quorum recovery: primary is serving again (cluster_state:ok) — aborting takeover",
			"name", vc.Name, "primary", t.primaryPod)
		return
	}

	log.Info("quorum recovery: fencing dead primary and taking over on replica",
		"name", vc.Name, "deadPrimary", t.primaryPod, "promote", t.replicaPod, "nodeID", t.primaryID)

	if err := r.deletePodByName(ctx, vc, t.primaryPod); err != nil {
		log.Error(err, "quorum recovery: failed to fence dead primary; skipping takeover this pass",
			"pod", t.primaryPod)
		return
	}

	rc := dialReplClient(ctx, podFQDN(vc, t.replicaPod), port, password,
		tlsEnabled(vc), loadMTLSClientCert(ctx, r, vc), 3*time.Second)
	if rc == nil {
		log.Error(nil, "quorum recovery: replica unreachable at takeover; will retry", "replica", t.replicaPod)
		return
	}
	defer rc.close()

	if err := rc.clusterFailoverTakeover(ctx); err != nil {
		log.Error(err, "quorum recovery: CLUSTER FAILOVER TAKEOVER failed", "replica", t.replicaPod)
		return
	}
	failoverTotal.WithLabelValues(vc.Namespace, vc.Name, "cluster-takeover").Inc()
	log.Info("quorum recovery: takeover issued", "name", vc.Name, "newPrimary", t.replicaPod)
}

// recoverableShards returns, for each down primary that is SAFE to recover, the
// dead primary and the surviving replica to promote. A shard qualifies only when
// ALL hold:
//   - its primary is unhealthy in CLUSTER NODES (fail/fail?/noaddr, or a
//     disconnected bus link);
//   - its node name maps to a real data pod of THIS cluster — a CLUSTER NODES
//     entry that parsed to a bare IP (no announce hostname) is never mistaken for
//     a fenced pod of ours (its Get would 404 and look "fenced");
//   - the primary pod is k8s-FENCED — not serving per the k8s API (missing, not
//     Running, not Ready, or on a NotReady/absent node);
//   - AND the primary is confirmed not serving on its data path — unreachable, or
//     reachable but reporting cluster_state:fail (it has detected its own minority
//     isolation and stopped serving, Valkey's built-in partition protection). A
//     primary reachable AND reporting cluster_state:ok is genuinely serving and is
//     refused. Requiring BOTH the k8s view and this direct check is what closes
//     the split-brain window: k8s-dead-but-still-serving (control-plane partition)
//     is caught by the data-path check, and briefly-unreachable-but-alive (a GC
//     blip) is caught by the k8s view;
//   - at least one of its replicas is reachable (the takeover target). Among
//     reachable replicas the most up-to-date (highest slave_repl_offset) wins.
//
// A shard that lost every pod has no replica to promote and is skipped — it needs
// a restore, which is out of scope for automatic recovery.
func (r *ValkeyClusterReconciler) recoverableShards(
	ctx context.Context, vc *cachev1beta1.ValkeyCluster, password string, nodes []clusterNode,
) []quorumTakeoverTarget {
	log := logf.FromContext(ctx)
	known := knownDataPods(vc)

	replicasByMaster := map[string][]clusterNode{}
	for _, n := range nodes {
		// Only real data pods of THIS cluster are eligible promotion targets —
		// same guard the primaries get, so a bare-IP/foreign node name is never
		// dialed as a candidate replica.
		if !n.isMaster() && n.masterID != "" && known[n.podName] {
			replicasByMaster[n.masterID] = append(replicasByMaster[n.masterID], n)
		}
	}

	var out []quorumTakeoverTarget
	for _, n := range nodes {
		if !n.isMaster() || n.isHealthy() {
			continue // only DOWN primaries are recovery candidates
		}
		if n.podName == "" || !known[n.podName] {
			log.Info("quorum recovery: down primary does not map to a known data pod; skipping",
				"name", vc.Name, "podName", n.podName)
			continue
		}
		fenced, err := r.primaryFencedByK8s(ctx, vc, n.podName)
		if err != nil {
			log.Error(err, "quorum recovery: could not evaluate primary liveness; skipping", "pod", n.podName)
			continue
		}
		if !fenced {
			log.Info("quorum recovery: down primary still Running+Ready per k8s — not taking over (split-brain guard)",
				"name", vc.Name, "primary", n.podName)
			continue
		}
		if r.primaryStillServing(ctx, vc, password, n.podName) {
			log.Info("quorum recovery: down primary still reachable and reports cluster_state:ok — refusing takeover (split-brain guard)",
				"name", vc.Name, "primary", n.podName)
			continue
		}
		replica := r.pickTakeoverReplica(ctx, vc, password, replicasByMaster[n.id])
		if replica == "" {
			log.Info("quorum recovery: down primary has no reachable replica — shard needs restore, skipping",
				"name", vc.Name, "primary", n.podName)
			continue
		}
		out = append(out, quorumTakeoverTarget{primaryPod: n.podName, primaryID: n.id, replicaPod: replica})
	}
	return out
}

// countMasters returns (healthy, total) masters from a CLUSTER NODES view. A
// "healthy" master is one not flagged fail/fail? with a connected bus link — i.e.
// one that could still cast a failover vote. Used by the majority gate to decide
// whether gossip still has a quorum to promote replicas on its own.
func countMasters(nodes []clusterNode) (healthy, total int) {
	for _, n := range nodes {
		if !n.isMaster() {
			continue
		}
		total++
		if n.isHealthy() {
			healthy++
		}
	}
	return healthy, total
}

// knownDataPods returns the set of pod names that legitimately belong to this
// Cluster's data plane, so a CLUSTER NODES entry that parsed to a bare IP (no
// announce hostname) or any other unexpected name is never mistaken for a fenced
// pod of ours.
func knownDataPods(vc *cachev1beta1.ValkeyCluster) map[string]bool {
	out := map[string]bool{}
	for _, p := range clusterDataPods(vc) {
		out[podNameFromFQDN(p.host)] = true
	}
	return out
}

// primaryStillServing reports whether the (allegedly down) primary is OBSERVABLY
// still serving: reachable from the operator AND reporting cluster_state:ok. That
// is the one state a takeover must never override, so it returns true only then.
//
// A false result means "not observably serving", NOT a proof the process is dead:
// unreachable (the common case — pod/node gone) and reachable-but-cluster_state
// :fail (it detected its own minority isolation) both return false. Write safety
// for the unreachable case does not come from this check but from Valkey's
// minority-write-block plus the quorumRecoveryDownAfter debounce (see
// maybeRecoverClusterQuorum); this check's job is to refuse takeover of a primary
// that is provably still serving.
func (r *ValkeyClusterReconciler) primaryStillServing(ctx context.Context, vc *cachev1beta1.ValkeyCluster, password, podName string) bool {
	port := valkeyPort
	if tlsEnabled(vc) {
		port = valkeyTLSPort
	}
	c := dialReplClient(ctx, podFQDN(vc, podName), port, password,
		tlsEnabled(vc), loadMTLSClientCert(ctx, r, vc), 2*time.Second)
	if c == nil {
		return false // unreachable → serving no one → safe to take over
	}
	defer c.close()
	info, err := c.clusterInfoMap(ctx)
	if err != nil {
		return false // cannot confirm it is serving → treat as not serving
	}
	return info["cluster_state"] == "ok"
}

// pickTakeoverReplica dials each of a dead primary's replicas and returns the pod
// name of the reachable one with the highest replication offset — the least data
// behind, so promoting it loses the least. A direct dial (not the gossip health
// flag) is the authority on which replica is actually alive. Returns "" if none
// answer.
func (r *ValkeyClusterReconciler) pickTakeoverReplica(
	ctx context.Context, vc *cachev1beta1.ValkeyCluster, password string, replicas []clusterNode,
) string {
	port := valkeyPort
	if tlsEnabled(vc) {
		port = valkeyTLSPort
	}
	best := ""
	var bestOff int64 = -1
	for _, rep := range replicas {
		if rep.podName == "" {
			continue
		}
		rc := dialReplClient(ctx, podFQDN(vc, rep.podName), port, password,
			tlsEnabled(vc), loadMTLSClientCert(ctx, r, vc), 2*time.Second)
		if rc == nil {
			continue
		}
		info, err := rc.info(ctx)
		rc.close()
		if err != nil {
			continue
		}
		off := int64(0)
		if v, ok := info["slave_repl_offset"]; ok {
			if n, e := strconv.ParseInt(v, 10, 64); e == nil {
				off = n
			}
		}
		if off > bestOff {
			bestOff = off
			best = rep.podName
		}
	}
	return best
}

// primaryFencedByK8s reports whether the named primary pod is NOT serving clients
// per the k8s API. True when the pod is missing, not in the Running phase, not
// Ready, or on a Node that is NotReady or gone. False when the pod is Running AND
// Ready on a Ready node.
//
// This is only ONE half of the fence: a NotReady/absent node means the kubelet
// cannot report a live process, but on a pure control-plane partition the process
// may still be running, so the caller ALSO requires primaryStillServing==false
// (a direct data-path check) before taking over. Conversely, the Running+Ready
// case here guards the opposite error — never overruling a pod k8s still believes
// is alive on the strength of a transient Valkey unreachability.
func (r *ValkeyClusterReconciler) primaryFencedByK8s(ctx context.Context, vc *cachev1beta1.ValkeyCluster, podName string) (bool, error) {
	var pod corev1.Pod
	err := r.Get(ctx, types.NamespacedName{Namespace: vc.Namespace, Name: podName}, &pod)
	if apierrors.IsNotFound(err) {
		return true, nil // pod gone → fenced (per k8s)
	}
	if err != nil {
		return false, err
	}
	if pod.Status.Phase != corev1.PodRunning || !podIsReady(&pod) {
		return true, nil // crashed / terminating / not Ready → fenced
	}
	if pod.Spec.NodeName == "" {
		return false, nil // Running but unscheduled is impossible in practice; be conservative
	}
	ready, err := r.nodeReady(ctx, pod.Spec.NodeName)
	if err != nil {
		return false, err
	}
	// Live pod on a live node → NOT fenced; dead/absent node → stale-Ready pod → fenced.
	return !ready, nil
}

// nodeReady reports whether the named Node currently has Ready=True. A missing
// node counts as not-ready — it was deleted out from under its pods, which are
// therefore gone too.
func (r *ValkeyClusterReconciler) nodeReady(ctx context.Context, name string) (bool, error) {
	var node corev1.Node
	if err := r.Get(ctx, types.NamespacedName{Name: name}, &node); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	for _, cond := range node.Status.Conditions {
		if cond.Type == corev1.NodeReady {
			return cond.Status == corev1.ConditionTrue, nil
		}
	}
	return false, nil
}

// dialAnyClusterPod returns a client to the first reachable Cluster data pod, or
// nil if none answer. Unlike surveyCluster (which only tries pod-0), this walks
// every pod so a cluster whose pod-0 is itself a dead primary can still be
// surveyed and recovered.
func (r *ValkeyClusterReconciler) dialAnyClusterPod(ctx context.Context, vc *cachev1beta1.ValkeyCluster, password string) *replClient {
	port := valkeyPort
	if tlsEnabled(vc) {
		port = valkeyTLSPort
	}
	for _, p := range clusterDataPods(vc) {
		if c := dialReplClient(ctx, p.host, port, password,
			tlsEnabled(vc), loadMTLSClientCert(ctx, r, vc), 2*time.Second); c != nil {
			return c
		}
	}
	return nil
}

// quorumDownLongEnough records (on first observation) when the cluster went
// recoverably-stuck and reports whether it has now been stuck for at least
// quorumRecoveryDownAfter. The timestamp is persisted in status so the window
// survives across reconciles.
func (r *ValkeyClusterReconciler) quorumDownLongEnough(ctx context.Context, vc *cachev1beta1.ValkeyCluster, log logr.Logger) bool {
	if vc.Status.QuorumDownSince == nil {
		now := metav1.Now()
		log.Info("cluster quorum lost and recoverable; starting recovery debounce",
			"name", vc.Name, "downAfter", quorumRecoveryDownAfter.String())
		r.setQuorumDown(ctx, vc, &now)
		return false
	}
	return downElapsed(vc.Status.QuorumDownSince.Time, time.Now(), quorumRecoveryDownAfter)
}

func (r *ValkeyClusterReconciler) clearQuorumDown(ctx context.Context, vc *cachev1beta1.ValkeyCluster) {
	if vc.Status.QuorumDownSince != nil {
		r.setQuorumDown(ctx, vc, nil)
	}
}

// setQuorumDown persists the QuorumDownSince marker via its OWN status patch with
// a base captured before the mutation, so the change lands in the diff (the later
// updateClusterStatus patch captures its base after this and leaves the field
// untouched). Best-effort: a failed patch just means the next reconcile
// re-evaluates the window.
func (r *ValkeyClusterReconciler) setQuorumDown(ctx context.Context, vc *cachev1beta1.ValkeyCluster, t *metav1.Time) {
	base := client.MergeFrom(vc.DeepCopy())
	vc.Status.QuorumDownSince = t
	if err := r.Status().Patch(ctx, vc, base); err != nil {
		logf.FromContext(ctx).V(1).Info("quorum-down marker patch failed (will re-evaluate)", "err", err.Error())
	}
}
