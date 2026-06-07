/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
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
//     master over via `SENTINEL FAILOVER <name>` and let the Sentinel quorum
//     promote one of the (now already-fresh) replicas. The demoted old master is
//     picked up on the next pass as a stale replica and rolled like any other.
//     Crucially the operator does NOT promote a replica itself here (as it does
//     for Replication): in Sentinel topology the Sentinels own failover, and a
//     direct REPLICAOF would race their election.
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
			// Only the master is stale and every replica is fresh+ready: ask the
			// Sentinels to fail over. They promote a fresh replica; the old master
			// is demoted and rolled as a stale replica on the next pass.
			log.Info("proactive sentinel rollout: SENTINEL FAILOVER before restarting master",
				"name", vc.Name, "master", action.pod, "revision", dataRev)
			if err := r.issueSentinelFailover(ctx, vc, password); err != nil {
				// A failover already in flight, or a transient dial error, is fine —
				// requeue and re-evaluate. We never delete the master directly.
				log.V(1).Info("SENTINEL FAILOVER not accepted (will retry)", "err", err.Error())
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
	port := sentinelPort
	if tlsEnabled(vc) {
		port = sentinelPort + 1 // renderSentinelConf moves Sentinel to tls-port = sentinelPort+1
	}
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
