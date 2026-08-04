/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// ensurePrimaryService creates/updates the `<cluster>-primary` Service, a
// ClusterIP that resolves only to the pod currently carrying roleLabel=primary.
// It is static (the selector never changes); which pod it targets follows the
// role label that ensureRoleLabels moves on failover.
func (r *ValkeyClusterReconciler) ensurePrimaryService(ctx context.Context, vc *cachev1beta1.ValkeyCluster) error {
	desired := buildPrimaryService(vc)
	if err := controllerutil.SetControllerReference(vc, desired, r.Scheme); err != nil {
		return err
	}
	return r.applyService(ctx, desired)
}

// ensureReplicasService creates/updates the `<cluster>-replicas` Service, a
// ClusterIP that load-balances across the replica pods only (roleLabel=replica),
// for spreading reads. It has no endpoints until at least one replica is
// labelled; on a single-node or all-down cluster it is simply empty, which is
// correct.
func (r *ValkeyClusterReconciler) ensureReplicasService(ctx context.Context, vc *cachev1beta1.ValkeyCluster) error {
	desired := buildReplicasService(vc)
	if err := controllerutil.SetControllerReference(vc, desired, r.Scheme); err != nil {
		return err
	}
	return r.applyService(ctx, desired)
}

// ensureRoleLabels stamps the current primary pod with roleLabel=primary and
// every other data pod with roleLabel=replica, so the primary Service resolves to
// the right pod. It is called after failover has resolved the primary; `primary`
// is the pod NAME (e.g. "<cluster>-0"). When it is empty (no primary known yet,
// or a downstream replica cluster with spec.replicateFrom) the labels are left
// untouched.
//
// The role label is not part of any StatefulSet selector, so this is a plain
// label patch. A pod recreated by the StatefulSet comes back without the label
// (it is not in the pod template), so the next reconcile re-stamps it; an
// already-correct pod is skipped so this does not churn resourceVersion.
func (r *ValkeyClusterReconciler) ensureRoleLabels(ctx context.Context, vc *cachev1beta1.ValkeyCluster, primary string) error {
	if primary == "" {
		return nil
	}
	var pods corev1.PodList
	if err := r.List(ctx, &pods,
		client.InNamespace(vc.Namespace), client.MatchingLabels(labelsFor(vc))); err != nil {
		return err
	}
	log := logf.FromContext(ctx)
	for i := range pods.Items {
		pod := &pods.Items[i]
		want := roleReplica
		if pod.Name == primary {
			want = rolePrimary
		}
		if pod.Labels[roleLabel] == want {
			continue // already correct — no write
		}
		patch := client.MergeFrom(pod.DeepCopy())
		if pod.Labels == nil {
			pod.Labels = map[string]string{}
		}
		pod.Labels[roleLabel] = want
		if err := r.Patch(ctx, pod, patch); err != nil {
			// Best-effort: log and continue so one un-patchable pod does not block
			// the others; the next reconcile retries.
			log.Error(err, "role label patch failed", "pod", pod.Name, "role", want)
		}
	}
	return nil
}
