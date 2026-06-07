/*
Copyright 2026 The Wellcake Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// finalizerName protects deletion until the operator has had a chance to drain
// or otherwise cleanly tear down the underlying Valkey state. For Replication
// today this is a no-op (the StatefulSet teardown is enough); for Cluster it
// is a hook for future graceful resharding before the last shard goes away.
const finalizerName = "valkey.wellcake.io/cleanup"

// ValkeyClusterReconciler reconciles a ValkeyCluster object.
type ValkeyClusterReconciler struct {
	client.Client
	// APIReader is an uncached, read-through client. The embedded Client serves
	// reads from the informer cache, which lags writes; for the once-per-token
	// password-rotation guard we need an authoritative status read so a second,
	// quickly-following reconcile doesn't re-run a rotation the first already did.
	APIReader client.Reader
	Scheme    *runtime.Scheme
	// MaxConcurrentReconciles caps how many ValkeyClusters reconcile in parallel.
	// controller-runtime never runs two reconciles for the SAME object at once,
	// so raising this only parallelises across DIFFERENT clusters — the default
	// of 1 serialises the whole fleet and bottlenecks at 500+ CRs (design-review
	// Q3/SC2). 0 falls back to 1.
	MaxConcurrentReconciles int
}

// +kubebuilder:rbac:groups=cache.wellcake.io,resources=valkeyclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cache.wellcake.io,resources=valkeyclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cache.wellcake.io,resources=valkeyclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services;configmaps;secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=servicemonitors,verbs=get;list;watch;create;update;patch;delete

func (r *ValkeyClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (_ ctrl.Result, retErr error) {
	_ = logf.FromContext(ctx)
	defer recordReconcile("valkeycluster", req.Namespace, req.Name, &retErr)

	var vc cachev1beta1.ValkeyCluster
	if err := r.Get(ctx, req.NamespacedName, &vc); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Finalizer lifecycle. On deletion we run cleanup hooks before letting
	// Kubernetes garbage-collect the owned objects.
	if !vc.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, &vc)
	}
	if !controllerutil.ContainsFinalizer(&vc, finalizerName) {
		controllerutil.AddFinalizer(&vc, finalizerName)
		if err := r.Update(ctx, &vc); err != nil {
			return ctrl.Result{}, err
		}
		// Fall through into the work pass in the same reconcile: r.Update
		// refreshed vc's resourceVersion in place, so later writes don't
		// conflict. (Previously returned Result{Requeue: true}; that field is
		// deprecated and the explicit requeue is unnecessary — continuing here
		// is equivalent and saves a reconcile round-trip.)
	}

	// Defensive defaulting: the mutating webhook normally fills these at
	// admission, but a disabled webhook or a pre-webhook object could reach
	// here unset. Default() is idempotent.
	vc.Default()

	// Hibernation: scale the StatefulSet to zero (keeping PVCs) and stop active
	// management. A single short path for every topology — no bootstrap, survey
	// or failover while asleep.
	if hibernated(&vc) {
		return r.reconcileHibernated(ctx, &vc)
	}

	// In-place password rotation (valkey.wellcake.io/rotate-password): drive a live
	// CONFIG SET requirepass/masterauth across the pods with no restart, then
	// rewrite the Secret. Runs once per annotation token. Requires the pods to be
	// reachable; if not, the error just requeues until they are.
	if rotated, err := r.reconcilePasswordRotation(ctx, &vc); err != nil {
		return ctrl.Result{}, err
	} else if rotated {
		return ctrl.Result{Requeue: true}, nil
	}

	// In-place TLS cert reload (cert-manager renewal): re-read the renewed cert
	// onto live pods via CONFIG SET tls-*, no restart. reloaded → requeue once;
	// requeueAfter>0 → the mounted Secret volume hasn't synced on some pod yet.
	if reloaded, after, err := r.reconcileTLSReload(ctx, &vc); err != nil {
		return ctrl.Result{}, err
	} else if reloaded {
		return ctrl.Result{Requeue: true}, nil
	} else if after > 0 {
		return ctrl.Result{RequeueAfter: after}, nil
	}

	switch vc.Spec.Topology {
	case cachev1beta1.TopologyStandalone, cachev1beta1.TopologyReplication:
		return r.reconcileReplication(ctx, &vc)
	case cachev1beta1.TopologySentinel:
		return r.reconcileSentinel(ctx, &vc)
	case cachev1beta1.TopologyCluster:
		return r.reconcileCluster(ctx, &vc)
	default:
		return ctrl.Result{}, fmt.Errorf("unknown topology %q", vc.Spec.Topology)
	}
}

// reconcileReplication ensures Secret, Services, ConfigMap and StatefulSet for the Replication topology.
func (r *ValkeyClusterReconciler) reconcileReplication(ctx context.Context, vc *cachev1beta1.ValkeyCluster) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	password, err := r.ensurePasswordSecret(ctx, vc)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("password secret: %w", err)
	}

	if err := r.ensureHeadlessService(ctx, vc); err != nil {
		return ctrl.Result{}, fmt.Errorf("headless service: %w", err)
	}
	if err := r.ensureClientService(ctx, vc); err != nil {
		return ctrl.Result{}, fmt.Errorf("client service: %w", err)
	}
	configHash, err := r.ensureConfigMap(ctx, vc, password)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("configmap: %w", err)
	}

	sts, err := r.ensureStatefulSet(ctx, vc, configHash)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("statefulset: %w", err)
	}

	if err := r.ensurePDB(ctx, vc); err != nil {
		return ctrl.Result{}, fmt.Errorf("pdb: %w", err)
	}

	if err := r.ensureNetworkPolicy(ctx, vc); err != nil {
		return ctrl.Result{}, fmt.Errorf("networkpolicy: %w", err)
	}

	if err := r.ensureMetricsServiceMonitor(ctx, vc); err != nil {
		log.Error(err, "metrics ServiceMonitor")
	}

	if err := r.ensureBackupCronJob(ctx, vc); err != nil {
		log.Error(err, "backup cronjob")
	}

	// Operator-driven failover for Replication: survey live pods and, if the
	// expected primary is unreachable, promote the most up-to-date replica.
	// Skipped when this cluster is itself a downstream replica of an
	// external primary (spec.replicateFrom) — no local pod is allowed to
	// promote itself, as it would diverge from the upstream source. To
	// promote on DR, clear spec.replicateFrom; the operator will then run
	// failover on the next reconcile.
	primary := ""
	requeueAfter := time.Duration(0)
	if vc.Spec.Topology == cachev1beta1.TopologyReplication && sts.Status.ReadyReplicas > 0 && vc.Spec.ReplicateFrom == nil {
		survey := r.surveyReplication(ctx, vc, password)
		// Proactive rolling restart (ADR 0004): when opted in and a config rollout
		// is pending (some pod is on a stale config-hash), the operator owns the
		// rollout — roll replicas, then promote a fresh replica before deleting the
		// old primary. While a rollout is in flight we skip reactive failover so the
		// two don't fight over the primary, and requeue quickly to drive the next
		// step. With no rollout pending this is a no-op and reactive failover runs.
		if proactiveRolloutEnabled(vc) {
			rp, inProgress, rerr := r.driveReplicationRollout(ctx, vc, password, survey)
			if rerr != nil {
				return ctrl.Result{}, rerr
			}
			if inProgress {
				res, err := r.updateStatus(ctx, vc, sts, rp)
				if err == nil {
					res.RequeueAfter = 5 * time.Second
				}
				return res, err
			}
		}
		primary = r.reconcileFailover(ctx, vc, password, survey)
		// Manual failover request (valkey.wellcake.io/failover): promote a chosen
		// replica once per distinct token, even if the current primary is healthy.
		if tok := vc.Annotations[failoverAnnotation]; tok != "" && tok != vc.Status.LastFailoverToken {
			if np := r.manualFailover(ctx, vc, password, survey, tok); np != "" {
				primary = np
			}
		}
		// Pods don't generate watch events when they merely become unreachable
		// at the application layer; we requeue periodically so failover can
		// notice a hung primary even if the pod is still Running.
		requeueAfter = 15 * time.Second
	}

	res, err := r.updateStatus(ctx, vc, sts, primary)
	if err == nil && requeueAfter > 0 {
		res.RequeueAfter = requeueAfter
	}
	return res, err
}

func (r *ValkeyClusterReconciler) ensurePDB(ctx context.Context, vc *cachev1beta1.ValkeyCluster) error {
	name := vc.Name + "-pdb"
	key := types.NamespacedName{Namespace: vc.Namespace, Name: name}
	if !pdbEnabled(vc) {
		var existing policyv1.PodDisruptionBudget
		if err := r.Get(ctx, key, &existing); err == nil {
			return r.Delete(ctx, &existing)
		}
		return nil
	}
	desired := buildPDB(vc)
	if err := controllerutil.SetControllerReference(vc, desired, r.Scheme); err != nil {
		return err
	}
	var existing policyv1.PodDisruptionBudget
	err := r.Get(ctx, key, &existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	// Retry on the benign "object has been modified" conflict: under the proactive
	// rollout's fast requeues + pod churn our cached copy goes stale between Get
	// and Update, and returning that conflict would abort the whole reconcile
	// (including the rollout step downstream). Re-Get fresh and re-apply.
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var cur policyv1.PodDisruptionBudget
		if err := r.Get(ctx, key, &cur); err != nil {
			return err
		}
		cur.Spec = desired.Spec
		cur.Labels = desired.Labels
		return r.Update(ctx, &cur)
	})
}

func (r *ValkeyClusterReconciler) ensureNetworkPolicy(ctx context.Context, vc *cachev1beta1.ValkeyCluster) error {
	name := vc.Name + "-allow"
	key := types.NamespacedName{Namespace: vc.Namespace, Name: name}
	if !networkPolicyEnabled(vc) {
		var existing networkingv1.NetworkPolicy
		if err := r.Get(ctx, key, &existing); err == nil {
			return r.Delete(ctx, &existing)
		}
		return nil
	}
	desired := buildNetworkPolicy(vc)
	if err := controllerutil.SetControllerReference(vc, desired, r.Scheme); err != nil {
		return err
	}
	var existing networkingv1.NetworkPolicy
	err := r.Get(ctx, key, &existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	existing.Spec = desired.Spec
	existing.Labels = desired.Labels
	return r.Update(ctx, &existing)
}

func (r *ValkeyClusterReconciler) ensurePasswordSecret(ctx context.Context, vc *cachev1beta1.ValkeyCluster) (string, error) {
	if vc.Spec.Auth == nil || !vc.Spec.Auth.Enabled {
		return "", nil
	}
	if vc.Spec.Auth.ExistingSecret != "" {
		var sec corev1.Secret
		if err := r.Get(ctx, types.NamespacedName{Namespace: vc.Namespace, Name: vc.Spec.Auth.ExistingSecret}, &sec); err != nil {
			return "", err
		}
		return string(sec.Data[secretKeyPassword]), nil
	}

	name := passwordSecretName(vc)
	var existing corev1.Secret
	err := r.Get(ctx, types.NamespacedName{Namespace: vc.Namespace, Name: name}, &existing)
	if err == nil {
		return string(existing.Data[secretKeyPassword]), nil
	}
	if !apierrors.IsNotFound(err) {
		return "", err
	}

	pw, err := generatePassword(32)
	if err != nil {
		return "", err
	}
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: vc.Namespace,
			Labels:    labelsFor(vc),
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{secretKeyPassword: []byte(pw)},
	}
	if err := controllerutil.SetControllerReference(vc, sec, r.Scheme); err != nil {
		return "", err
	}
	if err := r.Create(ctx, sec); err != nil {
		return "", err
	}
	return pw, nil
}

func (r *ValkeyClusterReconciler) ensureHeadlessService(ctx context.Context, vc *cachev1beta1.ValkeyCluster) error {
	desired := buildHeadlessService(vc)
	if err := controllerutil.SetControllerReference(vc, desired, r.Scheme); err != nil {
		return err
	}
	return r.applyService(ctx, desired)
}

func (r *ValkeyClusterReconciler) ensureClientService(ctx context.Context, vc *cachev1beta1.ValkeyCluster) error {
	desired := buildClientService(vc)
	if err := controllerutil.SetControllerReference(vc, desired, r.Scheme); err != nil {
		return err
	}
	return r.applyService(ctx, desired)
}

func (r *ValkeyClusterReconciler) applyService(ctx context.Context, desired *corev1.Service) error {
	var existing corev1.Service
	err := r.Get(ctx, client.ObjectKeyFromObject(desired), &existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	existing.Spec.Ports = desired.Spec.Ports
	existing.Spec.Selector = desired.Spec.Selector
	existing.Labels = desired.Labels
	return r.Update(ctx, &existing)
}

// ensureConfigMap reconciles the ConfigMap and returns a stable hash of its
// final Data, which the StatefulSet uses as a pod template annotation so that
// pods roll whenever valkey.conf or entrypoint.sh changes.
func (r *ValkeyClusterReconciler) ensureConfigMap(ctx context.Context, vc *cachev1beta1.ValkeyCluster, password string) (string, error) {
	desired := buildConfigMap(vc, password)
	if err := controllerutil.SetControllerReference(vc, desired, r.Scheme); err != nil {
		return "", err
	}
	hash := configHashFromData(desired.Data)
	var existing corev1.ConfigMap
	err := r.Get(ctx, client.ObjectKeyFromObject(desired), &existing)
	if apierrors.IsNotFound(err) {
		if err := r.Create(ctx, desired); err != nil {
			return "", err
		}
		return hash, nil
	}
	if err != nil {
		return "", err
	}
	existing.Data = desired.Data
	existing.Labels = desired.Labels
	if err := r.Update(ctx, &existing); err != nil {
		return "", err
	}
	return hash, nil
}

func (r *ValkeyClusterReconciler) ensureStatefulSet(ctx context.Context, vc *cachev1beta1.ValkeyCluster, configHash string) (*appsv1.StatefulSet, error) {
	// Proactive (OnDelete) rollout is implemented for Replication, Cluster and
	// Sentinel; Standalone keeps RollingUpdate even when opted in (a single node
	// has nothing to fail over to).
	proactive := proactiveRolloutEnabled(vc) &&
		(vc.Spec.Topology == cachev1beta1.TopologyReplication ||
			vc.Spec.Topology == cachev1beta1.TopologyCluster ||
			vc.Spec.Topology == cachev1beta1.TopologySentinel)
	desired := buildStatefulSet(vc, configHash, proactive)
	sts, err := r.applyStatefulSet(ctx, vc, desired)
	if err != nil {
		return nil, err
	}
	if err := r.ensurePVCSize(ctx, vc); err != nil {
		return nil, err
	}
	return sts, nil
}

// applyStatefulSet creates the StatefulSet or reconciles its mutable fields
// (Replicas, Template, UpdateStrategy, Labels) in place. Shared by the
// single-StatefulSet path and the per-shard path (ADR 0005); PVC growth is the
// caller's concern (ensurePVCSize), run once across all shards.
func (r *ValkeyClusterReconciler) applyStatefulSet(ctx context.Context, vc *cachev1beta1.ValkeyCluster, desired *appsv1.StatefulSet) (*appsv1.StatefulSet, error) {
	if err := controllerutil.SetControllerReference(vc, desired, r.Scheme); err != nil {
		return nil, err
	}
	var existing appsv1.StatefulSet
	err := r.Get(ctx, client.ObjectKeyFromObject(desired), &existing)
	if apierrors.IsNotFound(err) {
		if err := r.Create(ctx, desired); err != nil {
			return nil, err
		}
		return desired, nil
	}
	if err != nil {
		return nil, err
	}
	// Reconcile the mutable fields, retrying on the benign "object has been
	// modified" conflict: under reconcile churn (pod events + the 15s failover
	// requeue + the StatefulSet controller writing status) our cached copy goes
	// stale between Get and Update. Returning that conflict as an error would
	// abort the WHOLE reconcile — including the failover survey downstream — and
	// delay promoting a lost/hung primary (surfaced by the hung-primary chaos
	// diagnostic). RetryOnConflict re-Gets a fresh copy and re-applies.
	//
	// NB: PodManagementPolicy and VolumeClaimTemplates are immutable on an
	// existing StatefulSet — we deliberately don't copy them. Storage growth is
	// applied to the live PVCs by ensurePVCSize (the STS template only governs
	// *new* pods).
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var cur appsv1.StatefulSet
		if err := r.Get(ctx, client.ObjectKeyFromObject(desired), &cur); err != nil {
			return err
		}
		cur.Spec.Replicas = desired.Spec.Replicas
		cur.Spec.Template = desired.Spec.Template
		cur.Spec.UpdateStrategy = desired.Spec.UpdateStrategy
		cur.Labels = desired.Labels
		if err := r.Update(ctx, &cur); err != nil {
			return err
		}
		existing = cur
		return nil
	}); err != nil {
		return nil, err
	}
	return &existing, nil
}

// ensurePVCSize grows the live data PVCs when spec.storage.size increases.
// A StatefulSet's volumeClaimTemplates are immutable, so a CR size bump never
// reaches the existing PVCs through the STS — we patch them directly here
// (requires the StorageClass to allow volume expansion). Only grows, never
// shrinks (CEL already forbids shrinking spec.storage.size).
func (r *ValkeyClusterReconciler) ensurePVCSize(ctx context.Context, vc *cachev1beta1.ValkeyCluster) error {
	if usesMemoryStorage(vc) {
		// tmpfs-backed data dir has no PVC to resize.
		return nil
	}
	desired := buildDataPVC(vc).Spec.Resources.Requests[corev1.ResourceStorage]
	if desired.IsZero() {
		return nil
	}
	var pvcs corev1.PersistentVolumeClaimList
	if err := r.List(ctx, &pvcs, client.InNamespace(vc.Namespace),
		client.MatchingLabels{instanceLabel: vc.Name}); err != nil {
		return err
	}
	// The instance-label selector also matches the Sentinel StatefulSet's PVCs —
	// in Sentinel topology the data pods and the Sentinel monitors share the same
	// app.kubernetes.io/{instance,component} labels (component is the lowercased
	// topology for both). The Sentinel PVCs have their own fixed 1Gi size and must
	// not be grown to the data PVC size, so exclude them by name. (Their name is
	// the only thing that distinguishes the two StatefulSets.)
	sentinelPVCPrefix := dataVolumeName + "-" + sentinelStatefulSetName(vc) + "-"
	for i := range pvcs.Items {
		p := &pvcs.Items[i]
		if strings.HasPrefix(p.Name, sentinelPVCPrefix) {
			continue
		}
		cur := p.Spec.Resources.Requests[corev1.ResourceStorage]
		if cur.Cmp(desired) >= 0 {
			continue
		}
		patch := client.MergeFrom(p.DeepCopy())
		p.Spec.Resources.Requests[corev1.ResourceStorage] = desired
		if err := r.Patch(ctx, p, patch); err != nil {
			return fmt.Errorf("expand pvc %s: %w", p.Name, err)
		}
		logf.FromContext(ctx).Info("expanded data PVC", "pvc", p.Name, "size", desired.String())
	}
	return nil
}

// ensureMetricsServiceMonitor creates/updates a Prometheus Operator
// ServiceMonitor (group monitoring.coreos.com/v1) when the user opts in.
// We use unstructured so the Prometheus Operator CRDs remain a soft
// dependency — if they are not installed in the cluster the API server
// returns a NoMatchError and we log it without failing the reconcile.
func (r *ValkeyClusterReconciler) ensureMetricsServiceMonitor(ctx context.Context, vc *cachev1beta1.ValkeyCluster) error {
	if vc.Spec.Metrics == nil || !vc.Spec.Metrics.Enabled || !vc.Spec.Metrics.ServiceMonitor {
		return nil
	}

	sm := &unstructured.Unstructured{}
	sm.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "monitoring.coreos.com",
		Version: "v1",
		Kind:    "ServiceMonitor",
	})
	sm.SetName(vc.Name)
	sm.SetNamespace(vc.Namespace)
	sm.SetLabels(labelsFor(vc))
	sm.Object["spec"] = map[string]any{
		"selector": map[string]any{
			"matchLabels": labelsFor(vc),
		},
		"namespaceSelector": map[string]any{
			"matchNames": []any{vc.Namespace},
		},
		"endpoints": []any{
			map[string]any{
				"port":     metricsPortName,
				"interval": "30s",
				"path":     "/metrics",
			},
		},
	}
	if err := controllerutil.SetControllerReference(vc, sm, r.Scheme); err != nil {
		return err
	}

	var existing unstructured.Unstructured
	existing.SetGroupVersionKind(sm.GroupVersionKind())
	err := r.Get(ctx, client.ObjectKeyFromObject(sm), &existing)
	switch {
	case apierrors.IsNotFound(err):
		return r.Create(ctx, sm)
	case meta.IsNoMatchError(err):
		// Prometheus Operator CRDs not installed; skip silently.
		return nil
	case err != nil:
		return err
	}
	existing.Object["spec"] = sm.Object["spec"]
	existing.SetLabels(sm.GetLabels())
	return r.Update(ctx, &existing)
}

// updateStatus is used by the Replication reconciler. The `observedPrimary`
// argument is the pod that the operator believes is currently primary after
// surveyReplication / reconcileFailover; an empty string means we have no
// signal yet (no Ready pods or non-Replication topology) — fall back to pod-0
// as a hint for users so kubectl get shows something meaningful.
func (r *ValkeyClusterReconciler) updateStatus(ctx context.Context, vc *cachev1beta1.ValkeyCluster, sts *appsv1.StatefulSet, observedPrimary string) (ctrl.Result, error) { //nolint:unparam // Result is always zero; kept for the reconcile-helper return shape
	phase := cachev1beta1.PhaseCreating
	if sts.Status.ReadyReplicas == *sts.Spec.Replicas && sts.Status.ReadyReplicas > 0 {
		phase = cachev1beta1.PhaseReady
	} else if sts.Status.ReadyReplicas > 0 {
		phase = cachev1beta1.PhaseUpdating
	}

	primary := observedPrimary
	if primary == "" && sts.Status.ReadyReplicas > 0 {
		primary = fmt.Sprintf("%s-0", sts.Name)
	}

	patch := client.MergeFrom(vc.DeepCopy())
	vc.Status.Phase = phase
	vc.Status.ReadyReplicas = sts.Status.ReadyReplicas
	vc.Status.Primary = primary
	vc.Status.InternalEndpoint = internalEndpoint(vc)
	vc.Status.ObservedGeneration = vc.Generation
	msg := fmt.Sprintf("%d/%d ready, primary=%s", sts.Status.ReadyReplicas, *sts.Spec.Replicas, primary)
	setCondition(&vc.Status.Conditions, metav1.Condition{
		Type:               cachev1beta1.ConditionAvailable,
		Status:             condStatus(phase == cachev1beta1.PhaseReady),
		Reason:             string(phase),
		Message:            msg,
		ObservedGeneration: vc.Generation,
		LastTransitionTime: metav1.Now(),
	})
	setReadyCondition(&vc.Status, phase == cachev1beta1.PhaseReady, string(phase), msg)
	if err := r.Status().Patch(ctx, vc, patch); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// setReadyCondition mirrors the kstatus-style Ready condition off whatever
// drives Available, so generic tooling (Crossplane auto-ready, kubectl wait,
// kstatus) sees a real readiness signal that flips only on actual transitions.
func setReadyCondition(st *cachev1beta1.ValkeyClusterStatus, ready bool, reason, msg string) {
	setCondition(&st.Conditions, metav1.Condition{
		Type:               cachev1beta1.ConditionReady,
		Status:             condStatus(ready),
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: st.ObservedGeneration,
		LastTransitionTime: metav1.Now(),
	})
}

// setPhase marks the cluster Failed with the given reason/message. (It only
// ever sets PhaseFailed — the happy-path phases are set by updateStatus.) The
// (ctrl.Result, error) shape lets callers `return r.setPhase(...)` directly.
//
//nolint:unparam // Result is intentionally zero — kept for the reconcile-helper return shape.
func (r *ValkeyClusterReconciler) setPhase(ctx context.Context, vc *cachev1beta1.ValkeyCluster, reason, msg string) (ctrl.Result, error) {
	patch := client.MergeFrom(vc.DeepCopy())
	vc.Status.Phase = cachev1beta1.PhaseFailed
	vc.Status.ObservedGeneration = vc.Generation
	setCondition(&vc.Status.Conditions, metav1.Condition{
		Type:               cachev1beta1.ConditionAvailable,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            msg,
		LastTransitionTime: metav1.Now(),
	})
	setReadyCondition(&vc.Status, false, reason, msg)
	return ctrl.Result{}, r.Status().Patch(ctx, vc, patch)
}

// reconcileHibernated scales the StatefulSet to zero (PVCs are retained by the
// StatefulSet controller) and reports a Hibernated phase. Waking up is just
// removing/flipping the annotation: the normal reconcile path then rebuilds the
// StatefulSet at its real replica count.
func (r *ValkeyClusterReconciler) reconcileHibernated(ctx context.Context, vc *cachev1beta1.ValkeyCluster) (ctrl.Result, error) { //nolint:unparam // Result is always zero; kept for the reconcile-helper return shape
	log := logf.FromContext(ctx)

	var sts appsv1.StatefulSet
	err := r.Get(ctx, types.NamespacedName{Namespace: vc.Namespace, Name: statefulSetName(vc)}, &sts)
	switch {
	case err == nil:
		if sts.Spec.Replicas == nil || *sts.Spec.Replicas != 0 {
			sts.Spec.Replicas = ptr.To[int32](0)
			if err := r.Update(ctx, &sts); err != nil {
				return ctrl.Result{}, fmt.Errorf("scaling down for hibernation: %w", err)
			}
			log.Info("hibernating: scaled StatefulSet to 0", "name", vc.Name)
		}
	case apierrors.IsNotFound(err):
		// Nothing to scale down yet.
	default:
		return ctrl.Result{}, err
	}

	patch := client.MergeFrom(vc.DeepCopy())
	vc.Status.Phase = cachev1beta1.PhaseHibernated
	vc.Status.ReadyReplicas = sts.Status.ReadyReplicas
	vc.Status.ObservedGeneration = vc.Generation
	const hibMsg = "cluster is hibernated (scaled to 0); remove the valkey.wellcake.io/hibernate annotation to wake it"
	setCondition(&vc.Status.Conditions, metav1.Condition{
		Type:               cachev1beta1.ConditionAvailable,
		Status:             metav1.ConditionFalse,
		Reason:             "Hibernated",
		Message:            hibMsg,
		ObservedGeneration: vc.Generation,
		LastTransitionTime: metav1.Now(),
	})
	setReadyCondition(&vc.Status, false, "Hibernated", hibMsg)
	return ctrl.Result{}, r.Status().Patch(ctx, vc, patch)
}

// handleDeletion runs topology-specific cleanup before removing the finalizer.
// For Cluster topology this is where future graceful resharding will live
// (CLUSTER FORGET / valkey-cli --cluster del-node for each pod before STS
// teardown). For now it logs and lets the StatefulSet/PVC owner refs do the
// teardown. PVCs are intentionally not deleted to avoid surprise data loss;
// operators must remove PVCs manually after CR deletion.
func (r *ValkeyClusterReconciler) handleDeletion(ctx context.Context, vc *cachev1beta1.ValkeyCluster) (ctrl.Result, error) { //nolint:unparam // Result is always zero; kept for the reconcile-helper return shape
	log := logf.FromContext(ctx)
	if !controllerutil.ContainsFinalizer(vc, finalizerName) {
		return ctrl.Result{}, nil
	}
	log.Info("ValkeyCluster is being deleted; running cleanup", "name", vc.Name, "topology", vc.Spec.Topology)

	// For Cluster topology we currently do not run reshard-away-from-everything
	// on delete: the whole cluster is going away, so resharding is pointless
	// busywork. Per-shard removal during normal operation is handled by the
	// scale-down Job (runClusterScaleDown). When backups land they will run
	// here on the way out.
	// For Sentinel/Replication: StatefulSet GC is sufficient — replicas can
	// shut down in any order.

	controllerutil.RemoveFinalizer(vc, finalizerName)
	if err := r.Update(ctx, vc); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// SetupWithManager wires the controller and owned objects.
func (r *ValkeyClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	concurrency := max(1, r.MaxConcurrentReconciles)
	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(controller.Options{MaxConcurrentReconciles: concurrency}).
		For(&cachev1beta1.ValkeyCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Secret{}, builder.MatchEveryOwner).
		Owns(&policyv1.PodDisruptionBudget{}).
		Owns(&networkingv1.NetworkPolicy{}).
		Owns(&batchv1.Job{}).
		Owns(&batchv1.CronJob{}).
		// Watch the data-plane pods directly. Pods are owned by the StatefulSet,
		// not the ValkeyCluster, so Owns() doesn't cover them — but a primary
		// pod going away must trigger an *immediate* reconcile so the operator
		// promotes a data-holding replica before the recreated (empty) primary
		// rejoins. Relying on the StatefulSet status event or the 15s poll is too
		// slow: the pod can be recreated faster than that, and the survivors then
		// resync from the empty primary (data loss — see cha-02/cha-03 chaos).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(mapPodToCluster)).
		// Watch the TLS cert Secret. cert-manager renews it in place (owned by the
		// Certificate, not the ValkeyCluster, so Owns() misses it); a renewal must
		// enqueue a reconcile so the operator reloads the cert onto live pods
		// without a restart.
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.mapTLSSecretToCluster)).
		Named("valkeycluster").
		Complete(r)
}

// mapTLSSecretToCluster routes a Secret event to any ValkeyCluster in the same
// namespace that uses it as its TLS cert Secret. The cert Secret carries no
// operator labels (cert-manager owns it), so we match by the resolved TLS secret
// name; the per-namespace list is cheap under the one-cluster-per-namespace DBaaS
// layout.
func (r *ValkeyClusterReconciler) mapTLSSecretToCluster(ctx context.Context, obj client.Object) []reconcile.Request {
	var list cachev1beta1.ValkeyClusterList
	if err := r.List(ctx, &list, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}
	var reqs []reconcile.Request
	for i := range list.Items {
		vc := &list.Items[i]
		if tlsEnabled(vc) && tlsSecretName(vc) == obj.GetName() {
			reqs = append(reqs, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: vc.Namespace, Name: vc.Name},
			})
		}
	}
	return reqs
}

// mapPodToCluster routes a Pod event to its owning ValkeyCluster reconcile.
// Filters to the operator's own pods by label so cluster-wide pod churn doesn't
// enqueue spurious reconciles; the instance label carries the CR name.
func mapPodToCluster(_ context.Context, obj client.Object) []reconcile.Request {
	l := obj.GetLabels()
	if l[managedByLabel] != operatorName || l[instanceLabel] == "" {
		return nil
	}
	return []reconcile.Request{{
		NamespacedName: types.NamespacedName{Namespace: obj.GetNamespace(), Name: l[instanceLabel]},
	}}
}
