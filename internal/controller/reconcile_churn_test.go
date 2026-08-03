/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// The fake client bumps resourceVersion on every Update, including a no-op one, so
// a stable resourceVersion across a reconcile proves the diff gate skipped the
// write (the fleet-scale churn this change fixes), and a changed one proves a real
// change still lands.

func rvOf(t *testing.T, c client.Client, obj client.Object, key types.NamespacedName) string {
	t.Helper()
	if err := c.Get(context.Background(), key, obj); err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	return obj.GetResourceVersion()
}

// applyService must skip a no-op and MUST update on a port shrink — the exact
// DeepDerivative prefix-match bug (turning metrics off drops a port from desired
// but a derivative check treats the shorter list as already-settled).
func TestApplyServiceSkipsNoOpAndCatchesPortShrink(t *testing.T) {
	scheme := newTestScheme(t)
	ns, name := "ns", "vk"
	key := types.NamespacedName{Namespace: ns, Name: name}

	mk := func(withMetrics bool) *corev1.Service {
		ports := []corev1.ServicePort{{
			Name: "valkey", Port: 6379, TargetPort: intstr.FromInt32(6379), Protocol: corev1.ProtocolTCP,
		}}
		if withMetrics {
			ports = append(ports, corev1.ServicePort{
				Name: "metrics", Port: 9121, TargetPort: intstr.FromInt32(9121), Protocol: corev1.ProtocolTCP,
			})
		}
		return &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Labels: map[string]string{"app": "vk"}},
			Spec:       corev1.ServiceSpec{Ports: ports, Selector: map[string]string{"app": "vk"}},
		}
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mk(true)).Build()
	r := &ValkeyClusterReconciler{Client: c, Scheme: scheme}
	rv0 := rvOf(t, c, &corev1.Service{}, key)

	// no-op: identical desired must not write.
	if err := r.applyService(context.Background(), mk(true)); err != nil {
		t.Fatalf("applyService no-op: %v", err)
	}
	if rv := rvOf(t, c, &corev1.Service{}, key); rv != rv0 {
		t.Fatalf("no-op applyService bumped resourceVersion %s->%s (churn)", rv0, rv)
	}

	// shrink: metrics off → one port → MUST update and actually drop the port.
	if err := r.applyService(context.Background(), mk(false)); err != nil {
		t.Fatalf("applyService shrink: %v", err)
	}
	var got corev1.Service
	rv := rvOf(t, c, &got, key)
	if rv == rv0 {
		t.Fatal("port shrink was skipped (DeepDerivative prefix-match bug)")
	}
	if len(got.Spec.Ports) != 1 {
		t.Fatalf("metrics port not removed: %d ports remain", len(got.Spec.Ports))
	}
}

// ensureNetworkPolicy: same shrink risk on Ingress[0].Ports when metrics is
// toggled off.
func TestEnsureNetworkPolicySkipsNoOpAndCatchesPortShrink(t *testing.T) {
	scheme := newTestScheme(t)
	vc := minimalCR()
	vc.Namespace, vc.Name, vc.UID = "ns", "vk", types.UID("u1")
	vc.Spec.NetworkPolicy = &cachev1beta1.NetworkPolicySpec{Enabled: true}
	vc.Spec.Metrics = &cachev1beta1.MetricsSpec{Enabled: true}
	key := types.NamespacedName{Namespace: "ns", Name: "vk-allow"}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(vc).Build()
	r := &ValkeyClusterReconciler{Client: c, Scheme: scheme}

	if err := r.ensureNetworkPolicy(context.Background(), vc); err != nil {
		t.Fatalf("initial ensureNetworkPolicy: %v", err)
	}
	rv0 := rvOf(t, c, &networkingv1.NetworkPolicy{}, key)

	// no-op.
	if err := r.ensureNetworkPolicy(context.Background(), vc); err != nil {
		t.Fatalf("no-op ensureNetworkPolicy: %v", err)
	}
	if rv := rvOf(t, c, &networkingv1.NetworkPolicy{}, key); rv != rv0 {
		t.Fatalf("no-op ensureNetworkPolicy bumped resourceVersion %s->%s (churn)", rv0, rv)
	}

	// shrink: metrics off removes the exporter port from the ingress rule.
	vc.Spec.Metrics.Enabled = false
	if err := r.ensureNetworkPolicy(context.Background(), vc); err != nil {
		t.Fatalf("shrink ensureNetworkPolicy: %v", err)
	}
	var np networkingv1.NetworkPolicy
	if rv := rvOf(t, c, &np, key); rv == rv0 {
		t.Fatal("NetworkPolicy port shrink was skipped (DeepDerivative prefix-match bug)")
	}
	if len(np.Spec.Ingress) == 0 {
		t.Fatal("ingress rule missing")
	}
	if n := len(np.Spec.Ingress[0].Ports); n != 1 {
		t.Fatalf("exporter port not removed: %d ingress ports remain", n)
	}
}

// applyStatefulSet must remove the metrics exporter sidecar when metrics is
// turned off. This is the shrink the earlier DeepDerivative gate silently
// skipped (a shorter container list looks like a prefix of the live one); the
// hash gate catches it.
func TestApplyStatefulSetCatchesSidecarShrink(t *testing.T) {
	scheme := newTestScheme(t)
	vc := minimalCR()
	vc.Namespace, vc.Name, vc.UID = "ns", "vk", types.UID("u1")
	vc.Spec.Replicas = 1
	vc.Spec.Metrics = &cachev1beta1.MetricsSpec{Enabled: true}
	key := types.NamespacedName{Namespace: "ns", Name: statefulSetName(vc)}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(vc).Build()
	r := &ValkeyClusterReconciler{Client: c, Scheme: scheme}

	if _, err := r.applyStatefulSet(context.Background(), vc, buildStatefulSet(vc, "hash", false)); err != nil {
		t.Fatalf("initial applyStatefulSet: %v", err)
	}
	var withMetrics appsv1.StatefulSet
	rv0 := rvOf(t, c, &withMetrics, key)
	nWith := len(withMetrics.Spec.Template.Spec.Containers)
	if nWith < 2 {
		t.Fatalf("expected an exporter sidecar with metrics on, got %d container(s)", nWith)
	}

	// settle: identical reconcile is a no-op.
	if _, err := r.applyStatefulSet(context.Background(), vc, buildStatefulSet(vc, "hash", false)); err != nil {
		t.Fatalf("no-op applyStatefulSet: %v", err)
	}
	if rv := rvOf(t, c, &appsv1.StatefulSet{}, key); rv != rv0 {
		t.Fatalf("no-op bumped resourceVersion %s->%s", rv0, rv)
	}

	// metrics off → the exporter container MUST be dropped.
	vc.Spec.Metrics.Enabled = false
	if _, err := r.applyStatefulSet(context.Background(), vc, buildStatefulSet(vc, "hash", false)); err != nil {
		t.Fatalf("metrics-off applyStatefulSet: %v", err)
	}
	var noMetrics appsv1.StatefulSet
	if rv := rvOf(t, c, &noMetrics, key); rv == rv0 {
		t.Fatal("sidecar shrink was skipped (prefix-match bug)")
	}
	if n := len(noMetrics.Spec.Template.Spec.Containers); n != nWith-1 {
		t.Fatalf("metrics sidecar not removed: %d containers remain (was %d)", n, nWith)
	}
}

// applyStatefulSet must skip a no-op reconcile (the common steady-state case).
func TestApplyStatefulSetSkipsNoOp(t *testing.T) {
	scheme := newTestScheme(t)
	vc := minimalCR()
	vc.Namespace, vc.Name, vc.UID = "ns", "vk", types.UID("u1")
	vc.Spec.Replicas = 3
	key := types.NamespacedName{Namespace: "ns", Name: statefulSetName(vc)}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(vc).Build()
	r := &ValkeyClusterReconciler{Client: c, Scheme: scheme}

	if _, err := r.applyStatefulSet(context.Background(), vc, buildStatefulSet(vc, "hash", false)); err != nil {
		t.Fatalf("initial applyStatefulSet: %v", err)
	}
	rv0 := rvOf(t, c, &appsv1.StatefulSet{}, key)

	// A second identical reconcile must not write.
	if _, err := r.applyStatefulSet(context.Background(), vc, buildStatefulSet(vc, "hash", false)); err != nil {
		t.Fatalf("no-op applyStatefulSet: %v", err)
	}
	if rv := rvOf(t, c, &appsv1.StatefulSet{}, key); rv != rv0 {
		t.Fatalf("no-op applyStatefulSet bumped resourceVersion %s->%s (churn)", rv0, rv)
	}

	// A real change (replica count) must write.
	vc.Spec.Replicas = 5
	if _, err := r.applyStatefulSet(context.Background(), vc, buildStatefulSet(vc, "hash", false)); err != nil {
		t.Fatalf("scale applyStatefulSet: %v", err)
	}
	var sts appsv1.StatefulSet
	if rv := rvOf(t, c, &sts, key); rv == rv0 {
		t.Fatal("replica change was skipped")
	}
	if *sts.Spec.Replicas != 5 {
		t.Fatalf("replicas not updated: %d", *sts.Spec.Replicas)
	}
}

// ensureBackupCronJob is hash-gated like the STS: a no-op reconcile must not
// write, and turning TLS off must drop the TLS volume from the Job pod template
// (the shrink class the hash gate exists for).
func TestEnsureBackupCronJobSkipsNoOpAndCatchesVolumeShrink(t *testing.T) {
	scheme := newTestScheme(t)
	vc := minimalCR()
	vc.Namespace, vc.Name, vc.UID = "ns", "vk", types.UID("u1")
	vc.Spec.Backup = &cachev1beta1.BackupSpec{Enabled: true, S3: &cachev1beta1.S3Spec{Bucket: "b"}}
	vc.Spec.TLS = &cachev1beta1.TLSSpec{Enabled: true}
	key := types.NamespacedName{Namespace: "ns", Name: "vk-backup"}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(vc).Build()
	r := &ValkeyClusterReconciler{Client: c, Scheme: scheme}

	if err := r.ensureBackupCronJob(context.Background(), vc); err != nil {
		t.Fatalf("initial ensureBackupCronJob: %v", err)
	}
	var withTLS batchv1.CronJob
	rv0 := rvOf(t, c, &withTLS, key)
	nVol := len(withTLS.Spec.JobTemplate.Spec.Template.Spec.Volumes)

	// no-op.
	if err := r.ensureBackupCronJob(context.Background(), vc); err != nil {
		t.Fatalf("no-op ensureBackupCronJob: %v", err)
	}
	if rv := rvOf(t, c, &batchv1.CronJob{}, key); rv != rv0 {
		t.Fatalf("no-op ensureBackupCronJob bumped resourceVersion %s->%s (churn)", rv0, rv)
	}

	// TLS off → the TLS volume must be removed.
	vc.Spec.TLS.Enabled = false
	if err := r.ensureBackupCronJob(context.Background(), vc); err != nil {
		t.Fatalf("tls-off ensureBackupCronJob: %v", err)
	}
	var noTLS batchv1.CronJob
	if rv := rvOf(t, c, &noTLS, key); rv == rv0 {
		t.Fatal("TLS volume shrink was skipped")
	}
	if n := len(noTLS.Spec.JobTemplate.Spec.Template.Spec.Volumes); n >= nVol {
		t.Fatalf("TLS volume not removed: %d volumes remain (was %d)", n, nVol)
	}
}

// ensurePDB skips a no-op and still updates when the budget switches from
// MaxUnavailable to MinAvailable (the DeepDerivative gate that stays on PDB).
func TestEnsurePDBSkipsNoOpAndSwitchesBudget(t *testing.T) {
	scheme := newTestScheme(t)
	vc := minimalCR()
	vc.Namespace, vc.Name, vc.UID = "ns", "vk", types.UID("u1")
	vc.Spec.Replicas = 3 // PDB defaults on for replicas > 1
	key := types.NamespacedName{Namespace: "ns", Name: "vk-pdb"}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(vc).Build()
	r := &ValkeyClusterReconciler{Client: c, Scheme: scheme}

	if err := r.ensurePDB(context.Background(), vc); err != nil {
		t.Fatalf("initial ensurePDB: %v", err)
	}
	rv0 := rvOf(t, c, &policyv1.PodDisruptionBudget{}, key)

	// no-op.
	if err := r.ensurePDB(context.Background(), vc); err != nil {
		t.Fatalf("no-op ensurePDB: %v", err)
	}
	if rv := rvOf(t, c, &policyv1.PodDisruptionBudget{}, key); rv != rv0 {
		t.Fatalf("no-op ensurePDB bumped resourceVersion %s->%s (churn)", rv0, rv)
	}

	// switch maxUnavailable -> minAvailable: DeepDerivative sees the newly-set
	// pointer and must update.
	mn := intstr.FromInt32(2)
	vc.Spec.PodDisruptionBudget = &cachev1beta1.PDBSpec{Enabled: true, MinAvailable: &mn}
	if err := r.ensurePDB(context.Background(), vc); err != nil {
		t.Fatalf("switch ensurePDB: %v", err)
	}
	var pdb policyv1.PodDisruptionBudget
	if rv := rvOf(t, c, &pdb, key); rv == rv0 {
		t.Fatal("budget switch was skipped")
	}
	if pdb.Spec.MinAvailable == nil || pdb.Spec.MaxUnavailable != nil {
		t.Fatalf("budget not switched: min=%v max=%v", pdb.Spec.MinAvailable, pdb.Spec.MaxUnavailable)
	}
}

// ensureConfigMap returns the pod-rollout hash and must skip a no-op reconcile;
// a real config change (here a rotated password re-rendering valkey.conf) must
// still write and change the hash. Data is fully operator-owned with no
// API-server-defaulted keys, so the gate is an exact DeepEqual.
func TestEnsureConfigMapSkipsNoOpAndUpdatesOnChange(t *testing.T) {
	scheme := newTestScheme(t)
	vc := minimalCR()
	vc.Namespace, vc.Name, vc.UID = "ns", "vk", types.UID("u1")
	key := types.NamespacedName{Namespace: "ns", Name: configMapName(vc)}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(vc).Build()
	r := &ValkeyClusterReconciler{Client: c, Scheme: scheme}

	// create.
	h0, err := r.ensureConfigMap(context.Background(), vc, "pw-1")
	if err != nil {
		t.Fatalf("initial ensureConfigMap: %v", err)
	}
	if h0 == "" {
		t.Fatal("ensureConfigMap returned an empty hash")
	}
	rv0 := rvOf(t, c, &corev1.ConfigMap{}, key)

	// no-op: identical desired must not write, and must return the same hash.
	h1, err := r.ensureConfigMap(context.Background(), vc, "pw-1")
	if err != nil {
		t.Fatalf("no-op ensureConfigMap: %v", err)
	}
	if h1 != h0 {
		t.Fatalf("no-op ensureConfigMap changed hash %s->%s", h0, h1)
	}
	if rv := rvOf(t, c, &corev1.ConfigMap{}, key); rv != rv0 {
		t.Fatalf("no-op ensureConfigMap bumped resourceVersion %s->%s (churn)", rv0, rv)
	}

	// change a config key: a rotated password re-renders valkey.conf → MUST write,
	// change the hash, and land the new Data.
	h2, err := r.ensureConfigMap(context.Background(), vc, "pw-2")
	if err != nil {
		t.Fatalf("changed ensureConfigMap: %v", err)
	}
	var got corev1.ConfigMap
	if rv := rvOf(t, c, &got, key); rv == rv0 {
		t.Fatal("config change was skipped (gate too aggressive)")
	}
	if h2 == h0 {
		t.Fatalf("hash unchanged after config change: %s", h2)
	}
	if configHashFromData(got.Data) != h2 {
		t.Fatalf("live Data does not reflect the change: returned hash %s, live-data hash %s", h2, configHashFromData(got.Data))
	}
}
