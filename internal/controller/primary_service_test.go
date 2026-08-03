/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

func TestBuildPrimaryServiceSelectsPrimaryRole(t *testing.T) {
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "vk"},
		Spec:       cachev1beta1.ValkeyClusterSpec{Topology: cachev1beta1.TopologyReplication},
	}
	svc := buildPrimaryService(vc)

	if svc.Name != "vk-primary" {
		t.Fatalf("name=%q, want vk-primary", svc.Name)
	}
	if svc.Spec.Selector[roleLabel] != rolePrimary {
		t.Fatalf("selector must include %s=%s, got %v", roleLabel, rolePrimary, svc.Spec.Selector)
	}
	// Must still scope to this cluster, not just any primary.
	if svc.Spec.Selector[instanceLabel] != "vk" {
		t.Fatalf("selector must keep the instance scope, got %v", svc.Spec.Selector)
	}
	// The Service's own metadata labels must NOT carry the role selector.
	if _, ok := svc.Labels[roleLabel]; ok {
		t.Fatalf("service metadata labels must not include the role selector, got %v", svc.Labels)
	}
}

// TestEnsureRoleLabels covers stamping, idempotency (no resourceVersion churn),
// failover swap, and the empty-primary no-op.
func TestEnsureRoleLabels(t *testing.T) {
	scheme := newTestScheme(t)
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "vk"},
		Spec:       cachev1beta1.ValkeyClusterSpec{Topology: cachev1beta1.TopologyReplication},
	}
	mkPod := func(name string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name, Labels: labelsFor(vc)}}
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mkPod("vk-0"), mkPod("vk-1")).Build()
	r := &ValkeyClusterReconciler{Client: c, Scheme: scheme}
	ctx := context.Background()

	get := func(name string) corev1.Pod {
		var p corev1.Pod
		if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: name}, &p); err != nil {
			t.Fatalf("get %s: %v", name, err)
		}
		return p
	}

	if err := r.ensureRoleLabels(ctx, vc, "vk-0"); err != nil {
		t.Fatalf("ensureRoleLabels: %v", err)
	}
	if r0 := get("vk-0").Labels[roleLabel]; r0 != rolePrimary {
		t.Fatalf("vk-0 role=%q, want primary", r0)
	}
	if r1 := get("vk-1").Labels[roleLabel]; r1 != roleReplica {
		t.Fatalf("vk-1 role=%q, want replica", r1)
	}

	// Idempotent: a second identical call must not write (stable resourceVersion).
	rv0, rv1 := get("vk-0").ResourceVersion, get("vk-1").ResourceVersion
	if err := r.ensureRoleLabels(ctx, vc, "vk-0"); err != nil {
		t.Fatalf("ensureRoleLabels (2): %v", err)
	}
	if get("vk-0").ResourceVersion != rv0 || get("vk-1").ResourceVersion != rv1 {
		t.Fatal("second call churned resourceVersion — not idempotent")
	}

	// Failover: primary moves to vk-1, labels swap.
	if err := r.ensureRoleLabels(ctx, vc, "vk-1"); err != nil {
		t.Fatalf("ensureRoleLabels (3): %v", err)
	}
	if r0, r1 := get("vk-0").Labels[roleLabel], get("vk-1").Labels[roleLabel]; r0 != roleReplica || r1 != rolePrimary {
		t.Fatalf("after failover vk-0=%s vk-1=%s, want replica/primary", r0, r1)
	}

	// Empty primary (unknown / downstream replica) is a no-op, not an error.
	if err := r.ensureRoleLabels(ctx, vc, ""); err != nil {
		t.Fatalf("ensureRoleLabels(empty): %v", err)
	}
}
