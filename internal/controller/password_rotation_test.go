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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// TestPasswordRotationGuards covers the cases where reconcilePasswordRotation
// must short-circuit WITHOUT touching pods (no dial): auth off, user-managed
// ExistingSecret, no annotation, and a token already acted on. Only a fresh
// token on an operator-managed Secret would proceed to dial pods, which needs a
// live cluster (covered by the e2e/local validation).
func TestPasswordRotationGuards(t *testing.T) {
	scheme := newTestScheme(t)
	mk := func(mut func(vc *cachev1beta1.ValkeyCluster)) *cachev1beta1.ValkeyCluster {
		vc := &cachev1beta1.ValkeyCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
			Spec: cachev1beta1.ValkeyClusterSpec{
				Topology: cachev1beta1.TopologyReplication,
				Replicas: 3,
				Auth:     &cachev1beta1.AuthSpec{Enabled: new(true)},
			},
		}
		mut(vc)
		return vc
	}

	cases := []struct {
		name string
		vc   *cachev1beta1.ValkeyCluster
	}{
		{"auth disabled", mk(func(vc *cachev1beta1.ValkeyCluster) {
			vc.Spec.Auth = &cachev1beta1.AuthSpec{Enabled: new(false)}
			vc.Annotations = map[string]string{rotatePasswordAnnotation: "t1"}
		})},
		{"existing (user-managed) secret", mk(func(vc *cachev1beta1.ValkeyCluster) {
			vc.Spec.Auth.ExistingSecret = "my-secret"
			vc.Annotations = map[string]string{rotatePasswordAnnotation: "t1"}
		})},
		{"no annotation", mk(func(vc *cachev1beta1.ValkeyCluster) {})},
		{"token already acted on", mk(func(vc *cachev1beta1.ValkeyCluster) {
			vc.Annotations = map[string]string{rotatePasswordAnnotation: "t1"}
			vc.Status.LastPasswordRotationToken = "t1"
		})},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.vc).Build()
			r := &ValkeyClusterReconciler{Client: c, Scheme: scheme}
			rotated, err := r.reconcilePasswordRotation(context.Background(), tc.vc)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if rotated {
				t.Fatalf("expected no rotation (guard), got rotated=true")
			}
		})
	}
}

// TestPasswordRotationAPIReaderGuard simulates the cache-lag race: the cached
// object still shows an empty token (so the cheap guard would proceed), but the
// authoritative APIReader already reflects the token as acted-on. The rotation
// must NOT re-run.
func TestPasswordRotationAPIReaderGuard(t *testing.T) {
	scheme := newTestScheme(t)
	// Cached view: annotation set, status token still empty (lagging).
	cached := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "c", Namespace: "ns",
			Annotations: map[string]string{rotatePasswordAnnotation: "t1"},
		},
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology: cachev1beta1.TopologyReplication,
			Replicas: 3,
			Auth:     &cachev1beta1.AuthSpec{Enabled: new(true)},
		},
	}
	// Authoritative view: token already recorded.
	authoritative := cached.DeepCopy()
	authoritative.Status.LastPasswordRotationToken = "t1"

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cached).Build()
	apiReader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(authoritative).Build()
	r := &ValkeyClusterReconciler{Client: c, APIReader: apiReader, Scheme: scheme}

	rotated, err := r.reconcilePasswordRotation(context.Background(), cached)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rotated {
		t.Fatalf("expected APIReader guard to skip the already-done rotation")
	}
}

// TestPasswordRotationWaitsForPods: a fresh token on a cluster whose data pods
// are not all Ready (a cluster created with the annotation) is deferred without
// an error, and without touching the Secret. An error here returned before the
// reconcile created the StatefulSet, so such a cluster never came up.
func TestPasswordRotationWaitsForPods(t *testing.T) {
	scheme := newTestScheme(t)
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "c", Namespace: "ns",
			Annotations: map[string]string{rotatePasswordAnnotation: "t1"},
		},
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology: cachev1beta1.TopologySentinel,
			Replicas: 2,
			Auth:     &cachev1beta1.AuthSpec{Enabled: new(true)},
		},
	}
	pod := func(name string, ready bool) *corev1.Pod {
		st := corev1.ConditionFalse
		if ready {
			st = corev1.ConditionTrue
		}
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", Labels: map[string]string{instanceLabel: "c", componentLabel: componentSentinel}},
			Status:     corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: st}}},
		}
	}
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: passwordSecretName(vc), Namespace: "ns"},
		Data:       map[string][]byte{secretKeyPassword: []byte("old")},
	}
	for name, objs := range map[string][]client.Object{
		"no pods yet":                  {vc, sec},
		"one data pod not Ready":       {vc, sec, pod("c-0", true), pod("c-1", false)},
		"Ready Sentinels do not count": {vc, sec, pod("c-0", true), pod("c-sentinel-0", true), pod("c-sentinel-1", true)},
	} {
		t.Run(name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
			r := &ValkeyClusterReconciler{Client: c, Scheme: scheme}
			rotated, err := r.reconcilePasswordRotation(context.Background(), vc)
			if err != nil || rotated {
				t.Fatalf("want deferred (false, nil), got (%v, %v)", rotated, err)
			}
			var got corev1.Secret
			if err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: passwordSecretName(vc)}, &got); err != nil || string(got.Data[secretKeyPassword]) != "old" {
				t.Fatalf("Secret must be untouched, got %q (%v)", got.Data[secretKeyPassword], err)
			}
		})
	}
}

// TestUpdatePasswordSecret verifies the Secret is rewritten in place.
func TestUpdatePasswordSecret(t *testing.T) {
	scheme := newTestScheme(t)
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
		Spec:       cachev1beta1.ValkeyClusterSpec{Topology: cachev1beta1.TopologyReplication},
	}
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: passwordSecretName(vc), Namespace: "ns"},
		Data:       map[string][]byte{secretKeyPassword: []byte("old")},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(vc, sec).Build()
	r := &ValkeyClusterReconciler{Client: c, Scheme: scheme}

	if err := r.updatePasswordSecret(context.Background(), vc, "newpass"); err != nil {
		t.Fatalf("updatePasswordSecret: %v", err)
	}
	var got corev1.Secret
	if err := c.Get(context.Background(), types.NamespacedName{Name: sec.Name, Namespace: sec.Namespace}, &got); err != nil {
		t.Fatalf("get secret: %v", err)
	}
	if string(got.Data[secretKeyPassword]) != "newpass" {
		t.Errorf("password = %q, want newpass", string(got.Data[secretKeyPassword]))
	}
}
