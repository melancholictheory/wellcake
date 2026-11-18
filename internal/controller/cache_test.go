/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestManagedCacheFallbackRepairsUnlabelledStatefulSet(t *testing.T) {
	scheme := newTestScheme(t)
	vc := minimalCR()
	sts := buildStatefulSet(vc, "config-hash", false)
	sts.Annotations = map[string]string{
		appliedHashAnnotation: appliedSpecHash([]any{sts.Spec.Replicas, sts.Spec.Template, sts.Spec.UpdateStrategy, sts.Labels}),
	}
	delete(sts.Labels, managedByLabel)
	filtered := fake.NewClientBuilder().WithScheme(scheme).Build()
	direct := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sts).Build()

	var got appsv1.StatefulSet
	key := client.ObjectKeyFromObject(sts)
	fallback := WithManagedCacheFallback(filtered, direct)
	if err := fallback.Get(t.Context(), key, &got); err != nil {
		t.Fatal(err)
	}
	if got.Labels[managedByLabel] != "" {
		t.Fatal("fixture already has the managed-by label")
	}

	r := &ValkeyClusterReconciler{Client: direct, Scheme: scheme}
	if _, err := r.applyStatefulSet(t.Context(), vc, buildStatefulSet(vc, "config-hash", false)); err != nil {
		t.Fatal(err)
	}
	if err := direct.Get(t.Context(), key, &got); err != nil {
		t.Fatal(err)
	}
	if got.Labels[managedByLabel] != operatorName {
		t.Fatal("reconcile did not restore the managed-by label")
	}

	// Preserve the existing Pod cache semantics and avoid a lookup of a
	// foreign Pod whose absence is expected.
	pod := &corev1.Pod{}
	pod.Name, pod.Namespace = "foreign", vc.Namespace
	if err := direct.Create(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	if err := fallback.Get(t.Context(), client.ObjectKeyFromObject(pod), &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expected the foreign pod to stay outside the cache: %v", err)
	}
}

func TestManagedObjectCacheSelectorsExcludeForeignObjects(t *testing.T) {
	for object, options := range ManagedObjectCache() {
		if options.Label.Matches(labels.Set{}) || !options.Label.Matches(labels.Set{managedByLabel: operatorName}) {
			t.Errorf("%T cache selector must match only objects labelled as operator-managed", object)
		}
	}
}
