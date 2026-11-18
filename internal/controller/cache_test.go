/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestManagedCacheFallbackRepairsUnlabelledStatefulSet(t *testing.T) {
	scheme := newTestScheme(t)
	vc := minimalCR()
	sts := buildStatefulSet(vc, "config-hash", false)
	sts.Annotations = map[string]string{
		appliedHashAnnotation: appliedSpecHash([]any{sts.Spec.Replicas, sts.Spec.Template, sts.Spec.UpdateStrategy, sts.Labels}),
	}
	delete(sts.Labels, managedByLabel)
	sts.Spec.Template.Spec.TerminationGracePeriodSeconds = new(int64(99))
	direct := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sts).Build()
	filtered := interceptor.NewClient(direct, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := c.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			if obj.GetLabels()[managedByLabel] != operatorName {
				return apierrors.NewNotFound(schema.GroupResource{}, key.Name)
			}
			return nil
		},
	})

	var got appsv1.StatefulSet
	key := client.ObjectKeyFromObject(sts)
	fallback := WithManagedCacheFallback(filtered, direct)
	if err := fallback.Get(t.Context(), key, &got); err != nil {
		t.Fatal(err)
	}
	if got.Labels[managedByLabel] != "" {
		t.Fatal("fixture already has the managed-by label")
	}

	r := &ValkeyClusterReconciler{Client: fallback, Scheme: scheme}
	if _, err := r.applyStatefulSet(t.Context(), vc, buildStatefulSet(vc, "config-hash", false)); err != nil {
		t.Fatal(err)
	}
	if err := direct.Get(t.Context(), key, &got); err != nil {
		t.Fatal(err)
	}
	if got.Labels[managedByLabel] != operatorName {
		t.Fatal("reconcile did not restore the managed-by label")
	}

	if got.Spec.Template.Spec.TerminationGracePeriodSeconds == nil || *got.Spec.Template.Spec.TerminationGracePeriodSeconds != 99 {
		t.Fatal("label repair overwrote the live pod template")
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

func TestDisabledResourceAvoidsRepeatedAPILookups(t *testing.T) {
	vc := minimalCR()
	vc.Generation = 2
	vc.Status.ObservedGeneration = 2
	scheme := newTestScheme(t)
	cached := fake.NewClientBuilder().WithScheme(scheme).Build()
	liveGets := 0
	live := interceptor.NewClient(cached, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			liveGets++
			return c.Get(ctx, key, obj, opts...)
		},
	})
	r := &ValkeyClusterReconciler{Client: WithManagedCacheFallback(cached, live), Scheme: scheme}
	ctx := t.Context()
	if err := r.ensureBackupCronJob(ctx, vc); err != nil {
		t.Fatal(err)
	}
	if liveGets != 0 {
		t.Fatalf("disabled backup requested %d live reads in a settled generation", liveGets)
	}
	vc.Generation++
	if err := r.ensureBackupCronJob(ctx, vc); err != nil {
		t.Fatal(err)
	}
	if liveGets != 1 {
		t.Fatalf("changed spec requested %d live reads, want 1", liveGets)
	}
}
