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
	"maps"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// ManagedObjectCache restricts list/watch for managed workloads and their
// secondary resources. Secrets use a separate metadata-only watch.
func ManagedObjectCache() map[client.Object]cache.ByObject {
	managed := cache.ByObject{Label: labels.SelectorFromSet(labels.Set{managedByLabel: operatorName})}
	return map[client.Object]cache.ByObject{
		&corev1.Pod{}:                   managed,
		&appsv1.StatefulSet{}:           managed,
		&corev1.Service{}:               managed,
		&corev1.ConfigMap{}:             managed,
		&policyv1.PodDisruptionBudget{}: managed,
		&networkingv1.NetworkPolicy{}:   managed,
		&batchv1.Job{}:                  managed,
		&batchv1.CronJob{}:              managed,
	}
}

type managedCacheClient struct {
	client.Client
	reader client.Reader
}

// WithManagedCacheFallback preserves named lookups of objects with missing
// labels, such as objects from an older operator or after a user edits labels.
// They can be reconciled in place while informer lists stay label-filtered.
func WithManagedCacheFallback(c client.Client, reader client.Reader) client.Client {
	return managedCacheClient{Client: c, reader: reader}
}

func (c managedCacheClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	err := c.Client.Get(ctx, key, obj, opts...)
	if !apierrors.IsNotFound(err) {
		return err
	}
	switch obj.(type) {
	case *appsv1.StatefulSet, *corev1.Service, *corev1.ConfigMap, *policyv1.PodDisruptionBudget,
		*networkingv1.NetworkPolicy, *batchv1.Job, *batchv1.CronJob:
		return c.reader.Get(ctx, key, obj, opts...)
	default:
		return err
	}
}

// A disabled resource in an already observed generation is usually absent.
// Use the filtered cache then. During a spec change, check the API as well so
// disabling a resource can still delete an older object that lacks its label.
func (r *ValkeyClusterReconciler) removeOptionalResource(ctx context.Context, vc *cachev1beta1.ValkeyCluster, key client.ObjectKey, obj client.Object) error {
	reader := client.Reader(r.Client)
	if cached, ok := r.Client.(managedCacheClient); ok && vc.Status.ObservedGeneration >= vc.Generation {
		reader = cached.Client
	}
	if err := reader.Get(ctx, key, obj); err != nil {
		return client.IgnoreNotFound(err)
	}
	return client.IgnoreNotFound(r.Delete(ctx, obj))
}

func patchManagedLabels(ctx context.Context, c client.Client, obj client.Object, desired map[string]string) error {
	if labelsContained(desired, obj.GetLabels()) {
		return nil
	}
	before := obj.DeepCopyObject().(client.Object)
	current := obj.GetLabels()
	if current == nil {
		current = make(map[string]string, len(desired))
	}
	maps.Copy(current, desired)
	obj.SetLabels(current)
	return c.Patch(ctx, obj, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
}
