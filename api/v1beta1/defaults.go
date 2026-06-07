/*
Copyright 2026 The Wellcake Authors.
*/

package v1beta1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	defaultImage   = "valkey/valkey:8.0"
	defaultPVCSize = "10Gi"
)

// Default fills unset spec fields with their conventional values. It is the
// single source of truth for defaulting, invoked both by the mutating admission
// webhook (so `kubectl apply`/`get` shows the effective spec and there is no
// defaulting race) and defensively from reconcile (in case the webhook is
// disabled or the object predates it). It must be idempotent.
func (vc *ValkeyCluster) Default() {
	if vc.Spec.Topology == "" {
		vc.Spec.Topology = TopologyReplication
	}
	if vc.Spec.Profile == "" {
		vc.Spec.Profile = ProfileCache
	}
	if vc.Spec.Image == "" {
		vc.Spec.Image = defaultImage
	}
	if vc.Spec.ImagePullPolicy == "" {
		vc.Spec.ImagePullPolicy = corev1.PullIfNotPresent
	}
	if vc.Spec.Replicas == 0 {
		if vc.Spec.Topology == TopologyStandalone {
			vc.Spec.Replicas = 1
		} else {
			vc.Spec.Replicas = 3
		}
	}
	// Durable profile defaults: PVC-backed persistence (RDB+AOF).
	if vc.Spec.Profile == ProfileDurable && vc.Spec.Storage == nil {
		vc.Spec.Storage = &StorageSpec{
			Size: resource.MustParse(defaultPVCSize),
			Mode: "both",
		}
	}
}
