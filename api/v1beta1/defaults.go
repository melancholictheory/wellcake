/*
Copyright 2026 The Wellcake Authors.
*/

package v1beta1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"
)

const (
	defaultImage   = "valkey/valkey:8.0"
	defaultPVCSize = "10Gi"
	// defaultBackupRetention mirrors the +kubebuilder:default on BackupSpec.Retention.
	defaultBackupRetention int32 = 7
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

// The accessors below resolve the optional fields whose CRD default differs
// from their zero value. Those fields are pointers so that an explicit false/0
// survives a round-trip through the Go types; nil means "not set" and resolves
// to the same default the CRD applies, so callers behave identically whether or
// not the API server defaulted the object.

// AuthEnabled reports whether password authentication is on. No auth block
// means off; an auth block without enabled means the CRD default (on).
func (s *ValkeyClusterSpec) AuthEnabled() bool {
	return s.Auth != nil && ptr.Deref(s.Auth.Enabled, true)
}

// AutoReshardEnabled reports whether slot rebalancing runs automatically on
// scale-up/down (CRD default: true).
func (s *ValkeyClusterSpec) AutoReshardEnabled() bool {
	return ptr.Deref(s.AutoReshard, true)
}

// RetentionCount is the number of snapshots to keep; 0 keeps them all
// (CRD default: 7).
func (b *BackupSpec) RetentionCount() int32 {
	return ptr.Deref(b.Retention, defaultBackupRetention)
}
