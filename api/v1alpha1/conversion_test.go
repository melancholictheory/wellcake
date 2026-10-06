/*
Copyright 2026 The Wellcake Authors.
*/

package v1alpha1

import (
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	v1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// TestValkeyClusterConversionRoundTrip verifies the v1alpha1 <-> v1beta1 hub
// conversion preserves the spec/status/objectmeta (the two versions are
// structurally identical today, so a round trip must be lossless).
func TestValkeyClusterConversionRoundTrip(t *testing.T) {
	src := &ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns", Labels: map[string]string{"k": "v"}},
		Spec: ValkeyClusterSpec{
			Topology:         TopologyCluster,
			Profile:          ProfileDurable,
			Image:            "valkey/valkey:9.1",
			Replicas:         3,
			Shards:           ptr.To[int32](3),
			ReplicasPerShard: ptr.To[int32](1),
			PerShardWorkload: ptr.To(true), // must survive round-trip (public composition writes v1alpha1)
			Auth:             &AuthSpec{Enabled: ptr.To(true), ExistingSecret: "s"},
			Logging:          &LoggingSpec{Format: "json"}, // guards the v1alpha1/v1beta1 json-tag parity
			// Explicit false/0 on fields whose CRD default differs: the JSON
			// round trip must keep them apart from "unset" (nil).
			AutoReshard:         ptr.To(false),
			PodDisruptionBudget: &PDBSpec{Enabled: ptr.To(false)},
			Backup:              &BackupSpec{Retention: ptr.To[int32](0), S3: &S3Spec{Bucket: "b", CredentialsSecret: "c"}},
		},
		Status: ValkeyClusterStatus{Phase: "Ready", ReadyReplicas: 6, ClusterInitialized: true},
	}

	hub := &v1beta1.ValkeyCluster{}
	if err := src.ConvertTo(hub); err != nil {
		t.Fatalf("ConvertTo: %v", err)
	}
	if hub.Spec.Topology != v1beta1.TopologyCluster || hub.Spec.Profile != v1beta1.ProfileDurable {
		t.Errorf("hub spec not carried: %+v", hub.Spec)
	}
	if hub.Spec.AutoReshard == nil || *hub.Spec.AutoReshard ||
		hub.Spec.Backup.Retention == nil || *hub.Spec.Backup.Retention != 0 ||
		hub.Spec.PodDisruptionBudget.Enabled == nil || *hub.Spec.PodDisruptionBudget.Enabled {
		t.Errorf("explicit false/0 not carried to the hub: %+v", hub.Spec)
	}
	if hub.Name != "ns" && hub.Name != "c" { // objectmeta carried
		t.Errorf("hub objectmeta name = %q", hub.Name)
	}

	back := &ValkeyCluster{}
	if err := back.ConvertFrom(hub); err != nil {
		t.Fatalf("ConvertFrom: %v", err)
	}
	if !reflect.DeepEqual(src.Spec, back.Spec) {
		t.Errorf("spec lost in round trip:\n src=%+v\nback=%+v", src.Spec, back.Spec)
	}
	if !reflect.DeepEqual(src.Status, back.Status) {
		t.Errorf("status lost in round trip:\n src=%+v\nback=%+v", src.Status, back.Status)
	}
	if back.Name != src.Name || back.Namespace != src.Namespace {
		t.Errorf("objectmeta lost: %q/%q", back.Namespace, back.Name)
	}
}

// TestValkeyACLConversionRoundTrip mirrors the above for ValkeyACL.
func TestValkeyACLConversionRoundTrip(t *testing.T) {
	src := &ValkeyACL{
		ObjectMeta: metav1.ObjectMeta{Name: "acl", Namespace: "ns"},
		Spec: ValkeyACLSpec{
			Users: []ValkeyACLUser{{Name: "alice", Rules: "on ~* +@read"}},
		},
		Status: ValkeyACLStatus{AppliedUsers: []string{"alice"}},
	}
	hub := &v1beta1.ValkeyACL{}
	if err := src.ConvertTo(hub); err != nil {
		t.Fatalf("ConvertTo: %v", err)
	}
	back := &ValkeyACL{}
	if err := back.ConvertFrom(hub); err != nil {
		t.Fatalf("ConvertFrom: %v", err)
	}
	if !reflect.DeepEqual(src.Spec, back.Spec) || !reflect.DeepEqual(src.Status, back.Status) {
		t.Errorf("ACL round trip lossy:\n src=%+v\nback=%+v", src, back)
	}
}
