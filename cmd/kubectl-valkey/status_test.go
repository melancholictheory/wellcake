/*
Copyright 2026 The Wellcake Authors.
*/

package main

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

func TestFormatStatusCluster(t *testing.T) {
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "demo-ns"},
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology: cachev1beta1.TopologyCluster,
			Profile:  cachev1beta1.ProfileCache,
			Image:    "valkey/valkey:9.0",
			Shards:   ptr.To[int32](3),
		},
		Status: cachev1beta1.ValkeyClusterStatus{
			Phase:              cachev1beta1.PhaseReady,
			Shards:             3,
			ReadyShards:        2,
			ClusterInitialized: true,
			ShardDetails: []cachev1beta1.ShardStatus{
				{Index: 0, Primary: "demo-0", Replicas: []string{"demo-3"}, SlotCount: 5461, Health: cachev1beta1.ShardHealthReady, MaxLagBytes: 0},
				{Index: 1, Primary: "demo-1", Replicas: []string{"demo-4"}, SlotCount: 5461, Health: cachev1beta1.ShardHealthDegraded, MaxLagBytes: 1024},
			},
			Conditions: []metav1.Condition{
				{Type: cachev1beta1.ConditionAvailable, Status: metav1.ConditionTrue, Reason: "Ready", Message: "ok"},
				{Type: cachev1beta1.ConditionShardsHealthy, Status: metav1.ConditionFalse, Reason: "ShardsNotHealthy", Message: "unhealthy shards: 1:Degraded"},
			},
		},
	}
	out := formatStatus(vc)

	for _, want := range []string{
		"Cluster:   demo-ns/demo",
		"Topology:  Cluster",
		"Shards:    2/3 ready",
		"Bootstrapped: true",
		"demo-0", "demo-4", "Degraded", "1024",
		"ShardsHealthy", "unhealthy shards: 1:Degraded",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("status output missing %q\n%s", want, out)
		}
	}
	// Cluster topology must not print the Replication-only Primary line.
	if strings.Contains(out, "\nPrimary:") {
		t.Errorf("cluster status should not show a Primary line\n%s", out)
	}
}

func TestFormatStatusReplication(t *testing.T) {
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "web-cache", Namespace: "web"},
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology: cachev1beta1.TopologyReplication,
			Profile:  cachev1beta1.ProfileCache,
			Image:    "valkey/valkey:8.0",
			Replicas: 3,
		},
		Status: cachev1beta1.ValkeyClusterStatus{
			Phase:         cachev1beta1.PhaseReady,
			Primary:       "web-cache-0",
			ReadyReplicas: 3,
		},
	}
	out := formatStatus(vc)
	for _, want := range []string{
		"Primary:   web-cache-0",
		"Ready:     3/3",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("status output missing %q\n%s", want, out)
		}
	}
	// Replication topology must not print the Cluster-only Shards line.
	if strings.Contains(out, "Bootstrapped:") {
		t.Errorf("replication status should not show Bootstrapped line\n%s", out)
	}
}

func TestFormatStatusEmptyValuesRenderDash(t *testing.T) {
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "fresh", Namespace: "ns"},
		Spec:       cachev1beta1.ValkeyClusterSpec{Topology: cachev1beta1.TopologyReplication, Replicas: 1},
	}
	out := formatStatus(vc)
	if !strings.Contains(out, "Phase:     -") {
		t.Errorf("empty phase should render as dash\n%s", out)
	}
	if !strings.Contains(out, "Primary:   -") {
		t.Errorf("empty primary should render as dash\n%s", out)
	}
}
