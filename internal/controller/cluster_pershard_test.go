/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

func perShardCR() *cachev1beta1.ValkeyCluster {
	vc := minimalCR()
	vc.Name = "kv"
	vc.Spec.Topology = cachev1beta1.TopologyCluster
	vc.Spec.Shards = ptr.To[int32](3)
	vc.Spec.ReplicasPerShard = ptr.To[int32](1)
	vc.Spec.PerShardWorkload = ptr.To(true)
	return vc
}

func TestPerShardEnabled(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*cachev1beta1.ValkeyCluster)
		want bool
	}{
		{"cluster + flag true", func(vc *cachev1beta1.ValkeyCluster) {}, true},
		{"flag nil", func(vc *cachev1beta1.ValkeyCluster) { vc.Spec.PerShardWorkload = nil }, false},
		{"flag false", func(vc *cachev1beta1.ValkeyCluster) { vc.Spec.PerShardWorkload = ptr.To(false) }, false},
		{"non-cluster", func(vc *cachev1beta1.ValkeyCluster) { vc.Spec.Topology = cachev1beta1.TopologyReplication }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vc := perShardCR()
			tc.mut(vc)
			if got := perShardEnabled(vc); got != tc.want {
				t.Errorf("perShardEnabled = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestBuildShardStatefulSet(t *testing.T) {
	vc := perShardCR() // 3 shards, 1 replica/shard
	sts := buildShardStatefulSet(vc, 1, "hash", false)

	if sts.Name != "kv-sh1" {
		t.Errorf("name = %q, want kv-sh1", sts.Name)
	}
	if sts.Spec.ServiceName != "kv-sh1" {
		t.Errorf("serviceName = %q, want kv-sh1", sts.Spec.ServiceName)
	}
	// 1 primary + 1 replica = 2 pods for this shard (NOT the whole-cluster total).
	if sts.Spec.Replicas == nil || *sts.Spec.Replicas != 2 {
		t.Errorf("replicas = %v, want 2", sts.Spec.Replicas)
	}
	if sts.Labels[shardLabel] != "1" || sts.Spec.Selector.MatchLabels[shardLabel] != "1" ||
		sts.Spec.Template.Labels[shardLabel] != "1" {
		t.Errorf("shard label not propagated to sts/selector/template")
	}

	// The whole point: REQUIRED shard-aware anti-affinity by shard label.
	aff := sts.Spec.Template.Spec.Affinity
	if aff == nil || aff.PodAntiAffinity == nil ||
		len(aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 1 {
		t.Fatalf("expected required pod anti-affinity, got %+v", aff)
	}
	term := aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0]
	if term.TopologyKey != topologyKeyHostname {
		t.Errorf("topologyKey = %q, want hostname", term.TopologyKey)
	}
	if term.LabelSelector.MatchLabels[shardLabel] != "1" || term.LabelSelector.MatchLabels[instanceLabel] != "kv" {
		t.Errorf("anti-affinity selector must match this shard: %+v", term.LabelSelector.MatchLabels)
	}

	// Pod template (containers/volumes) reused from the base builder.
	if len(sts.Spec.Template.Spec.Containers) == 0 {
		t.Errorf("shard STS lost the base pod template containers")
	}

	// User-pinned affinity must be respected (not overwritten).
	vc2 := perShardCR()
	vc2.Spec.Affinity = &corev1.Affinity{}
	if sts2 := buildShardStatefulSet(vc2, 0, "h", false); sts2.Spec.Template.Spec.Affinity.PodAntiAffinity != nil {
		t.Errorf("explicit spec.affinity must not be overwritten by shard anti-affinity")
	}
}

func TestClusterDataPods(t *testing.T) {
	// per-shard: <cluster>-sh<shard>-<ord>.<cluster>-sh<shard>...
	vc := perShardCR() // 3 shards, 1 replica/shard → 6 pods
	pods := clusterDataPods(vc)
	if len(pods) != 6 {
		t.Fatalf("per-shard pods = %d, want 6", len(pods))
	}
	if pods[0].host != "kv-sh0-0.kv-sh0.ns.svc.cluster.local" || pods[0].shard != 0 || pods[0].ord != 0 {
		t.Errorf("pod[0] = %+v, want kv-sh0-0 shard0 ord0", pods[0])
	}
	if pods[1].host != "kv-sh0-1.kv-sh0.ns.svc.cluster.local" || pods[1].ord != 1 {
		t.Errorf("pod[1] should be the shard-0 replica, got %+v", pods[1])
	}

	// single-STS: <cluster>-<i>.<cluster>-headless..., shard -1
	vcSingle := perShardCR()
	vcSingle.Spec.PerShardWorkload = nil
	single := clusterDataPods(vcSingle)
	if single[0].shard != -1 || single[0].host != "kv-0.kv-headless.ns.svc.cluster.local" {
		t.Errorf("single-STS pod[0] = %+v, want kv-0 shard -1", single[0])
	}
}

func TestBuildShardCreateCmds(t *testing.T) {
	vc := perShardCR() // 3 shards, 1 replica each
	script := buildShardCreateCmds(vc, clusterDataPods(vc), 6379, "", "")

	// masters-only create from each shard's pod-0.
	if !strings.Contains(script, "--cluster create kv-sh0-0.kv-sh0.ns.svc.cluster.local:6379 kv-sh1-0.kv-sh1.ns.svc.cluster.local:6379 kv-sh2-0.kv-sh2.ns.svc.cluster.local:6379 --cluster-replicas 0 --cluster-yes") {
		t.Errorf("expected masters-only create from pod-0 of each shard:\n%s", script)
	}
	// each shard's replica attached to THAT shard's master.
	if !strings.Contains(script, "add-node kv-sh0-1.kv-sh0.ns.svc.cluster.local:6379 kv-sh0-0.kv-sh0.ns.svc.cluster.local:6379 --cluster-slave") {
		t.Errorf("expected shard-0 replica attached to shard-0 master:\n%s", script)
	}
	if !strings.Contains(script, "add-node kv-sh2-1.kv-sh2.ns.svc.cluster.local:6379 kv-sh2-0.kv-sh2.ns.svc.cluster.local:6379 --cluster-slave") {
		t.Errorf("expected shard-2 replica attached to shard-2 master:\n%s", script)
	}
}

func TestBuildShardHeadlessService(t *testing.T) {
	vc := perShardCR()
	svc := buildShardHeadlessService(vc, 2)
	if svc.Name != "kv-sh2" {
		t.Errorf("name = %q, want kv-sh2", svc.Name)
	}
	if svc.Spec.Selector[shardLabel] != "2" {
		t.Errorf("selector must be scoped to shard 2, got %+v", svc.Spec.Selector)
	}
	if svc.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Errorf("shard service must be headless")
	}
}
