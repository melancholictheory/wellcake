/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// Utilization is a RATE: it needs two samples, must ignore a counter reset, and
// must be clamped to 0-1. The math is delta / (elapsed * 1e6 * threads).
func TestPodThreadUtilization(t *testing.T) {
	vc := minimalCR()
	vc.Namespace, vc.Name = "ns", "u"
	const host = "u-0.h.ns.svc.cluster.local"
	key := vc.Namespace + "/" + vc.Name + "/" + host

	reset := func() {
		threadSamplesMu.Lock()
		delete(threadSamples, key)
		threadSamplesMu.Unlock()
	}
	// seed a previous sample `ago` in the past with the given cumulative active time
	seed := func(ago time.Duration, active float64) {
		threadSamplesMu.Lock()
		threadSamples[key] = threadSample{at: time.Now().Add(-ago), activeUsec: active, threads: 2}
		threadSamplesMu.Unlock()
	}

	// A server without the 9.1 fields reports nothing at all.
	reset()
	if _, ok := podThreadUtilization(vc, host, map[string]string{"role": "master"}); ok {
		t.Fatal("absent active-time fields must not report utilization")
	}

	// The first sample only primes the cache.
	reset()
	first := map[string]string{"used_active_time_main_thread": "1000000"}
	if _, ok := podThreadUtilization(vc, host, first); ok {
		t.Fatal("first sample must not report a rate")
	}

	// Two threads, 50% busy over 10s => delta 10s of thread-time = 1e7 usec.
	reset()
	seed(10*time.Second, 0)
	half := map[string]string{
		"used_active_time_main_thread": "5000000",
		"used_active_time_io_thread_1": "5000000",
	}
	got, ok := podThreadUtilization(vc, host, half)
	if !ok {
		t.Fatal("second sample must report a rate")
	}
	if got < 0.4 || got > 0.6 {
		t.Fatalf("utilization = %v, want ~0.5", got)
	}

	// A counter reset (pod restart) must not produce a bogus negative/huge rate.
	reset()
	seed(10*time.Second, 9_000_000_000)
	if _, ok := podThreadUtilization(vc, host, half); ok {
		t.Fatal("counter reset must not report a rate")
	}

	// Saturated threads clamp to 1, never above.
	reset()
	seed(1*time.Second, 0)
	busy := map[string]string{
		"used_active_time_main_thread": "9000000",
		"used_active_time_io_thread_1": "9000000",
	}
	got, ok = podThreadUtilization(vc, host, busy)
	if !ok || got != 1 {
		t.Fatalf("utilization = %v (ok=%v), want clamped to 1", got, ok)
	}
	reset()
}

// The groups that must not share a zone: one per shard for Cluster (from the
// surveyed shard details), the whole set otherwise.
func TestReplicationGroups(t *testing.T) {
	cl := minimalCR()
	cl.Name = "c"
	cl.Spec.Topology = cachev1beta1.TopologyCluster
	cl.Status.ShardDetails = []cachev1beta1.ShardStatus{
		{Index: 0, Primary: "c-0", Replicas: []string{"c-3"}},
		{Index: 1, Primary: "c-1", Replicas: []string{"c-4"}},
	}
	groups := replicationGroups(cl)
	if len(groups) != 2 {
		t.Fatalf("cluster groups = %v, want one per shard", groups)
	}
	if len(groups[0]) != 2 || groups[0][0] != "c-0" || groups[0][1] != "c-3" {
		t.Fatalf("shard 0 group = %v, want primary+replica", groups[0])
	}

	rep := minimalCR()
	rep.Name = "r"
	rep.Spec.Replicas = 3
	groups = replicationGroups(rep)
	if len(groups) != 1 || len(groups[0]) != 3 || groups[0][2] != "r-2" {
		t.Fatalf("replication groups = %v, want a single 3-pod group", groups)
	}

	// A single pod cannot be spread, so there is nothing to report on.
	solo := minimalCR()
	solo.Spec.Replicas = 1
	if g := replicationGroups(solo); g != nil {
		t.Fatalf("single-pod groups = %v, want nil", g)
	}
}

func TestCheckZoneColocation(t *testing.T) {
	scheme := newTestScheme(t)

	mkPod := func(vc *cachev1beta1.ValkeyCluster, name, node string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: vc.Namespace, Labels: labelsFor(vc)},
			Spec:       corev1.PodSpec{NodeName: node},
		}
	}
	mkNode := func(name, zone string) *corev1.Node {
		return &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{corev1.LabelTopologyZone: zone}},
		}
	}

	cases := []struct {
		name  string
		zones []string // zone per pod ordinal
		want  float64
	}{
		{"spread across zones is healthy", []string{"za", "zb"}, 0},
		{"both pods in one zone is co-located", []string{"za", "za"}, 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// zone lookups are cached process-wide; keep cases independent
			nodeZoneMu.Lock()
			nodeZoneCache = map[string]zoneEntry{}
			nodeZoneMu.Unlock()

			vc := minimalCR()
			vc.Namespace, vc.Name = "ns", "z"+tc.name[:1]
			vc.Spec.Replicas = int32(len(tc.zones))

			objs := []client.Object{}
			for i, z := range tc.zones {
				node := vc.Name + "-node-" + string(rune('a'+i))
				objs = append(objs, mkPod(vc, vc.Name+"-"+string(rune('0'+i)), node), mkNode(node, z))
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
			r := &ValkeyClusterReconciler{Client: c, APIReader: c, Scheme: scheme}

			r.checkZoneColocation(context.Background(), vc)
			got := testutil.ToFloat64(zoneColocatedGauge.WithLabelValues(vc.Namespace, vc.Name))
			if got != tc.want {
				t.Fatalf("colocated groups = %v, want %v", got, tc.want)
			}
		})
	}
}
