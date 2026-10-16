/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// A scale of the data pods or of the Sentinels must not change either pod
// template, or the StatefulSets roll every pod, the primary included (#77).
func TestSentinelScaleKeepsPodTemplates(t *testing.T) {
	newVC := func(replicas, sentinels int32) *cachev1beta1.ValkeyCluster {
		vc := &cachev1beta1.ValkeyCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "ns"},
			Spec: cachev1beta1.ValkeyClusterSpec{
				Topology: cachev1beta1.TopologySentinel, Replicas: replicas,
				Sentinel: &cachev1beta1.SentinelSpec{Replicas: sentinels, Quorum: 2},
				Storage:  &cachev1beta1.StorageSpec{Mode: storageModeNone},
			},
		}
		return vc
	}
	base := newVC(3, 3)
	if !sentinelPrimaryGuardActive(base) {
		t.Fatal("test wants the primary guard entrypoint (persistence off)")
	}
	for _, vc := range []*cachev1beta1.ValkeyCluster{newVC(5, 3), newVC(3, 5)} {
		if !equality.Semantic.DeepEqual(buildStatefulSet(base, "h", false).Spec.Template, buildStatefulSet(vc, "h", false).Spec.Template) {
			t.Errorf("data pod template changes with replicas=%d sentinels=%d", vc.Spec.Replicas, vc.Spec.Sentinel.Replicas)
		}
		if !equality.Semantic.DeepEqual(buildSentinelStatefulSet(base, false).Spec.Template, buildSentinelStatefulSet(vc, false).Spec.Template) {
			t.Errorf("Sentinel pod template changes with replicas=%d sentinels=%d", vc.Spec.Replicas, vc.Spec.Sentinel.Replicas)
		}
	}
}

// With five Sentinels the data init still asks all of them although the count
// is not rendered: the bound grows to what the Sentinels report.
func TestSentinelDataRoleFindsEverySentinel(t *testing.T) {
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "ns"},
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology: cachev1beta1.TopologySentinel, Replicas: 3,
			Sentinel: &cachev1beta1.SentinelSpec{Replicas: 5, Quorum: 3},
		},
	}
	s1 := "s-1.s-headless.ns.svc.cluster.local"
	conf, _ := runSentinelDataRole(t, vc, "s-0", "10.0.0.10", map[string]string{
		"s-sentinel-0.s-sentinel": "name mymaster ip 10.0.0.99 port 6379 config-epoch 2 num-other-sentinels 4",
		"s-sentinel-4.s-sentinel": "name mymaster ip " + s1 + " port 6379 config-epoch 7 num-other-sentinels 4",
	}, nil, s1)
	if !strings.Contains(conf, "replicaof "+s1+" 6379\n") {
		t.Errorf("should follow s-1, the answer of s-sentinel-4:\n%s", conf)
	}
}
