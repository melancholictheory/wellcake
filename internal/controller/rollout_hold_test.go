/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"context"
	"maps"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

func TestRolloutHeld(t *testing.T) {
	cases := []struct {
		val  string
		want bool
	}{
		{"true", true},
		{"1", true},
		{"false", false},
		{"", false},
		{"yes", false}, // ParseBool rejects → not held
	}
	for _, tc := range cases {
		vc := &cachev1beta1.ValkeyCluster{}
		if tc.val != "" {
			vc.Annotations = map[string]string{rolloutHoldAnnotation: tc.val}
		}
		if got := rolloutHeld(vc); got != tc.want {
			t.Errorf("rolloutHeld(%q) = %v, want %v", tc.val, got, tc.want)
		}
	}
}

// holdTestPod is a Ready pod on the given StatefulSet revision.
func holdTestPod(name string, labels map[string]string, revision string) *corev1.Pod {
	l := map[string]string{appsv1.ControllerRevisionHashLabelKey: revision}
	maps.Copy(l, labels)
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", Labels: l},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
		}},
	}
}

func holdTestSTS(name, updateRevision string) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
		Status:     appsv1.StatefulSetStatus{UpdateRevision: updateRevision},
	}
}

func podExists(t *testing.T, c client.Client, name string) bool {
	t.Helper()
	err := c.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: name}, &corev1.Pod{})
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("get pod %s: %v", name, err)
	}
	return err == nil
}

// TestSentinelRolloutHold: with the data plane converged and the Sentinel pods
// stale, the rollout deletes the lowest stale Sentinel, unless the cluster
// carries rollout-hold: then it deletes nothing and reports no rollout.
func TestSentinelRolloutHold(t *testing.T) {
	for _, held := range []bool{false, true} {
		vc := &cachev1beta1.ValkeyCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "vc", Namespace: "ns"},
			Spec: cachev1beta1.ValkeyClusterSpec{
				Topology: cachev1beta1.TopologySentinel,
				Replicas: 3,
				Sentinel: &cachev1beta1.SentinelSpec{Replicas: 3},
			},
		}
		if held {
			vc.Annotations = map[string]string{rolloutHoldAnnotation: "true"}
		}
		data := map[string]string{instanceLabel: "vc"}
		sen := map[string]string{instanceLabel: "vc", componentLabel: componentSentinel}
		objs := make([]client.Object, 0, 9)
		objs = append(objs, vc, holdTestSTS("vc", "d2"), holdTestSTS("vc-sentinel", "s2"))
		for _, n := range []string{"vc-0", "vc-1", "vc-2"} {
			objs = append(objs, holdTestPod(n, data, "d2"))
		}
		for _, n := range []string{"vc-sentinel-0", "vc-sentinel-1", "vc-sentinel-2"} {
			objs = append(objs, holdTestPod(n, sen, "s1"))
		}
		c := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(objs...).Build()
		r := &ValkeyClusterReconciler{Client: c}

		inProgress, err := r.driveSentinelRollout(context.Background(), vc, "")
		if err != nil {
			t.Fatalf("held=%v: driveSentinelRollout: %v", held, err)
		}
		if inProgress == held {
			t.Errorf("held=%v: inProgress = %v, want %v", held, inProgress, !held)
		}
		if podExists(t, c, "vc-sentinel-0") != held {
			t.Errorf("held=%v: vc-sentinel-0 exists = %v, want %v", held, !held, held)
		}
		for _, n := range []string{"vc-sentinel-1", "vc-sentinel-2", "vc-0", "vc-1", "vc-2"} {
			if !podExists(t, c, n) {
				t.Errorf("held=%v: %s was deleted", held, n)
			}
		}
	}
}

// TestReplicationRolloutHold: a stale Replication cluster rolls its lowest
// stale replica, unless held: then no pod is deleted and the primary is kept.
func TestReplicationRolloutHold(t *testing.T) {
	for _, held := range []bool{false, true} {
		vc := &cachev1beta1.ValkeyCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "vc", Namespace: "ns"},
			Spec: cachev1beta1.ValkeyClusterSpec{
				Topology: cachev1beta1.TopologyReplication,
				Replicas: 3,
			},
			Status: cachev1beta1.ValkeyClusterStatus{Primary: "vc-0"},
		}
		if held {
			vc.Annotations = map[string]string{rolloutHoldAnnotation: "true"}
		}
		data := map[string]string{instanceLabel: "vc"}
		objs := make([]client.Object, 0, 5)
		objs = append(objs, vc, holdTestSTS("vc", "r2"))
		for _, n := range []string{"vc-0", "vc-1", "vc-2"} {
			objs = append(objs, holdTestPod(n, data, "r1"))
		}
		c := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(objs...).Build()
		r := &ValkeyClusterReconciler{Client: c}

		primary, inProgress, err := r.driveReplicationRollout(context.Background(), vc, "", nil)
		if err != nil {
			t.Fatalf("held=%v: driveReplicationRollout: %v", held, err)
		}
		if primary != "vc-0" {
			t.Errorf("held=%v: primary = %q, want vc-0", held, primary)
		}
		if inProgress == held {
			t.Errorf("held=%v: inProgress = %v, want %v", held, inProgress, !held)
		}
		if podExists(t, c, "vc-1") != held {
			t.Errorf("held=%v: vc-1 exists = %v, want %v", held, !held, held)
		}
		for _, n := range []string{"vc-0", "vc-2"} {
			if !podExists(t, c, n) {
				t.Errorf("held=%v: %s was deleted", held, n)
			}
		}
	}
}

// notReady marks a test pod NotReady.
func notReady(p *corev1.Pod) *corev1.Pod {
	p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
	return p
}

// TestReplicationRolloutHoldPrimaryDown: with two Ready replicas on the old
// revision and the primary pod NotReady or absent, an unheld rollout waits
// (in progress, so the caller skips reactive failover), while a held cluster
// reports no rollout, so the caller runs reactive failover. Nothing is deleted.
func TestReplicationRolloutHoldPrimaryDown(t *testing.T) {
	data := map[string]string{instanceLabel: "vc"}
	cases := []struct {
		name    string
		primary *corev1.Pod // nil: the pod is absent
	}{
		{"primary NotReady", notReady(holdTestPod("vc-0", data, "r1"))},
		{"primary absent", nil},
	}
	for _, tc := range cases {
		for _, held := range []bool{false, true} {
			vc := &cachev1beta1.ValkeyCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "vc", Namespace: "ns"},
				Spec: cachev1beta1.ValkeyClusterSpec{
					Topology: cachev1beta1.TopologyReplication,
					Replicas: 3,
				},
				Status: cachev1beta1.ValkeyClusterStatus{Primary: "vc-0"},
			}
			if held {
				vc.Annotations = map[string]string{rolloutHoldAnnotation: "true"}
			}
			objs := make([]client.Object, 0, 5)
			objs = append(objs, vc, holdTestSTS("vc", "r2"),
				holdTestPod("vc-1", data, "r1"), holdTestPod("vc-2", data, "r1"))
			if tc.primary != nil {
				objs = append(objs, tc.primary.DeepCopy())
			}
			c := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(objs...).Build()
			r := &ValkeyClusterReconciler{Client: c}

			primary, inProgress, err := r.driveReplicationRollout(context.Background(), vc, "", nil)
			if err != nil {
				t.Fatalf("%s, held=%v: driveReplicationRollout: %v", tc.name, held, err)
			}
			if primary != "vc-0" {
				t.Errorf("%s, held=%v: primary = %q, want vc-0", tc.name, held, primary)
			}
			if inProgress == held {
				t.Errorf("%s, held=%v: inProgress = %v, want %v", tc.name, held, inProgress, !held)
			}
			if podExists(t, c, "vc-0") != (tc.primary != nil) {
				t.Errorf("%s, held=%v: vc-0 was deleted", tc.name, held)
			}
			for _, n := range []string{"vc-1", "vc-2"} {
				if !podExists(t, c, n) {
					t.Errorf("%s, held=%v: %s was deleted", tc.name, held, n)
				}
			}
		}
	}
}

// TestSentinelRolloutHoldNotSteady: a Sentinel cluster whose pods are not all
// there or not all Ready is in progress for an unheld rollout, but not for a
// held one. Nothing is deleted.
func TestSentinelRolloutHoldNotSteady(t *testing.T) {
	data := map[string]string{instanceLabel: "vc"}
	sen := map[string]string{instanceLabel: "vc", componentLabel: componentSentinel}
	cases := []struct {
		name string
		pods []*corev1.Pod
	}{
		{"data pod absent", []*corev1.Pod{
			holdTestPod("vc-1", data, "d1"), holdTestPod("vc-2", data, "d1"),
			holdTestPod("vc-sentinel-0", sen, "s1"), holdTestPod("vc-sentinel-1", sen, "s1"),
			holdTestPod("vc-sentinel-2", sen, "s1"),
		}},
		{"Sentinel pod NotReady", []*corev1.Pod{
			holdTestPod("vc-0", data, "d2"), holdTestPod("vc-1", data, "d2"), holdTestPod("vc-2", data, "d2"),
			notReady(holdTestPod("vc-sentinel-0", sen, "s1")), holdTestPod("vc-sentinel-1", sen, "s1"),
			holdTestPod("vc-sentinel-2", sen, "s1"),
		}},
	}
	for _, tc := range cases {
		for _, held := range []bool{false, true} {
			vc := &cachev1beta1.ValkeyCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "vc", Namespace: "ns"},
				Spec: cachev1beta1.ValkeyClusterSpec{
					Topology: cachev1beta1.TopologySentinel,
					Replicas: 3,
					Sentinel: &cachev1beta1.SentinelSpec{Replicas: 3},
				},
			}
			if held {
				vc.Annotations = map[string]string{rolloutHoldAnnotation: "true"}
			}
			objs := make([]client.Object, 0, 3+len(tc.pods))
			objs = append(objs, vc, holdTestSTS("vc", "d2"), holdTestSTS("vc-sentinel", "s2"))
			for _, p := range tc.pods {
				objs = append(objs, p.DeepCopy())
			}
			c := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(objs...).Build()
			r := &ValkeyClusterReconciler{Client: c}

			inProgress, err := r.driveSentinelRollout(context.Background(), vc, "")
			if err != nil {
				t.Fatalf("%s, held=%v: driveSentinelRollout: %v", tc.name, held, err)
			}
			if inProgress == held {
				t.Errorf("%s, held=%v: inProgress = %v, want %v", tc.name, held, inProgress, !held)
			}
			for _, p := range tc.pods {
				if !podExists(t, c, p.Name) {
					t.Errorf("%s, held=%v: %s was deleted", tc.name, held, p.Name)
				}
			}
		}
	}
}
