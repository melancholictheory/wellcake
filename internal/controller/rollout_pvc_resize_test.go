/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// resizePVC is a data PVC of cluster "vc" requesting `request`, with
// status.capacity `capacity` ("" leaves it unset) and, when requestedAt is not
// zero, the resize-requested-at stamp ensurePVCSize writes.
func resizePVC(name, request, capacity string, requestedAt time.Time) *corev1.PersistentVolumeClaim {
	p := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", Labels: map[string]string{instanceLabel: "vc"}},
		Spec: corev1.PersistentVolumeClaimSpec{Resources: corev1.VolumeResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(request)},
		}},
	}
	if capacity != "" {
		p.Status.Capacity = corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(capacity)}
	}
	if !requestedAt.IsZero() {
		p.Annotations = map[string]string{resizeRequestedAtAnnotation: requestedAt.UTC().Format(time.RFC3339)}
	}
	return p
}

func TestDataPVCResizeState(t *testing.T) {
	now := time.Now()
	recent := now.Add(-10 * time.Second)
	old := now.Add(-pvcResizeSettleTimeout - time.Second)
	infeasible := resizePVC("p", "2Gi", "1Gi", recent)
	infeasible.Status.AllocatedResourceStatuses = map[corev1.ResourceName]corev1.ClaimResourceStatus{
		corev1.ResourceStorage: corev1.PersistentVolumeClaimControllerResizeInfeasible,
	}
	badStamp := resizePVC("p", "2Gi", "1Gi", time.Time{})
	badStamp.Annotations = map[string]string{resizeRequestedAtAnnotation: "yesterday"}
	cases := []struct {
		name string
		pvc  *corev1.PersistentVolumeClaim
		want pvcResizeState
	}{
		{"not stamped by the operator", resizePVC("p", "2Gi", "1Gi", time.Time{}), pvcResizeSettled},
		{"unparsable stamp", badStamp, pvcResizeSettled},
		{"capacity reached", resizePVC("p", "2Gi", "2Gi", recent), pvcResizeSettled},
		{"capacity above the request", resizePVC("p", "2Gi", "3Gi", recent), pvcResizeSettled},
		{"in flight, recent", resizePVC("p", "2Gi", "1Gi", recent), pvcResizeWaiting},
		{"no capacity yet, recent", resizePVC("p", "2Gi", "", recent), pvcResizeWaiting},
		{"in flight past the timeout", resizePVC("p", "2Gi", "1Gi", old), pvcResizeOverdue},
		{"expansion infeasible", infeasible, pvcResizeOverdue},
	}
	for _, tc := range cases {
		if got := dataPVCResizeState(tc.pvc, now); got != tc.want {
			t.Errorf("%s: state = %d, want %d", tc.name, got, tc.want)
		}
	}
}

// TestEnsurePVCSizeStampsRequest: a PVC the operator grows carries the
// resize-requested-at stamp; a PVC already at the size is left unstamped.
func TestEnsurePVCSizeStampsRequest(t *testing.T) {
	scheme := newTestScheme(t)
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "vc", Namespace: "ns"},
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology: cachev1beta1.TopologyReplication, Replicas: 2,
			Storage: &cachev1beta1.StorageSpec{Size: resource.MustParse("2Gi")},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(vc, resizePVC("data-vc-0", "1Gi", "1Gi", time.Time{}),
			resizePVC("data-vc-1", "2Gi", "2Gi", time.Time{})).Build()
	r := &ValkeyClusterReconciler{Client: c, Scheme: scheme}

	before := time.Now().Add(-time.Second)
	if err := r.ensurePVCSize(context.Background(), vc); err != nil {
		t.Fatalf("ensurePVCSize: %v", err)
	}
	var grown, kept corev1.PersistentVolumeClaim
	_ = c.Get(context.Background(), types.NamespacedName{Name: "data-vc-0", Namespace: "ns"}, &grown)
	at, err := time.Parse(time.RFC3339, grown.Annotations[resizeRequestedAtAnnotation])
	if err != nil {
		t.Fatalf("data-vc-0 stamp %q: %v", grown.Annotations[resizeRequestedAtAnnotation], err)
	}
	if at.Before(before.Truncate(time.Second)) {
		t.Errorf("data-vc-0 stamp %s is older than the call", at)
	}
	_ = c.Get(context.Background(), types.NamespacedName{Name: "data-vc-1", Namespace: "ns"}, &kept)
	if _, ok := kept.Annotations[resizeRequestedAtAnnotation]; ok {
		t.Errorf("data-vc-1 was not grown but got stamped")
	}
}

// TestReplicationRolloutWaitsForPVCResize: a stale replica is not deleted while
// a data PVC expansion requested by the operator is in flight; it is once the
// expansion completes, or once it has been in flight past the timeout.
func TestReplicationRolloutWaitsForPVCResize(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name           string
		pvc            *corev1.PersistentVolumeClaim
		wantInProgress bool
		wantDeleted    bool
	}{
		{"expansion in flight", resizePVC("data-vc-2", "2Gi", "1Gi", now.Add(-10*time.Second)), false, false},
		{"expansion done", resizePVC("data-vc-2", "2Gi", "2Gi", now.Add(-10*time.Second)), true, true},
		{"expansion overdue", resizePVC("data-vc-2", "2Gi", "1Gi", now.Add(-pvcResizeSettleTimeout-time.Minute)), true, true},
	}
	for _, tc := range cases {
		vc := &cachev1beta1.ValkeyCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "vc", Namespace: "ns"},
			Spec: cachev1beta1.ValkeyClusterSpec{
				Topology: cachev1beta1.TopologyReplication,
				Replicas: 3,
			},
			Status: cachev1beta1.ValkeyClusterStatus{Primary: "vc-0"},
		}
		data := map[string]string{instanceLabel: "vc"}
		objs := make([]client.Object, 0, 6)
		objs = append(objs, vc, holdTestSTS("vc", "r2"), tc.pvc)
		for _, n := range []string{"vc-0", "vc-1", "vc-2"} {
			objs = append(objs, holdTestPod(n, data, "r1"))
		}
		c := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(objs...).Build()
		r := &ValkeyClusterReconciler{Client: c}

		_, inProgress, err := r.driveReplicationRollout(context.Background(), vc, "", nil)
		if err != nil {
			t.Fatalf("%s: driveReplicationRollout: %v", tc.name, err)
		}
		if inProgress != tc.wantInProgress {
			t.Errorf("%s: inProgress = %v, want %v", tc.name, inProgress, tc.wantInProgress)
		}
		if podExists(t, c, "vc-1") == tc.wantDeleted {
			t.Errorf("%s: vc-1 deleted = %v, want %v", tc.name, !tc.wantDeleted, tc.wantDeleted)
		}
		for _, n := range []string{"vc-0", "vc-2"} {
			if !podExists(t, c, n) {
				t.Errorf("%s: %s was deleted", tc.name, n)
			}
		}
	}
}

func TestReplicationRolloutReadsPVCAfterResize(t *testing.T) {
	for _, tc := range []struct {
		name, replicaRevision        string
		primaryPresent, primaryReady bool
	}{
		{"ready primary", "r1", true, true},
		{"not ready primary", "r1", true, false},
		{"absent primary", "r1", false, false},
		{"absent primary with updated replica", "r2", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			vc := &cachev1beta1.ValkeyCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "vc", Namespace: "ns"},
				Spec: cachev1beta1.ValkeyClusterSpec{
					Topology: cachev1beta1.TopologyReplication, Replicas: 2,
					Storage: &cachev1beta1.StorageSpec{Size: resource.MustParse("2Gi")},
				},
				Status: cachev1beta1.ValkeyClusterStatus{Primary: "vc-0"},
			}
			stale := resizePVC("data-vc-1", "1Gi", "1Gi", time.Time{})
			labels := map[string]string{instanceLabel: "vc"}
			objects := []client.Object{vc, holdTestSTS("vc", "r2"), stale.DeepCopy(), holdTestPod("vc-1", labels, tc.replicaRevision)}
			if tc.primaryPresent {
				primary := holdTestPod("vc-0", labels, "r1")
				if !tc.primaryReady {
					primary.Status.Conditions = nil
				}
				objects = append(objects, primary)
			}
			live := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(objects...).Build()
			cached := interceptor.NewClient(live, interceptor.Funcs{
				List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					if pvcs, ok := list.(*corev1.PersistentVolumeClaimList); ok {
						pvcs.Items = []corev1.PersistentVolumeClaim{*stale.DeepCopy()}
						return nil
					}
					return c.List(ctx, list, opts...)
				},
			})
			r := &ValkeyClusterReconciler{Client: cached, APIReader: live}
			if err := r.ensurePVCSize(ctx, vc); err != nil {
				t.Fatal(err)
			}
			var patched corev1.PersistentVolumeClaim
			if err := live.Get(ctx, client.ObjectKeyFromObject(stale), &patched); err != nil {
				t.Fatal(err)
			}
			if state := dataPVCResizeState(&patched, time.Now()); state != pvcResizeWaiting {
				t.Fatalf("patched PVC resize state = %v, want waiting", state)
			}
			_, inProgress, err := r.driveReplicationRollout(ctx, vc, "", nil)
			if err != nil {
				t.Fatal(err)
			}
			if inProgress {
				t.Error("rollout would suppress reactive failover while PVC expansion is pending")
			}
			if !podExists(t, live, "vc-1") {
				t.Error("rollout deleted a replica before PVC expansion finished")
			}

			// A second reconcile can still observe the old cache entry.
			// Preserve the timestamp even if the wait has already expired.
			expired := time.Now().Add(-3 * time.Minute).UTC().Format(time.RFC3339)
			patched.Annotations[resizeRequestedAtAnnotation] = expired
			if err := live.Update(ctx, &patched); err != nil {
				t.Fatal(err)
			}
			if err := r.ensurePVCSize(ctx, vc); err != nil {
				t.Fatal(err)
			}
			if err := live.Get(ctx, client.ObjectKeyFromObject(stale), &patched); err != nil {
				t.Fatal(err)
			}
			if got := patched.Annotations[resizeRequestedAtAnnotation]; got != expired {
				t.Errorf("resize request timestamp became %q, want %q", got, expired)
			}
		})
	}
}
