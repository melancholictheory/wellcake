/*
Copyright 2026 The Wellcake Authors.
*/

package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

func testClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := cachev1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
}

func TestPatchClusterAnnotationsSetsAnnotations(t *testing.T) {
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "ns"},
		Spec:       cachev1beta1.ValkeyClusterSpec{Topology: cachev1beta1.TopologyReplication},
	}
	c := testClient(t, vc)
	var out bytes.Buffer

	err := patchClusterAnnotations(context.Background(), c, &out, "ns", "web",
		map[string]string{failoverAnnotation: "tok", failoverTargetAnnotation: "web-1"},
		requireNotCluster, "failover requested")
	if err != nil {
		t.Fatalf("patch: %v", err)
	}

	var got cachev1beta1.ValkeyCluster
	if err := c.Get(context.Background(), types.NamespacedName{Name: "web", Namespace: "ns"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Annotations[failoverAnnotation] != "tok" || got.Annotations[failoverTargetAnnotation] != "web-1" {
		t.Errorf("annotations not applied: %v", got.Annotations)
	}
	if !strings.Contains(out.String(), "failover requested for ns/web") {
		t.Errorf("missing confirmation message: %q", out.String())
	}
}

func TestPatchClusterAnnotationsTopologyGuard(t *testing.T) {
	cluster := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "ns"},
		Spec:       cachev1beta1.ValkeyClusterSpec{Topology: cachev1beta1.TopologyCluster},
	}
	c := testClient(t, cluster)
	var out bytes.Buffer

	// failover on a Cluster must be rejected, and nothing should be patched.
	err := patchClusterAnnotations(context.Background(), c, &out, "ns", "demo",
		map[string]string{failoverAnnotation: "tok"}, requireNotCluster, "failover requested")
	if err == nil {
		t.Fatalf("expected failover to be rejected for Cluster topology")
	}
	var got cachev1beta1.ValkeyCluster
	_ = c.Get(context.Background(), types.NamespacedName{Name: "demo", Namespace: "ns"}, &got)
	if _, ok := got.Annotations[failoverAnnotation]; ok {
		t.Errorf("annotation must not be set when the guard rejects")
	}
}

func TestTopologyGuards(t *testing.T) {
	if err := requireCluster(cachev1beta1.TopologyCluster); err != nil {
		t.Errorf("requireCluster(Cluster) should pass: %v", err)
	}
	if err := requireCluster(cachev1beta1.TopologyReplication); err == nil {
		t.Errorf("requireCluster(Replication) should fail")
	}
	if err := requireNotCluster(cachev1beta1.TopologyReplication); err != nil {
		t.Errorf("requireNotCluster(Replication) should pass: %v", err)
	}
	if err := requireNotCluster(cachev1beta1.TopologyCluster); err == nil {
		t.Errorf("requireNotCluster(Cluster) should fail")
	}
}
