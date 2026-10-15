/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// A change to sentinel.conf that only applies at start (here: turning auth off)
// must change the Sentinel pod template, so the Sentinels restart with it. The
// timings, which the operator sets live, must not.
func TestSentinelConfigChangeRollsSentinelPods(t *testing.T) {
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "ns"},
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology: cachev1beta1.TopologySentinel, Replicas: 3,
			Sentinel: &cachev1beta1.SentinelSpec{Replicas: 3, Quorum: 2},
			Auth:     &cachev1beta1.AuthSpec{Enabled: ptr.To(true)},
		},
	}
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = cachev1beta1.AddToScheme(scheme)
	r := &ValkeyClusterReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).Build(), Scheme: scheme}
	ctx := context.Background()
	podHash := func(password string) string {
		t.Helper()
		hash, err := r.ensureSentinelConfigMap(ctx, vc, password)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := r.ensureSentinelStatefulSet(ctx, vc, hash); err != nil {
			t.Fatal(err)
		}
		var sts appsv1.StatefulSet
		if err := r.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "s-sentinel"}, &sts); err != nil {
			t.Fatal(err)
		}
		return sts.Spec.Template.Annotations[configHashAnnotation]
	}

	withAuth := podHash("secret")
	if withAuth == "" {
		t.Fatal("Sentinel pod template carries no config hash")
	}
	vc.Spec.Auth.Enabled = ptr.To(false)
	if podHash("") == withAuth {
		t.Error("turning auth off did not change the Sentinel pod template")
	}

	vc.Spec.Auth.Enabled = ptr.To(true)
	vc.Spec.Sentinel.DownAfterMilliseconds = 2000
	vc.Spec.Sentinel.FailoverTimeout = 20000
	if podHash("secret") != withAuth {
		t.Error("a timing change restarted the Sentinels; the operator applies timings live")
	}
}
