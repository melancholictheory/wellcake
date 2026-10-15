/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// Unset timings keep the previously hard-coded values, so the Sentinel
// ConfigMap of existing clusters is unchanged by an operator upgrade.
func TestRenderSentinelConfTimings(t *testing.T) {
	def := renderSentinelConf(sentinelCR(), "")
	for _, want := range []string{
		"sentinel down-after-milliseconds mymaster 5000\n",
		"sentinel failover-timeout mymaster 60000\n",
	} {
		if !strings.Contains(def, want) {
			t.Errorf("default sentinel conf missing %q\n%s", want, def)
		}
	}

	vc := sentinelCR()
	vc.Spec.Sentinel.DownAfterMilliseconds = 2000
	vc.Spec.Sentinel.FailoverTimeout = 20000
	custom := renderSentinelConf(vc, "")
	for _, want := range []string{
		"sentinel down-after-milliseconds mymaster 2000\n",
		"sentinel failover-timeout mymaster 20000\n",
	} {
		if !strings.Contains(custom, want) {
			t.Errorf("custom sentinel conf missing %q\n%s", want, custom)
		}
	}
}

func TestParseSentinelMaster(t *testing.T) {
	resp2 := []any{"name", "mymaster", "down-after-milliseconds", "5000", "failover-timeout", "60000"}
	resp3 := map[any]any{"name": "mymaster", "down-after-milliseconds": "5000", "failover-timeout": "60000"}
	for name, res := range map[string]any{"resp2": resp2, "resp3": resp3} {
		got, err := parseSentinelMaster(res)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got["down-after-milliseconds"] != "5000" || got["failover-timeout"] != "60000" {
			t.Errorf("%s: parsed %v", name, got)
		}
	}
	if _, err := parseSentinelMaster("unexpected"); err == nil {
		t.Error("expected an error for an unexpected reply type")
	}
}

// syncSentinelTimings must only write what differs, in one SENTINEL SET, and
// write nothing when the Sentinel already runs the configured values.
func TestSyncSentinelTimings(t *testing.T) {
	want := sentinelTimings(sentinelCR())
	want[0][1], want[1][1] = "2000", "20000"

	cases := []struct {
		name    string
		current []string
		wantSet []string // nil → no SENTINEL SET expected
	}{
		{
			name:    "both differ",
			current: []string{"down-after-milliseconds", "5000", "failover-timeout", "60000"},
			wantSet: []string{"SENTINEL", "SET", "mymaster", "down-after-milliseconds", "2000", "failover-timeout", "20000"},
		},
		{
			name:    "one differs",
			current: []string{"down-after-milliseconds", "2000", "failover-timeout", "60000"},
			wantSet: []string{"SENTINEL", "SET", "mymaster", "failover-timeout", "20000"},
		},
		{
			name:    "in sync",
			current: []string{"down-after-milliseconds", "2000", "failover-timeout", "20000"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newRespMock(t)
			m.sentinelMaster = append([]string{"name", "mymaster"}, tc.current...)
			host, port := m.hostPort(t)
			c := dialReplClient(context.Background(), host, port, "", false, nil, 2*time.Second)
			if c == nil {
				t.Fatal("dial nil")
			}
			defer c.close()

			changed, err := syncSentinelTimings(context.Background(), c, want)
			if err != nil {
				t.Fatalf("syncSentinelTimings: %v", err)
			}
			got := m.recorded("SENTINEL SET")
			if changed != (tc.wantSet != nil) {
				t.Errorf("changed = %v, want %v", changed, tc.wantSet != nil)
			}
			if strings.Join(got, " ") != strings.Join(tc.wantSet, " ") {
				t.Errorf("SENTINEL SET = %v, want %v", got, tc.wantSet)
			}
		})
	}
}

// Only Ready Sentinels are dialed: one that is not Ready yet (starting or
// crashlooping) reads the timings from the ConfigMap when it starts, so it
// must not keep the cluster on a requeue. A Ready one that cannot be reached
// still does.
func TestApplySentinelTimingsSkipsPodsNotReady(t *testing.T) {
	vc := sentinelCR()
	pod := func(ord string, ready bool) *corev1.Pod {
		status := corev1.ConditionFalse
		if ready {
			status = corev1.ConditionTrue
		}
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: vc.Name + "-sentinel-" + ord, Namespace: vc.Namespace, Labels: sentinelPodLabels(vc),
			},
			Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: status}}},
		}
	}
	pending := func(objs ...client.Object) bool {
		t.Helper()
		scheme := runtime.NewScheme()
		_ = clientgoscheme.AddToScheme(scheme)
		_ = cachev1beta1.AddToScheme(scheme)
		r := &ValkeyClusterReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build(), Scheme: scheme}
		return r.applySentinelTimings(context.Background(), vc, "")
	}

	if pending() {
		t.Error("no Sentinel pod yet: nothing to retry")
	}
	if pending(pod("0", false), pod("1", false), pod("2", false)) {
		t.Error("Sentinels that are not Ready must not keep the cluster pending")
	}
	if !pending(pod("0", true), pod("1", false)) {
		t.Error("a Ready Sentinel that cannot be reached must keep the cluster pending")
	}
}
