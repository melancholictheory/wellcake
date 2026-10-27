/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

func TestPickSentinelToReset(t *testing.T) {
	healthy := func(ord int32, others, stale int) sentinelView {
		return sentinelView{ordinal: ord, master: "s-1.s-headless:6379", flags: "master", otherSentinels: others, staleReplicas: stale}
	}
	cases := []struct {
		name  string
		views []sentinelView
		want  int32
	}{
		{"clean", []sentinelView{healthy(0, 2, 0), healthy(1, 2, 0), healthy(2, 2, 0)}, -1},
		{"ghost Sentinel", []sentinelView{healthy(0, 2, 0), healthy(1, 3, 0), healthy(2, 3, 0)}, 1},
		{"stale replica entries", []sentinelView{healthy(0, 2, 0), healthy(1, 2, 0), healthy(2, 2, 2)}, 2},
		{"a Sentinel is not up", []sentinelView{healthy(0, 3, 0), healthy(1, 3, 0)}, -1},
		{"just reset, still finding peers", []sentinelView{healthy(0, 1, 0), healthy(1, 3, 0), healthy(2, 3, 0)}, -1},
		{"master down", []sentinelView{{ordinal: 0, master: "m:6379", flags: "master,s_down", otherSentinels: 3}, healthy(1, 3, 0), healthy(2, 3, 0)}, -1},
		{"failover in progress", []sentinelView{{ordinal: 0, master: "m:6379", flags: "master,failover_in_progress", otherSentinels: 3}, healthy(1, 3, 0), healthy(2, 3, 0)}, -1},
		{"Sentinels disagree on the master", []sentinelView{{ordinal: 0, master: "other:6379", flags: "master", otherSentinels: 3}, healthy(1, 3, 0), healthy(2, 3, 0)}, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pickSentinelToReset(tc.views, 3); got != tc.want {
				t.Errorf("pickSentinelToReset = %d, want %d", got, tc.want)
			}
		})
	}
}

// sentinelView counts only dead replica entries known by IP: live replicas
// announce their DNS name, and a replica that is down under its name is a pod
// restarting, which Sentinel will see again.
func TestSentinelViewCountsStaleReplicas(t *testing.T) {
	m := newRespMock(t)
	m.sentinelMaster = []string{"name", "mymaster", "ip", "s-1.s-headless", "port", "6379", "flags", "master", "num-other-sentinels", "3"}
	m.sentinelReplicas = [][]string{
		{"name", "s-0.s-headless:6379", "ip", "s-0.s-headless", "flags", "slave"},
		{"name", "s-2.s-headless:6379", "ip", "s-2.s-headless", "flags", "s_down,slave"},
		{"name", "10.0.0.7:6379", "ip", "10.0.0.7", "flags", "s_down,slave"},
		{"name", "10.0.0.8:6379", "ip", "10.0.0.8", "flags", "slave"},
		{"name", "s-4.s-headless:6379", "ip", "s-4.s-headless", "flags", "s_down,slave"},
		{"name", "s-3.s-headless:6379", "ip", "s-3.s-headless", "flags", "slave"},
	}
	host, port := m.hostPort(t)
	c := dialReplClient(context.Background(), host, port, "", false, nil, 2*time.Second)
	if c == nil {
		t.Fatal("dial mock")
	}
	defer c.close()
	// spec.replicas is 3: s-4 was scaled down, s-2 is restarting, and s-3 is
	// not down (a scale-up the Sentinels already know about).
	scaledDown := func(host string) bool { return host == "s-3.s-headless" || host == "s-4.s-headless" }
	v, err := c.sentinelView(context.Background(), "mymaster", scaledDown)
	if err != nil {
		t.Fatal(err)
	}
	if v.master != "s-1.s-headless:6379" || v.flags != "master" || v.otherSentinels != 3 || v.staleReplicas != 2 {
		t.Errorf("sentinelView = %+v", v)
	}
}

func TestMisplacedDataPod(t *testing.T) {
	ordinalOf := func(h string) (int32, bool) {
		switch h {
		case "s-1.s-headless.ns.svc.cluster.local", "10.0.0.11":
			return 1, true
		case "s-0.s-headless.ns.svc.cluster.local":
			return 0, true
		}
		return 0, false
	}
	replica := func(ord int32, of string) podRole {
		return podRole{ordinal: ord, reachable: true, role: roleSlave, masterHost: of}
	}
	master := podRole{ordinal: 1, reachable: true, role: roleMaster}
	cases := []struct {
		name   string
		survey []podRole
		want   int
	}{
		{"replicas by name and by IP", []podRole{replica(0, "s-1.s-headless.ns.svc.cluster.local"), master, replica(2, "10.0.0.11")}, -1},
		{"replica chained behind another", []podRole{replica(0, "s-1.s-headless.ns.svc.cluster.local"), master, replica(2, "s-0.s-headless.ns.svc.cluster.local")}, 2},
		{"a second primary", []podRole{{ordinal: 0, reachable: true, role: roleMaster}, master, replica(2, "10.0.0.11")}, 0},
		{"replica of an address no pod has", []podRole{replica(0, "10.0.0.99"), master, replica(2, "10.0.0.11")}, 0},
		{"unreachable pods are left alone", []podRole{{ordinal: 0}, master, replica(2, "10.0.0.11")}, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := misplacedDataPod(tc.survey, 1, ordinalOf); got != tc.want {
				t.Errorf("misplacedDataPod = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestScaledDownDataPod(t *testing.T) {
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "ns"},
		Spec:       cachev1beta1.ValkeyClusterSpec{Topology: cachev1beta1.TopologySentinel, Replicas: 3},
	}
	gone := scaledDownDataPod(vc)
	for host, want := range map[string]bool{
		"s-3.s-headless.ns.svc.cluster.local":          true, // scaled down
		"s-12.s-headless.ns.svc.cluster.local":         true,
		"s-2.s-headless.ns.svc.cluster.local":          false, // within spec.replicas: restarting
		"s-0.s-headless.ns.svc.cluster.local":          false,
		"s-3.s-headless.other.svc.cluster.local":       false, // another namespace
		"x-3.x-headless.ns.svc.cluster.local":          false, // another cluster
		"s-sentinel-3.s-sentinel.ns.svc.cluster.local": false,
		"10.0.0.7": false, // by IP: handled separately
	} {
		if got := gone(host); got != want {
			t.Errorf("scaledDownDataPod(%q) = %v, want %v", host, got, want)
		}
	}
}
