/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"context"
	"testing"
	"time"
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
	}
	host, port := m.hostPort(t)
	c := dialReplClient(context.Background(), host, port, "", false, nil, 2*time.Second)
	if c == nil {
		t.Fatal("dial mock")
	}
	defer c.close()
	v, err := c.sentinelView(context.Background(), "mymaster")
	if err != nil {
		t.Fatal(err)
	}
	if v.master != "s-1.s-headless:6379" || v.flags != "master" || v.otherSentinels != 3 || v.staleReplicas != 1 {
		t.Errorf("sentinelView = %+v", v)
	}
}
