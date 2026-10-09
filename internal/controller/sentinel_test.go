/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"context"
	"strings"
	"testing"
	"time"
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
