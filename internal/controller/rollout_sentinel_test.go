/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"testing"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

func TestAnyStale(t *testing.T) {
	cases := []struct {
		name string
		pods []rolloutPod
		rev  string
		want bool
	}{
		{"all fresh", []rolloutPod{rp("a-0", 0, "r2", true), rp("a-1", 1, "r2", true)}, "r2", false},
		{"one stale", []rolloutPod{rp("a-0", 0, "r1", true), rp("a-1", 1, "r2", true)}, "r2", true},
		{"empty", nil, "r2", false},
	}
	for _, tc := range cases {
		if got := anyStale(tc.pods, tc.rev); got != tc.want {
			t.Errorf("%s: anyStale = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestSentinelDataMaster(t *testing.T) {
	vc := &cachev1beta1.ValkeyCluster{}
	vc.Name = "vc"

	survey := []podRole{
		{ordinal: 0, role: roleSlave, reachable: true},
		{ordinal: 1, role: roleMaster, reachable: true},
		{ordinal: 2, role: roleSlave, reachable: true},
	}
	if got := sentinelDataMaster(vc, survey); got != "vc-1" {
		t.Errorf("sentinelDataMaster = %q, want vc-1", got)
	}

	// An unreachable master is not authoritative — report none until it answers.
	unreachableMaster := []podRole{
		{ordinal: 0, role: roleSlave, reachable: true},
		{ordinal: 1, role: roleMaster, reachable: false},
	}
	if got := sentinelDataMaster(vc, unreachableMaster); got != "" {
		t.Errorf("sentinelDataMaster (unreachable master) = %q, want empty", got)
	}

	// No master visible at all.
	if got := sentinelDataMaster(vc, []podRole{{ordinal: 0, role: roleSlave, reachable: true}}); got != "" {
		t.Errorf("sentinelDataMaster (no master) = %q, want empty", got)
	}
}

// TestSentinelNames pins the Sentinel StatefulSet / Service name and pod FQDN
// the rollout dials, so a rename can't silently break SENTINEL FAILOVER.
func TestSentinelNames(t *testing.T) {
	vc := &cachev1beta1.ValkeyCluster{}
	vc.Name = "vc"
	vc.Namespace = "ns"

	if got := sentinelStatefulSetName(vc); got != "vc-sentinel" {
		t.Errorf("sentinelStatefulSetName = %q, want vc-sentinel", got)
	}
	if got := sentinelPodFQDN(vc, 2); got != "vc-sentinel-2.vc-sentinel.ns.svc.cluster.local" {
		t.Errorf("sentinelPodFQDN = %q", got)
	}
}

// TestSentinelDataRolloutSequence walks the data-plane decision the way
// driveSentinelRollout drives it: replicas roll first (lowest ordinal), and the
// master is only ever surfaced via promote (→ SENTINEL FAILOVER) once it is the
// sole stale pod. Mirrors the Replication contract since the same
// nextRolloutStep powers both — only the promote ACTION differs.
func TestSentinelDataRolloutSequence(t *testing.T) {
	const old, new = "r1", "r2"
	master := "vc-0"

	// All stale, all ready → roll the lowest-ordinal stale REPLICA first, never
	// the master.
	pods := []rolloutPod{
		rp("vc-0", 0, old, true), // master
		rp("vc-1", 1, old, true),
		rp("vc-2", 2, old, true),
	}
	if a := nextRolloutStep(pods, new, master); a.kind != rolloutRollReplica || a.pod != "vc-1" {
		t.Fatalf("step1 = %+v, want rollReplica vc-1", a)
	}

	// Replicas fresh, only the master stale → promote (the driver issues SENTINEL
	// FAILOVER here rather than promoting directly).
	pods = []rolloutPod{
		rp("vc-0", 0, old, true), // master, still stale
		rp("vc-1", 1, new, true),
		rp("vc-2", 2, new, true),
	}
	if a := nextRolloutStep(pods, new, master); a.kind != rolloutPromote || a.pod != master {
		t.Fatalf("step2 = %+v, want promote %s", a, master)
	}

	// A not-yet-ready pod parks the rollout (one disruption at a time).
	pods = []rolloutPod{
		rp("vc-0", 0, old, true),
		rp("vc-1", 1, new, false), // just restarted, not Ready
		rp("vc-2", 2, old, true),
	}
	if a := nextRolloutStep(pods, new, master); a.kind != rolloutWait {
		t.Fatalf("step3 = %+v, want wait", a)
	}
}

// TestSentinelPlaneRolloutNoPrimary confirms the Sentinel plane (primary="")
// only ever rolls replicas — it must never emit a promote, since Sentinels have
// no master to hand over.
func TestSentinelPlaneRolloutNoPrimary(t *testing.T) {
	const old, new = "r1", "r2"
	pods := []rolloutPod{
		rp("vc-sentinel-0", 0, old, true),
		rp("vc-sentinel-1", 1, old, true),
		rp("vc-sentinel-2", 2, old, true),
	}
	a := nextRolloutStep(pods, new, "")
	if a.kind != rolloutRollReplica || a.pod != "vc-sentinel-0" {
		t.Fatalf("sentinel plane step = %+v, want rollReplica vc-sentinel-0", a)
	}
	// All fresh → done.
	for i := range pods {
		pods[i].revision = new
	}
	if a := nextRolloutStep(pods, new, ""); a.kind != rolloutNone {
		t.Fatalf("sentinel plane done = %+v, want none", a)
	}
}
