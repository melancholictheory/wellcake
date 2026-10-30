/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

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

func TestReplicasCaughtUp(t *testing.T) {
	info := func(master string, slaves ...string) map[string]string {
		m := map[string]string{"role": "master", "master_repl_offset": master, "connected_slaves": strconv.Itoa(len(slaves))}
		for i, s := range slaves {
			m["slave"+strconv.Itoa(i)] = s
		}
		return m
	}
	cases := []struct {
		name string
		info map[string]string
		want bool
	}{
		{"all at the offset", info("100",
			"ip=vc-1.vc-headless,port=6379,state=online,offset=100,lag=0",
			"ip=vc-2.vc-headless,port=6379,state=online,offset=100,lag=0"), true},
		{"one behind", info("100",
			"ip=vc-1.vc-headless,port=6379,state=online,offset=100,lag=0",
			"ip=vc-2.vc-headless,port=6379,state=online,offset=98,lag=0"), false},
		{"one still syncing", info("100",
			"ip=vc-1.vc-headless,port=6379,state=wait_bgsave,offset=0,lag=0"), false},
		{"no replica", info("100"), false},
		{"no offset", map[string]string{"role": "master"}, false},
	}
	for _, tc := range cases {
		if got := replicasCaughtUp(tc.info); got != tc.want {
			t.Errorf("%s: replicasCaughtUp = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// handoverMocks starts a master and two replicas, and returns a dial function
// that reaches them by the names "master", "r1" and "r2". The tests promote r2.
func handoverMocks(t *testing.T, masterInfo string) (master, r2 *respMock, dial func(string) *replClient) {
	t.Helper()
	master, r1, r2 := newRespMock(t), newRespMock(t), newRespMock(t)
	master.setInfo(masterInfo)
	r1.setInfo("role:slave\r\n")
	r2.setInfo("role:slave\r\n")
	byName := map[string]*respMock{"master": master, "r1": r1, "r2": r2}
	dial = func(name string) *replClient {
		host, port := byName[name].hostPort(t)
		return dialReplClient(context.Background(), host, port, "", false, nil, time.Second)
	}
	return master, r2, dial
}

// handoverSteps keeps the handover commands of the recorded ones, in order.
func handoverSteps(m *respMock) []string {
	var out []string
	for _, h := range m.commandHeads() {
		if h == "CLIENT PAUSE" || h == "CLIENT UNPAUSE" || strings.HasPrefix(h, "REPLICAOF") {
			out = append(out, h)
		}
	}
	return out
}

var testHandoverTimings = handoverTimings{
	pause: 2 * time.Second, catchUp: 300 * time.Millisecond, promote: 300 * time.Millisecond, poll: 10 * time.Millisecond,
}

const caughtUpMasterInfo = "role:master\r\nmaster_repl_offset:100\r\nconnected_slaves:2\r\n" +
	"slave0:ip=r1,port=6379,state=online,offset=100,lag=0\r\n" +
	"slave1:ip=r2,port=6379,state=online,offset=100,lag=0\r\n"

// The master's writes are paused before SENTINEL FAILOVER, and the old master is
// pointed at the promoted replica before the pause is lifted.
func TestHandOverMaster(t *testing.T) {
	master, r2, dial := handoverMocks(t, caughtUpMasterInfo)
	failedOver := false
	failover := func() error {
		if got := handoverSteps(master); len(got) != 1 || got[0] != "CLIENT PAUSE" {
			t.Errorf("SENTINEL FAILOVER issued after %v, want after CLIENT PAUSE only", got)
		}
		failedOver = true
		r2.setInfo("role:master\r\n")
		return nil
	}
	if err := handOverMaster(context.Background(), dial, failover, "master", []string{"r1", "r2"}, 6379, testHandoverTimings); err != nil {
		t.Fatalf("handOverMaster: %v", err)
	}
	if !failedOver {
		t.Fatal("SENTINEL FAILOVER was not issued")
	}
	want := []string{"CLIENT PAUSE", "REPLICAOF R2", "CLIENT UNPAUSE"}
	if got := handoverSteps(master); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("master commands = %v, want %v", got, want)
	}
	if c := master.recorded("CLIENT PAUSE"); len(c) != 4 || c[2] != "2000" || !strings.EqualFold(c[3], "WRITE") {
		t.Errorf("CLIENT PAUSE = %v, want CLIENT PAUSE 2000 WRITE", c)
	}
	if c := master.recorded("REPLICAOF"); len(c) != 3 || c[2] != "6379" {
		t.Errorf("REPLICAOF = %v, want REPLICAOF r2 6379", c)
	}
}

// A replica behind the master stops the handover before SENTINEL FAILOVER, and
// the pause is lifted.
func TestHandOverMasterReplicaBehind(t *testing.T) {
	master, _, dial := handoverMocks(t, strings.Replace(caughtUpMasterInfo, "offset=100,lag=0\r\nslave1", "offset=90,lag=0\r\nslave1", 1))
	failover := func() error {
		t.Error("SENTINEL FAILOVER issued while a replica was behind")
		return nil
	}
	if err := handOverMaster(context.Background(), dial, failover, "master", []string{"r1", "r2"}, 6379, testHandoverTimings); err == nil {
		t.Fatal("handOverMaster succeeded with a replica behind")
	}
	want := []string{"CLIENT PAUSE", "CLIENT UNPAUSE"}
	if got := handoverSteps(master); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("master commands = %v, want %v", got, want)
	}
}

// When no replica reports itself promoted, the old master is left as it is for
// the Sentinels, and the pause is lifted.
func TestHandOverMasterNoPromotion(t *testing.T) {
	master, _, dial := handoverMocks(t, caughtUpMasterInfo)
	if err := handOverMaster(context.Background(), dial, func() error { return nil }, "master", []string{"r1", "r2"}, 6379, testHandoverTimings); err == nil {
		t.Fatal("handOverMaster succeeded without a promoted replica")
	}
	want := []string{"CLIENT PAUSE", "CLIENT UNPAUSE"}
	if got := handoverSteps(master); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("master commands = %v, want %v", got, want)
	}
}

// A failover already in progress (INPROG), for example one the Sentinels started
// while the pause held their PUBLISH, is followed like ours.
func TestHandOverMasterFailoverInProgress(t *testing.T) {
	master, r2, dial := handoverMocks(t, caughtUpMasterInfo)
	failover := func() error {
		r2.setInfo("role:master\r\n")
		return errors.New("INPROG Failover already in progress")
	}
	if err := handOverMaster(context.Background(), dial, failover, "master", []string{"r1", "r2"}, 6379, testHandoverTimings); err != nil {
		t.Fatalf("handOverMaster: %v", err)
	}
	want := []string{"CLIENT PAUSE", "REPLICAOF R2", "CLIENT UNPAUSE"}
	if got := handoverSteps(master); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("master commands = %v, want %v", got, want)
	}
}
