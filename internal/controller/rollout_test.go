/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

func rp(name string, ord int32, revision string, ready bool) rolloutPod {
	return rolloutPod{name: name, ordinal: ord, revision: revision, ready: ready}
}

// TestNextRolloutStep covers the ADR 0004 rollout decision: replicas roll first
// (lowest ordinal), the primary is touched only via promote and only once it is
// the sole stale pod, and an unready pod parks the rollout (one at a time).
func TestNextRolloutStep(t *testing.T) {
	const want = "rev2" // desired revision
	cases := []struct {
		name    string
		pods    []rolloutPod
		primary string
		kind    rolloutKind
		pod     string
	}{
		{
			name:    "all on desired revision → done",
			pods:    []rolloutPod{rp("c-0", 0, want, true), rp("c-1", 1, want, true), rp("c-2", 2, want, true)},
			primary: "c-0",
			kind:    rolloutNone,
		},
		{
			name:    "stale replica rolls first (not the primary)",
			pods:    []rolloutPod{rp("c-0", 0, "h1", true), rp("c-1", 1, "h1", true), rp("c-2", 2, "h1", true)},
			primary: "c-0",
			kind:    rolloutRollReplica,
			pod:     "c-1", // lowest-ordinal non-primary stale pod
		},
		{
			name:    "primary lowest ordinal but rolled last — replica c-2 picked over primary c-0",
			pods:    []rolloutPod{rp("c-0", 0, "h1", true), rp("c-1", 1, want, true), rp("c-2", 2, "h1", true)},
			primary: "c-0",
			kind:    rolloutRollReplica,
			pod:     "c-2",
		},
		{
			name:    "only the primary is stale → promote",
			pods:    []rolloutPod{rp("c-0", 0, "h1", true), rp("c-1", 1, want, true), rp("c-2", 2, want, true)},
			primary: "c-0",
			kind:    rolloutPromote,
			pod:     "c-0",
		},
		{
			name:    "a pod not Ready parks the rollout (one at a time)",
			pods:    []rolloutPod{rp("c-0", 0, "h1", true), rp("c-1", 1, want, false), rp("c-2", 2, "h1", true)},
			primary: "c-0",
			kind:    rolloutWait,
		},
		{
			name:    "primary is not the lowest ordinal — replicas still roll first",
			pods:    []rolloutPod{rp("c-0", 0, "h1", true), rp("c-1", 1, "h1", true), rp("c-2", 2, "h1", true)},
			primary: "c-2",
			kind:    rolloutRollReplica,
			pod:     "c-0",
		},
		{
			name:    "empty desired revision is a no-op (nothing to roll toward)",
			pods:    []rolloutPod{rp("c-0", 0, "h1", true)},
			primary: "c-0",
			kind:    rolloutNone,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rev := want
			if tc.name == "empty desired revision is a no-op (nothing to roll toward)" {
				rev = ""
			}
			got := nextRolloutStep(tc.pods, rev, tc.primary)
			if got.kind != tc.kind {
				t.Fatalf("kind = %v, want %v", got.kind, tc.kind)
			}
			if tc.pod != "" && got.pod != tc.pod {
				t.Fatalf("pod = %q, want %q", got.pod, tc.pod)
			}
		})
	}
}

func TestOrdinalFromPodName(t *testing.T) {
	cases := []struct {
		name, sts string
		want      int32
		ok        bool
	}{
		{"my-vc-0", "my-vc", 0, true},
		{"my-vc-12", "my-vc", 12, true},
		{"my-vc", "my-vc", 0, false},      // no ordinal suffix
		{"other-3", "my-vc", 0, false},    // different STS
		{"my-vc-abc", "my-vc", 0, false},  // non-numeric suffix
		{"my-vc-1-0", "my-vc-1", 0, true}, // STS name itself ends in a number
	}
	for _, tc := range cases {
		got, ok := ordinalFromPodName(tc.name, tc.sts)
		if ok != tc.ok || (ok && got != tc.want) {
			t.Errorf("ordinalFromPodName(%q,%q) = (%d,%v), want (%d,%v)", tc.name, tc.sts, got, ok, tc.want, tc.ok)
		}
	}
}

func TestProactiveRolloutEnabled(t *testing.T) {
	cases := []struct {
		val  string
		want bool
	}{
		{"true", true},
		{"1", true},
		{"false", false},
		{"", false},
		{"yes", false}, // ParseBool rejects → off
	}
	for _, tc := range cases {
		vc := &cachev1beta1.ValkeyCluster{}
		if tc.val != "" {
			vc.Annotations = map[string]string{proactiveRolloutAnnotation: tc.val}
		}
		if got := proactiveRolloutEnabled(vc); got != tc.want {
			t.Errorf("proactiveRolloutEnabled(%q) = %v, want %v", tc.val, got, tc.want)
		}
	}
}

// TestNextClusterRolloutStep covers the ADR 0004 Cluster decision: a shard's
// replicas roll first, then its stale master is handed over via CLUSTER FAILOVER
// to a fresh replica; an unready pod parks the rollout; a 0-replica shard's
// master is restarted directly.
func TestNextClusterRolloutStep(t *testing.T) {
	const want = "rev2"
	// 2 shards: shard A primary a-0 replica a-1; shard B primary b-0 replica b-1.
	shards := []clusterShard{
		{primary: "a-0", replicas: []string{"a-1"}},
		{primary: "b-0", replicas: []string{"b-1"}},
	}
	cases := []struct {
		name string
		pods []rolloutPod
		kind clusterRolloutKind
		pod  string
	}{
		{
			name: "all fresh → none",
			pods: []rolloutPod{rp("a-0", 0, want, true), rp("a-1", 1, want, true), rp("b-0", 2, want, true), rp("b-1", 3, want, true)},
			kind: clusterRolloutNone,
		},
		{
			name: "stale replica rolls before any master",
			pods: []rolloutPod{rp("a-0", 0, "r1", true), rp("a-1", 1, "r1", true), rp("b-0", 2, "r1", true), rp("b-1", 3, "r1", true)},
			kind: clusterRolloutRollReplica,
			pod:  "a-1", // lowest-named stale replica (a-1 < b-1)
		},
		{
			name: "replicas fresh, stale master → failover on fresh replica",
			pods: []rolloutPod{rp("a-0", 0, "r1", true), rp("a-1", 1, want, true), rp("b-0", 2, want, true), rp("b-1", 3, want, true)},
			kind: clusterRolloutFailover,
			pod:  "a-1", // promote shard A's fresh replica
		},
		{
			name: "master fresh, its replica stale → roll the replica (not failover)",
			pods: []rolloutPod{rp("a-0", 0, want, true), rp("a-1", 1, "r1", true), rp("b-0", 2, want, true), rp("b-1", 3, want, true)},
			kind: clusterRolloutRollReplica,
			pod:  "a-1",
		},
		{
			name: "unready pod parks the rollout",
			pods: []rolloutPod{rp("a-0", 0, "r1", true), rp("a-1", 1, want, false), rp("b-0", 2, want, true), rp("b-1", 3, want, true)},
			kind: clusterRolloutWait,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := nextClusterRolloutStep(tc.pods, shards, want)
			if got.kind != tc.kind {
				t.Fatalf("kind = %v, want %v", got.kind, tc.kind)
			}
			if tc.pod != "" && got.pod != tc.pod {
				t.Fatalf("pod = %q, want %q", got.pod, tc.pod)
			}
		})
	}
}

// A shard with no replicas can only restart its master directly (nothing to
// fail over to) — verified separately to keep the table above on the HA path.
func TestNextClusterRolloutStepNoReplicaShard(t *testing.T) {
	const want = "rev2"
	shards := []clusterShard{{primary: "m-0", replicas: nil}, {primary: "m-1", replicas: nil}}
	pods := []rolloutPod{rp("m-0", 0, "r1", true), rp("m-1", 1, want, true)}
	got := nextClusterRolloutStep(pods, shards, want)
	if got.kind != clusterRolloutRollMaster || got.pod != "m-0" {
		t.Fatalf("got (%v,%q), want (rollMaster,m-0)", got.kind, got.pod)
	}
}

func TestUpdateStrategyFor(t *testing.T) {
	if got := updateStrategyFor(true).Type; got != appsv1.OnDeleteStatefulSetStrategyType {
		t.Errorf("proactive strategy = %v, want OnDelete", got)
	}
	if got := updateStrategyFor(false).Type; got != appsv1.RollingUpdateStatefulSetStrategyType {
		t.Errorf("default strategy = %v, want RollingUpdate", got)
	}
}
