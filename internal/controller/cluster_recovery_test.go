/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

func mkFencePod(node string, phase corev1.PodPhase, ready bool) *corev1.Pod {
	cond := corev1.ConditionFalse
	if ready {
		cond = corev1.ConditionTrue
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "c-0"},
		Spec:       corev1.PodSpec{NodeName: node},
		Status: corev1.PodStatus{
			Phase:      phase,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: cond}},
		},
	}
}

func mkNode(name string, ready bool) *corev1.Node {
	cond := corev1.ConditionFalse
	if ready {
		cond = corev1.ConditionTrue
	}
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status:     corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: cond}}},
	}
}

// TestPrimaryFencedByK8s locks down the split-brain guard: the operator may take
// a down primary's slots over ONLY when the pod is provably not serving, and must
// REFUSE when the pod is Running+Ready on a Ready node — where (with only a TCP
// readiness probe) it might still be serving clients on the far side of a
// Valkey-level partition.
func TestPrimaryFencedByK8s(t *testing.T) {
	ns := "ns"
	vc := &cachev1beta1.ValkeyCluster{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "c"}}

	cases := []struct {
		name string
		objs []client.Object
		want bool
	}{
		{
			name: "pod missing → fenced",
			objs: nil,
			want: true,
		},
		{
			name: "pod not Running (Failed) → fenced",
			objs: []client.Object{mkFencePod("n1", corev1.PodFailed, false), mkNode("n1", true)},
			want: true,
		},
		{
			name: "pod Running but not Ready → fenced",
			objs: []client.Object{mkFencePod("n1", corev1.PodRunning, false), mkNode("n1", true)},
			want: true,
		},
		{
			name: "pod Running+Ready but node NotReady → fenced (stale kubelet status)",
			objs: []client.Object{mkFencePod("n1", corev1.PodRunning, true), mkNode("n1", false)},
			want: true,
		},
		{
			name: "pod Running+Ready but node gone → fenced",
			objs: []client.Object{mkFencePod("goneNode", corev1.PodRunning, true)},
			want: true,
		},
		{
			name: "pod Running+Ready on Ready node → NOT fenced (split-brain guard holds)",
			objs: []client.Object{mkFencePod("n1", corev1.PodRunning, true), mkNode("n1", true)},
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scheme := newTestScheme(t)
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.objs...).Build()
			r := &ValkeyClusterReconciler{Client: c, Scheme: scheme}
			got, err := r.primaryFencedByK8s(context.Background(), vc, "c-0")
			if err != nil {
				t.Fatalf("primaryFencedByK8s: %v", err)
			}
			if got != tc.want {
				t.Fatalf("fenced=%v, want %v", got, tc.want)
			}
		})
	}
}

// TestQuorumDownDebounce verifies the recovery debounce: the first observation
// only records the marker (so gossip gets its window), and recovery fires only
// once the cluster has been stuck longer than quorumRecoveryDownAfter.
func TestQuorumDownDebounce(t *testing.T) {
	scheme := newTestScheme(t)
	vc := &cachev1beta1.ValkeyCluster{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "c"}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(vc).WithStatusSubresource(vc).Build()
	r := &ValkeyClusterReconciler{Client: c, Scheme: scheme}
	log := logr.Discard()
	ctx := context.Background()

	if r.quorumDownLongEnough(ctx, vc, log) {
		t.Fatal("first observation must start the debounce, not fire")
	}
	if vc.Status.QuorumDownSince == nil {
		t.Fatal("QuorumDownSince should have been recorded on first observation")
	}

	// Backdate the marker beyond the window: recovery should now fire.
	old := metav1.NewTime(time.Now().Add(-2 * quorumRecoveryDownAfter))
	r.setQuorumDown(ctx, vc, &old)
	if !r.quorumDownLongEnough(ctx, vc, log) {
		t.Fatal("after the window elapsed, debounce should fire")
	}

	r.clearQuorumDown(ctx, vc)
	if vc.Status.QuorumDownSince != nil {
		t.Fatal("clearQuorumDown should reset the marker")
	}
}

// TestCountMasters checks the majority-gate input: only fail/fail?-free,
// connected masters count as able to vote.
func TestCountMasters(t *testing.T) {
	nodes := parseClusterNodes(
		"a 10.0.0.1:6379@16379,c-0 master - 0 0 1 connected 0-5460\n" +
			"b 10.0.0.2:6379@16379,c-1 master - 0 0 2 connected 5461-10922\n" +
			"c 10.0.0.3:6379@16379,c-2 master,fail - 0 0 3 disconnected 10923-16383\n" +
			"d 10.0.0.4:6379@16379,c-3 slave a 0 0 1 connected\n")
	healthy, total := countMasters(nodes)
	if total != 3 {
		t.Fatalf("total masters=%d, want 3", total)
	}
	if healthy != 2 {
		t.Fatalf("healthy masters=%d, want 2 (one is fail/disconnected)", healthy)
	}
	// 3 masters, 2 healthy → 2*2 > 3 → quorum still exists → gate must NOT fire.
	if healthy*2 <= total {
		t.Fatal("majority gate would fire with a surviving quorum (2 of 3)")
	}
	// Lose a second master → 1 healthy of 3 → 1*2 <= 3 → gate fires.
	nodes[1].flags = []string{"master", "fail"}
	nodes[1].linkState = "disconnected"
	if h, tot := countMasters(nodes); h*2 > tot {
		t.Fatalf("majority gate should fire at %d/%d healthy", h, tot)
	}
}

// TestMajorityGateEvenN checks the even-N boundary: with 4 masters, gossip needs
// 3 votes (floor(4/2)+1), so exactly 2 healthy is already below quorum and the
// gate (healthy*2 <= total) must fire; 3 healthy must not.
func TestMajorityGateEvenN(t *testing.T) {
	for _, tc := range []struct {
		healthy, total int
		wantFire       bool
	}{
		{3, 4, false}, // 3 of 4 healthy → quorum intact → leave to gossip
		{2, 4, true},  // 2 of 4 → below quorum (needs 3) → operator may act
		{2, 3, false}, // 2 of 3 → quorum intact (needs 2) → leave to gossip
		{1, 3, true},  // 1 of 3 → below quorum → operator may act
		{1, 2, true},  // 1 of 2 → below quorum (needs 2) → operator may act
	} {
		fire := tc.healthy*2 <= tc.total
		if fire != tc.wantFire {
			t.Fatalf("healthy=%d total=%d: gate fires=%v, want %v", tc.healthy, tc.total, fire, tc.wantFire)
		}
	}
}

// TestRecoverableShardsFenceGate verifies the k8s half of the conjunction runs
// BEFORE any pod is dialled: a down primary whose pod is Running+Ready on a Ready
// node (not fenced) is filtered out, and so is one whose CLUSTER NODES name is not
// a known data pod. Both are rejected without a takeover target, using only the
// fake k8s client (no network).
func TestRecoverableShardsFenceGate(t *testing.T) {
	scheme := newTestScheme(t)
	shards := int32(3)
	repl := int32(1)
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "qtest"},
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology: cachev1beta1.TopologyCluster, Shards: &shards, ReplicasPerShard: &repl,
		},
	}
	// qtest-0 is a down primary but Running+Ready on a Ready node → NOT fenced.
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "qtest-0"},
			Spec:       corev1.PodSpec{NodeName: "n1"},
			Status: corev1.PodStatus{Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
		},
		mkNode("n1", true),
	).Build()
	r := &ValkeyClusterReconciler{Client: c, Scheme: scheme}

	nodes := []clusterNode{
		{id: "m0", podName: "qtest-0", flags: []string{"master", "fail"}, linkState: "disconnected"},  // known but NOT fenced
		{id: "m1", podName: "10.0.0.9", flags: []string{"master", "fail"}, linkState: "disconnected"}, // unknown name (bare IP)
		{id: "r0", podName: "qtest-3", flags: []string{"slave"}, masterID: "m0", linkState: "connected"},
	}
	got := r.recoverableShards(context.Background(), vc, "", nodes)
	if len(got) != 0 {
		t.Fatalf("expected no recoverable shards (one not-fenced, one unknown name), got %d: %+v", len(got), got)
	}
}

// TestQuorumRecoveryEnabled checks the profile/annotation gating: Cache recovers
// by default, Durable only on explicit opt-in, and an explicit annotation always
// wins.
func TestQuorumRecoveryEnabled(t *testing.T) {
	mk := func(profile cachev1beta1.Profile, ann string) *cachev1beta1.ValkeyCluster {
		vc := &cachev1beta1.ValkeyCluster{Spec: cachev1beta1.ValkeyClusterSpec{Profile: profile}}
		if ann != "" {
			vc.Annotations = map[string]string{quorumTakeoverAnnotation: ann}
		}
		return vc
	}
	cases := []struct {
		name    string
		profile cachev1beta1.Profile
		ann     string
		want    bool
	}{
		{"cache default on", cachev1beta1.ProfileCache, "", true},
		{"durable default off", cachev1beta1.ProfileDurable, "", false},
		{"durable opt-in on", cachev1beta1.ProfileDurable, "true", true},
		{"cache opt-out off", cachev1beta1.ProfileCache, "false", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := quorumRecoveryEnabled(mk(tc.profile, tc.ann)); got != tc.want {
				t.Fatalf("quorumRecoveryEnabled=%v, want %v", got, tc.want)
			}
		})
	}
}

// TestNodeReady covers the node-liveness cross-check the fence relies on.
func TestNodeReady(t *testing.T) {
	scheme := newTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(mkNode("ready", true), mkNode("down", false)).Build()
	r := &ValkeyClusterReconciler{Client: c, Scheme: scheme}
	ctx := context.Background()

	for _, tc := range []struct {
		node string
		want bool
	}{
		{"ready", true},
		{"down", false},
		{"missing", false}, // a deleted node counts as not-ready
	} {
		got, err := r.nodeReady(ctx, tc.node)
		if err != nil {
			t.Fatalf("nodeReady(%s): %v", tc.node, err)
		}
		if got != tc.want {
			t.Fatalf("nodeReady(%s)=%v, want %v", tc.node, got, tc.want)
		}
	}
}
