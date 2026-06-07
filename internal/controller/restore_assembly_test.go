/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

func TestClusterRestoreManifestKey(t *testing.T) {
	cases := []struct{ src, want string }{
		{"b/kv-20260602-030000-shard-{shard}.rdb", "b/kv-20260602-030000-manifest.txt"},
		{"backups/web-20260528-030000-shard-{shard}.rdb", "backups/web-20260528-030000-manifest.txt"},
	}
	for _, tc := range cases {
		if got := clusterRestoreManifestKey(tc.src); got != tc.want {
			t.Errorf("clusterRestoreManifestKey(%q) = %q, want %q", tc.src, got, tc.want)
		}
	}
}

func TestBuildRestoreAssemblyJob(t *testing.T) {
	shards := int32(3)
	rps := int32(1)
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "kv", Namespace: "ns"},
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology:         cachev1beta1.TopologyCluster,
			Shards:           &shards,
			ReplicasPerShard: &rps,
			Image:            "valkey:8.0",
			RestoreFrom: &cachev1beta1.RestoreSpec{
				SourceKey: "b/kv-20260602-030000-shard-{shard}.rdb",
				S3:        &cachev1beta1.S3Spec{Bucket: "bkt", Region: "r", CredentialsSecret: "c"},
			},
		},
	}
	pod := buildRestoreAssemblyJob(vc, "pw", "kv-restore-assembly").Spec.Template.Spec

	if len(pod.InitContainers) != 1 {
		t.Fatalf("want 1 init (fetch-manifest) container, got %d", len(pod.InitContainers))
	}
	fetch := pod.InitContainers[0].Command[len(pod.InitContainers[0].Command)-1]
	if !strings.Contains(fetch, "manifest.txt") || !strings.Contains(fetch, "aws s3 cp") {
		t.Errorf("init container must fetch the manifest:\n%s", fetch)
	}
	asm := pod.Containers[0].Command[len(pod.Containers[0].Command)-1]
	for _, want := range []string{
		// gap-fill (not a blind full-range addslotsrange), MEET by getent-resolved
		// IP, replicate; SET-CONFIG-EPOCH is intentionally NOT issued.
		"cluster addslotsrange", "getent hosts", "cluster meet",
		"cluster replicate", "/work/manifest.txt", "cluster_state:",
	} {
		if !strings.Contains(asm, want) {
			t.Errorf("assembly script missing %q:\n%s", want, asm)
		}
	}
	// The work volume must be shared between the two containers.
	mounts := map[string]bool{}
	for _, m := range pod.InitContainers[0].VolumeMounts {
		mounts["init:"+m.Name] = true
	}
	for _, m := range pod.Containers[0].VolumeMounts {
		mounts["main:"+m.Name] = true
	}
	if !mounts["init:work"] || !mounts["main:work"] {
		t.Errorf("work emptyDir must be mounted in both containers, got %v", mounts)
	}
}
