/*
Copyright 2026 The Wellcake Authors.
*/

package main

import (
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

func joinArgs(a []string) string { return strings.Join(a, " ") }

func TestBuildCliExecArgsReplicationWithAuth(t *testing.T) {
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "web-cache", Namespace: "web"},
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology: cachev1beta1.TopologyReplication,
			Auth:     &cachev1beta1.AuthSpec{Enabled: true},
		},
	}
	argv := buildCliExecArgs(vc, "web", "web-cache-0", "s3cr3t", []string{"INFO", "replication"})
	got := joinArgs(argv)

	for _, want := range []string{
		"exec -it -n web web-cache-0 -c valkey --",
		"env REDISCLI_AUTH=s3cr3t",
		"valkey-cli -p 6379",
		"INFO replication",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("argv missing %q\n%s", want, got)
		}
	}
	if strings.Contains(got, "--tls") {
		t.Errorf("non-TLS cluster should not pass --tls\n%s", got)
	}
	if strings.Contains(got, " -c\n") || strings.HasSuffix(got, " -c") {
		t.Errorf("replication topology must not pass cluster-mode -c\n%s", got)
	}
}

func TestBuildCliExecArgsClusterTLSNoAuth(t *testing.T) {
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "demo"},
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology: cachev1beta1.TopologyCluster,
			Shards:   ptr.To[int32](3),
			TLS:      &cachev1beta1.TLSSpec{Enabled: true},
		},
	}
	argv := buildCliExecArgs(vc, "demo", "demo-0", "", nil)
	got := joinArgs(argv)

	for _, want := range []string{
		"valkey-cli -p 6380",
		"--tls --cert " + tlsMountPath + "/tls.crt",
		"--key " + tlsMountPath + "/tls.key",
		"--cacert " + tlsMountPath + "/ca.crt",
		"-c", // cluster mode flag
	} {
		if !strings.Contains(got, want) {
			t.Errorf("argv missing %q\n%s", want, got)
		}
	}
	if strings.Contains(got, "REDISCLI_AUTH") {
		t.Errorf("no auth → must not set REDISCLI_AUTH\n%s", got)
	}
}

func TestTargetPod(t *testing.T) {
	repl := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "web"},
		Spec:       cachev1beta1.ValkeyClusterSpec{Topology: cachev1beta1.TopologyReplication},
		Status:     cachev1beta1.ValkeyClusterStatus{Primary: "web-2"},
	}
	if got := targetPod(repl); got != "web-2" {
		t.Errorf("replication target = %q, want web-2 (observed primary)", got)
	}

	replNoPrimary := repl.DeepCopy()
	replNoPrimary.Status.Primary = ""
	if got := targetPod(replNoPrimary); got != "web-0" {
		t.Errorf("replication without primary target = %q, want web-0", got)
	}

	cl := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo"},
		Spec:       cachev1beta1.ValkeyClusterSpec{Topology: cachev1beta1.TopologyCluster},
		Status:     cachev1beta1.ValkeyClusterStatus{Primary: "ignored"},
	}
	if got := targetPod(cl); got != "demo-0" {
		t.Errorf("cluster target = %q, want demo-0 (pod-0, primary ignored)", got)
	}
}

func TestJobFromCronJob(t *testing.T) {
	cj := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{Name: "web-cache-backup", Namespace: "web"},
		Spec: batchv1.CronJobSpec{
			Schedule: "0 3 * * *",
			JobTemplate: batchv1.JobTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "valkey"}},
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyOnFailure,
							Containers:    []corev1.Container{{Name: "backup", Image: "aws-cli"}},
						},
					},
				},
			},
		},
	}
	job := jobFromCronJob(cj)

	if job.GenerateName != "web-cache-backup-manual-" {
		t.Errorf("GenerateName = %q, want web-cache-backup-manual-", job.GenerateName)
	}
	if job.Namespace != "web" {
		t.Errorf("namespace = %q, want web", job.Namespace)
	}
	if job.Labels["app"] != "valkey" || job.Labels["valkey.wellcake.io/manual-backup"] != "true" {
		t.Errorf("labels not carried/marked: %v", job.Labels)
	}
	if len(job.Spec.Template.Spec.Containers) != 1 || job.Spec.Template.Spec.Containers[0].Name != "backup" {
		t.Errorf("job spec not copied from CronJob template: %+v", job.Spec.Template.Spec)
	}
}
