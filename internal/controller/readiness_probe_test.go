/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// The data pods' readiness probe runs in sh with a fake valkey-cli that prints
// a canned INFO reply (or fails like a server that does not answer).
func TestDataReadinessProbe(t *testing.T) {
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "ns"},
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology: cachev1beta1.TopologySentinel, Replicas: 3,
			Auth: &cachev1beta1.AuthSpec{Enabled: new(true)},
		},
	}
	probe := buildStatefulSet(vc, "h", false).Spec.Template.Spec.Containers[0].ReadinessProbe
	if probe == nil || probe.Exec == nil {
		t.Fatalf("readiness probe = %+v, want an exec probe", probe)
	}
	cmd := probe.Exec.Command
	if len(cmd) != 3 || !strings.Contains(cmd[2], "INFO persistence replication") || !strings.Contains(cmd[2], `-a "$VALKEY_PASSWORD"`) {
		t.Fatalf("readiness command = %q", cmd)
	}

	run := func(reply string, exit int) bool {
		t.Helper()
		bin := t.TempDir()
		body := "#!/bin/sh\nprintf '%s' '" + reply + "'\nexit " + string(rune('0'+exit)) + "\n"
		if err := os.WriteFile(filepath.Join(bin, "valkey-cli"), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
		c := exec.Command(cmd[0], cmd[1], cmd[2])
		c.Env = []string{"PATH=" + bin + ":" + os.Getenv("PATH"), "VALKEY_PASSWORD=pw"}
		return c.Run() == nil
	}
	info := func(loading, sync string) string {
		return "# Persistence\r\nloading:" + loading + "\r\nasync_loading:0\r\n# Replication\r\nrole:slave\r\nmaster_link_status:up\r\nmaster_sync_in_progress:" + sync + "\r\n"
	}
	for name, c := range map[string]struct {
		reply string
		exit  int
		ready bool
	}{
		"in sync":                {info("0", "0"), 0, true},
		"loading an RDB":         {info("1", "0"), 0, false},
		"receiving the RDB":      {info("0", "1"), 0, false},
		"primary, link down":     {"# Persistence\r\nloading:0\r\n# Replication\r\nrole:master\r\nconnected_slaves:0\r\n", 0, true},
		"NOAUTH (old password)":  {"NOAUTH Authentication required.\n", 0, true},
		"server does not answer": {"Could not connect to Valkey at 127.0.0.1:6379: Connection refused\n", 1, false},
	} {
		if got := run(c.reply, c.exit); got != c.ready {
			t.Errorf("%s: ready = %v, want %v", name, got, c.ready)
		}
	}

	// Liveness stays a plain check: a pod that loads for minutes must not be
	// restarted.
	if live := buildStatefulSet(vc, "h", false).Spec.Template.Spec.Containers[0].LivenessProbe; live == nil || live.TCPSocket == nil {
		t.Errorf("liveness probe = %+v, want the TCP check", live)
	}
}
