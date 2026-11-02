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

// When every Sentinel answers but rejects the password (a rotation or an auth
// change in progress), neither the data pod's init nor the primary guard may
// take it for a first bootstrap and start a second primary (#99).
func TestSentinelAuthRejectionIsNotABootstrap(t *testing.T) {
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "ns"},
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology: cachev1beta1.TopologySentinel, Replicas: 3,
			Sentinel: &cachev1beta1.SentinelSpec{Replicas: 3, Quorum: 2},
			Storage:  &cachev1beta1.StorageSpec{Mode: storageModeNone},
		},
	}
	run := func(t *testing.T, script, cliReply string) (string, error) {
		t.Helper()
		dir := t.TempDir()
		data, bin := filepath.Join(dir, "data"), filepath.Join(dir, "bin")
		for _, d := range []string{data, bin} {
			if err := os.MkdirAll(d, 0o755); err != nil {
				t.Fatal(err)
			}
		}
		for name, body := range map[string]string{
			"valkey-cli":    "#!/bin/sh\nprintf '%s\\n' '" + cliReply + "'\n",
			"valkey-server": "#!/bin/sh\necho started-as-server\n",
			"timeout":       "#!/bin/sh\nshift\nexec \"$@\"\n",
			"hostname":      "#!/bin/sh\necho 10.0.0.10\n",
			"sleep":         "#!/bin/sh\n",
			"getent":        "#!/bin/sh\nexit 0\n",
		} {
			if err := os.WriteFile(filepath.Join(bin, name), []byte(body), 0o755); err != nil {
				t.Fatal(err)
			}
		}
		script = strings.ReplaceAll(script, dataMountPath+"/", data+"/")
		cmd := exec.Command("sh", "-c", "set -eu\n"+script)
		cmd.Env = []string{"PATH=" + bin + ":" + os.Getenv("PATH"), "HOSTNAME=s-0", "POD_NAMESPACE=ns"}
		out, err := cmd.CombinedOutput()
		return string(out), err
	}

	for _, reply := range []string{"NOAUTH Authentication required.", "WRONGPASS invalid username-password pair or user is disabled."} {
		// pod-0 is the one that would otherwise start as the first-bootstrap primary.
		out, err := run(t, renderSentinelDataRole(vc), reply)
		if err == nil || !strings.Contains(out, "rejected the password") {
			t.Errorf("data init with %q: want a failed start, got err=%v\n%s", reply, err, out)
		}
		out, err = run(t, renderSentinelPrimaryGuard(vc), reply)
		if err == nil || strings.Contains(out, "started-as-server") {
			t.Errorf("primary guard with %q: want no valkey-server start, got err=%v\n%s", reply, err, out)
		}
	}

	// No Sentinel at all (first bootstrap) still starts pod-0 as the primary.
	out, err := run(t, renderSentinelDataRole(vc), "")
	if err != nil {
		t.Errorf("first bootstrap must still start: %v\n%s", err, out)
	}
}
