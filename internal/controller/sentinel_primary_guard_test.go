/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// runPrimaryGuard runs the data container entrypoint with valkey-server
// replaced by an echo. The Sentinels answer `first` on the first round of
// queries and `later` afterwards (a failover); primaries answer ROLE master.
func runPrimaryGuard(t *testing.T, vc *cachev1beta1.ValkeyCluster, runtimeConf string, first, later map[string]string, primaries ...string) (conf, out string, rdbKept bool) {
	t.Helper()
	dir := t.TempDir()
	data, bin := filepath.Join(dir, "data"), filepath.Join(dir, "bin")
	for _, d := range []string{data, bin} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(os.WriteFile(filepath.Join(data, "runtime.conf"), []byte(runtimeConf), 0o644))
	must(os.WriteFile(filepath.Join(data, "dump.rdb"), []byte("old resync"), 0o644))

	var cli strings.Builder
	cli.WriteString("#!/bin/sh\nfor a; do last=$a; done\nif [ \"$last\" = ROLE ]; then\n  case \"$2\" in\n")
	for _, h := range primaries {
		fmt.Fprintf(&cli, "    %s) echo master; exit 0 ;;\n", h)
	}
	fmt.Fprintf(&cli, "  esac\n  exit 1\nfi\nn=$(cat %[1]s 2>/dev/null || echo 0)\necho $((n + 1)) > %[1]s\n", filepath.Join(dir, "calls"))
	replies := func(m map[string]string) {
		cli.WriteString("case \"$2\" in\n")
		for host, reply := range m {
			fmt.Fprintf(&cli, "  %s) printf '%%s\\n' %s ;;\n", host, reply)
		}
		cli.WriteString("  *) exit 1 ;;\nesac\n")
	}
	if later != nil {
		fmt.Fprintf(&cli, "if [ \"$n\" -ge %d ]; then\n", vc.Spec.Sentinel.Replicas)
		replies(later)
		cli.WriteString("  exit 0\nfi\n")
	}
	replies(first)
	for name, body := range map[string]string{
		"valkey-cli": cli.String(),
		"timeout":    "#!/bin/sh\nshift\nexec \"$@\"\n",
		"hostname":   "#!/bin/sh\necho 10.0.0.10\n",
		"sleep":      "#!/bin/sh\n",
		"getent":     "#!/bin/sh\n[ \"$2\" = s-1.s-headless.ns.svc.cluster.local ] && echo \"10.0.0.11 $2\"\nexit 0\n",
	} {
		must(os.WriteFile(filepath.Join(bin, name), []byte(body), 0o755))
	}
	script := strings.ReplaceAll(renderSentinelPrimaryGuard(vc), dataMountPath+"/", data+"/")
	script = strings.ReplaceAll(script, fmt.Sprintf("+ %d))", sentinelPrimaryWaitSeconds), "+ 6))")
	script = strings.ReplaceAll(script, `exec valkey-server "$CONF"`, `echo STARTED`)
	cmd := exec.Command("sh", "-c", script)
	cmd.Env = []string{"PATH=" + bin + ":" + os.Getenv("PATH"), "HOSTNAME=s-0", "POD_NAMESPACE=ns"}
	b, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(b), "STARTED") {
		t.Fatalf("entrypoint: %v\n%s", err, b)
	}
	c, _ := os.ReadFile(filepath.Join(data, "runtime.conf"))
	_, statErr := os.Stat(filepath.Join(data, "dump.rdb"))
	return string(c), string(b), statErr == nil
}

func TestSentinelPrimaryGuardNeverRestartsAsPrimaryFromDisk(t *testing.T) {
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "ns"},
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology: cachev1beta1.TopologySentinel, Replicas: 3,
			Sentinel: &cachev1beta1.SentinelSpec{Replicas: 3, Quorum: 2},
		},
	}
	if !sentinelPrimaryGuardActive(vc) {
		t.Fatal("guard should be active for Sentinel with persistence off")
	}
	reports := func(host string) map[string]string {
		return map[string]string{"s-sentinel-1.s-sentinel": fmt.Sprintf("name mymaster ip %s port 6379 config-epoch 4", host)}
	}

	// A replica restarting in place: drop the old resync, start as a replica.
	conf, _, rdb := runPrimaryGuard(t, vc, "replicaof s-1.s-headless.ns.svc.cluster.local 6379\n", nil, nil)
	if rdb || strings.Count(conf, "replicaof") != 1 {
		t.Errorf("replica should drop dump.rdb and keep one replicaof:\n%s", conf)
	}

	// First bootstrap: no Sentinel answers, pod-0 starts as the primary.
	conf, _, rdb = runPrimaryGuard(t, vc, "", nil, nil)
	if !rdb || strings.Contains(conf, "replicaof") {
		t.Errorf("bootstrap primary should keep its data and start as primary:\n%s", conf)
	}

	// The primary's process restarted: the Sentinels still name this pod. It
	// waits for the failover, then replicates from the promoted pod by name.
	conf, out, rdb := runPrimaryGuard(t, vc, "", reports("10.0.0.10"), reports("10.0.0.11"), "10.0.0.11")
	if rdb || !strings.Contains(conf, "replicaof s-1.s-headless.ns.svc.cluster.local 6379\n") {
		t.Errorf("restarted primary should follow the promoted replica and drop dump.rdb:\n%s\n%s", conf, out)
	}

	// No failover before the deadline (no replica to promote): start as the
	// primary with what is on disk rather than not at all.
	conf, _, rdb = runPrimaryGuard(t, vc, "", reports("s-0.s-headless.ns.svc.cluster.local"), nil)
	if !rdb || strings.Contains(conf, "replicaof") {
		t.Errorf("without a failover the pod should start as primary with its data:\n%s", conf)
	}

	// Persistence on: the container starts valkey-server directly.
	vc.Spec.Profile = cachev1beta1.ProfileDurable
	if sentinelPrimaryGuardActive(vc) {
		t.Error("guard must not apply when persistence is on")
	}
	if c := buildStatefulSet(vc, "h", false).Spec.Template.Spec.Containers[0]; c.Command[0] != "valkey-server" {
		t.Errorf("Durable data container command = %v", c.Command)
	}
}
