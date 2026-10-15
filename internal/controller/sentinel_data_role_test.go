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

// runSentinelDataRole runs the Sentinel-topology tail of the data init script in
// sh with fake valkey-cli/timeout/hostname binaries and no wait for a failover.
// sentinels maps a Sentinel host to its `SENTINEL MASTER` reply; after the first
// round of queries the replies come from later instead, when it is set (a
// failover). primaries lists the hosts that answer ROLE with master. It returns
// runtime.conf and whether dump.rdb is still there.
func runSentinelDataRole(t *testing.T, vc *cachev1beta1.ValkeyCluster, hostname, podIP string, sentinels, later map[string]string, primaries ...string) (string, bool) {
	t.Helper()
	dir := t.TempDir()
	data, bin := filepath.Join(dir, "data"), filepath.Join(dir, "bin")
	for _, d := range []string{data, bin} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(data, "dump.rdb"), []byte("old resync"), 0o644); err != nil {
		t.Fatal(err)
	}
	var cli strings.Builder
	cli.WriteString("#!/bin/sh\nfor a; do last=$a; done\n")
	cli.WriteString("if [ \"$last\" = ROLE ]; then\n  case \"$2\" in\n")
	for _, h := range primaries {
		fmt.Fprintf(&cli, "    %s) echo master; exit 0 ;;\n", h)
	}
	fmt.Fprintf(&cli, "  esac\n  exit 1\nfi\nn=$(cat %[1]s 2>/dev/null || echo 0)\necho $((n + 1)) > %[1]s\n", filepath.Join(dir, "calls"))
	writeReplies := func(replies map[string]string) {
		cli.WriteString("case \"$2\" in\n")
		for host, reply := range replies {
			fmt.Fprintf(&cli, "  %s) printf '%%s\\n' %s ;;\n", host, reply)
		}
		cli.WriteString("  *) exit 1 ;;\nesac\n")
	}
	if later != nil {
		fmt.Fprintf(&cli, "if [ \"$n\" -ge %d ]; then\n", vc.Spec.Sentinel.Replicas)
		writeReplies(later)
		cli.WriteString("  exit 0\nfi\n")
	}
	writeReplies(sentinels)
	for name, body := range map[string]string{
		"valkey-cli": cli.String(),
		"timeout":    "#!/bin/sh\nshift\nexec \"$@\"\n",
		"hostname":   "#!/bin/sh\necho " + podIP + "\n",
		"sleep":      "#!/bin/sh\n",
		"getent":     "#!/bin/sh\n[ \"$2\" = s-2.s-headless.ns.svc.cluster.local ] && echo \"10.0.0.32 $2\"\nexit 0\n",
	} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	script := strings.ReplaceAll(renderSentinelDataRole(vc), dataMountPath+"/", data+"/")
	script = strings.ReplaceAll(script, fmt.Sprintf("+ %d))", sentinelPrimaryWaitSeconds), "+ 6))")
	cmd := exec.Command("sh", "-c", "set -eu\n"+script)
	cmd.Env = []string{"PATH=" + bin + ":" + os.Getenv("PATH"), "HOSTNAME=" + hostname, "POD_NAMESPACE=ns"}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("init script: %v\n%s", err, out)
	}
	conf, _ := os.ReadFile(filepath.Join(data, "runtime.conf"))
	_, err := os.Stat(filepath.Join(data, "dump.rdb"))
	return string(conf), err == nil
}

func TestSentinelDataPodsFollowTheSentinels(t *testing.T) {
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "ns"},
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology: cachev1beta1.TopologySentinel, Replicas: 3,
			Sentinel: &cachev1beta1.SentinelSpec{Replicas: 3, Quorum: 2},
		},
	}
	fqdn := func(pod string) string { return pod + ".s-headless.ns.svc.cluster.local" }
	master := func(host string, epoch int) string {
		return fmt.Sprintf("name mymaster ip %s port 6379 config-epoch %d", host, epoch)
	}
	agree := func(host string) map[string]string {
		return map[string]string{
			"s-sentinel-0.s-sentinel": master(host, 4),
			"s-sentinel-1.s-sentinel": master("10.0.0.99", 2), // stale, lower epoch
		}
	}

	// pod-0 after a failover: replicate from the primary the Sentinels report,
	// and drop the old resync RDB (persistence is off on Cache).
	conf, rdb := runSentinelDataRole(t, vc, "s-0", "10.0.0.10", agree(fqdn("s-1")), nil, fqdn("s-1"))
	if !strings.Contains(conf, "replicaof "+fqdn("s-1")+" 6379\n") || rdb {
		t.Errorf("pod-0 should replicate from s-1 and drop dump.rdb (dump kept=%v):\n%s", rdb, conf)
	}
	if !strings.Contains(conf, "replica-announce-ip "+fqdn("s-0")+"\n") {
		t.Errorf("missing replica-announce-ip:\n%s", conf)
	}

	// The Sentinels report this pod, by name or by IP: start as the primary and
	// keep the data.
	for _, host := range []string{fqdn("s-2"), "10.0.0.12"} {
		conf, rdb = runSentinelDataRole(t, vc, "s-2", "10.0.0.12", agree(host), nil)
		if strings.Contains(conf, "replicaof") || !rdb {
			t.Errorf("Sentinels report %s (this pod): should start as primary with its data:\n%s", host, conf)
		}
	}

	// First bootstrap, no Sentinel answers: pod-0 primary, the others follow it.
	if conf, _ = runSentinelDataRole(t, vc, "s-0", "10.0.0.10", nil, nil); strings.Contains(conf, "replicaof") {
		t.Errorf("bootstrap pod-0 should be the primary:\n%s", conf)
	}
	if conf, _ = runSentinelDataRole(t, vc, "s-1", "10.0.0.11", nil, nil); !strings.Contains(conf, "replicaof "+fqdn("s-0")+" 6379\n") {
		t.Errorf("bootstrap replica should follow pod-0:\n%s", conf)
	}

	// A restarting primary is still reported under its old IP: wait for the
	// Sentinels to fail over, then replicate from the new primary.
	conf, _ = runSentinelDataRole(t, vc, "s-1", "10.0.0.21", agree("10.0.0.11"), agree(fqdn("s-2")), fqdn("s-2"))
	if !strings.Contains(conf, "replicaof "+fqdn("s-2")+" 6379\n") {
		t.Errorf("should wait for the failover and follow s-2:\n%s", conf)
	}
	// A primary reported by IP is followed by its pod's name.
	conf, _ = runSentinelDataRole(t, vc, "s-0", "10.0.0.10", agree("10.0.0.32"), nil, "10.0.0.32")
	if !strings.Contains(conf, "replicaof "+fqdn("s-2")+" 6379\n") {
		t.Errorf("should replicate from s-2 by name:\n%s", conf)
	}
	// No reachable primary before the deadline: fall back to pod-0.
	conf, _ = runSentinelDataRole(t, vc, "s-1", "10.0.0.21", agree("10.0.0.11"), nil)
	if !strings.Contains(conf, "replicaof "+fqdn("s-0")+" 6379\n") || strings.Contains(conf, "10.0.0.11") {
		t.Errorf("should fall back to pod-0, never the dead address:\n%s", conf)
	}

	// With persistence on, a replica keeps its RDB (partial resync can use it).
	vc.Spec.Profile = cachev1beta1.ProfileDurable
	if _, rdb = runSentinelDataRole(t, vc, "s-0", "10.0.0.10", agree(fqdn("s-1")), nil, fqdn("s-1")); !rdb {
		t.Error("Durable replica must keep dump.rdb")
	}
}
