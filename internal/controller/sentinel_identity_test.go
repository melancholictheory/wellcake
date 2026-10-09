/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// runSentinelInit runs the rendered Sentinel init script in sh, with the config
// and data mounts pointed at temp dirs and fake valkey-cli/timeout binaries:
// peers maps a Sentinel ordinal to its `SENTINEL MASTER` reply.
func runSentinelInit(t *testing.T, vc *cachev1beta1.ValkeyCluster, hostname, oldRuntime string, peers map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	cfg, data, bin := filepath.Join(dir, "cfg"), filepath.Join(dir, "data"), filepath.Join(dir, "bin")
	for _, d := range []string{cfg, data, bin} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(cfg, sentinelConfigName), []byte(renderSentinelConf(vc, "")), 0o644); err != nil {
		t.Fatal(err)
	}
	if oldRuntime != "" {
		if err := os.WriteFile(filepath.Join(data, "runtime-sentinel.conf"), []byte(oldRuntime), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	var cli strings.Builder
	cli.WriteString("#!/bin/sh\ncase \"$2\" in\n")
	for host, reply := range peers {
		fmt.Fprintf(&cli, "  %s) printf '%%s\\n' %s ;;\n", host, reply)
	}
	cli.WriteString("  *) exit 1 ;;\nesac\n")
	writeExec := func(name, body string) {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	writeExec("valkey-cli", cli.String())
	writeExec("timeout", "#!/bin/sh\nshift\nexec \"$@\"\n")

	script := strings.ReplaceAll(renderSentinelInitScript(vc), "/etc/sentinel/", cfg+"/")
	script = strings.ReplaceAll(script, dataMountPath+"/", data+"/")
	cmd := exec.Command("sh", "-c", script)
	cmd.Env = []string{"PATH=" + bin + ":" + os.Getenv("PATH"), "HOSTNAME=" + hostname}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("init script: %v\n%s", err, out)
	}
	b, err := os.ReadFile(filepath.Join(data, "runtime-sentinel.conf"))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestSentinelInitKeepsIdentityAndFollowsPeers(t *testing.T) {
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "ns"},
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology: cachev1beta1.TopologySentinel, Replicas: 3,
			Sentinel: &cachev1beta1.SentinelSpec{Replicas: 3, Quorum: 2},
		},
	}
	master := func(ip string, epoch int) string {
		return fmt.Sprintf("name mymaster ip %s port 6379 config-epoch %d", ip, epoch)
	}
	old := "port 26379\nsentinel myid 0123456789abcdef0123456789abcdef01234567\nsentinel current-epoch 7\nsentinel monitor mymaster 10.0.0.9 6379 2\n"

	// Restart with peers: identity kept, primary taken from the highest epoch.
	got := runSentinelInit(t, vc, "s-sentinel-0", old, map[string]string{
		"s-sentinel-1.s-sentinel": master("10.0.0.2", 3),
		"s-sentinel-2.s-sentinel": master("10.0.0.1", 5),
	})
	for _, want := range []string{
		"sentinel myid 0123456789abcdef0123456789abcdef01234567",
		"sentinel current-epoch 7",
		"sentinel monitor mymaster 10.0.0.1 6379 2",
		"sentinel config-epoch mymaster 5",
	} {
		if !strings.Contains(got, want+"\n") {
			t.Errorf("runtime-sentinel.conf lacks %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "s-0.s-headless") || strings.Count(got, "sentinel monitor ") != 1 {
		t.Errorf("monitor line not replaced exactly once:\n%s", got)
	}

	// First bootstrap: no old file, no peer answers, the ConfigMap stands.
	got = runSentinelInit(t, vc, "s-sentinel-0", "", nil)
	if !strings.Contains(got, "sentinel monitor mymaster s-0.s-headless.ns.svc.cluster.local 6379 2\n") ||
		strings.Contains(got, "myid") || strings.Contains(got, "config-epoch") {
		t.Errorf("bootstrap conf should be the ConfigMap as is:\n%s", got)
	}
}

func TestSentinelServiceSelectsPlaneLabelOnlyOnceAllPodsCarryIt(t *testing.T) {
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "ns"},
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology: cachev1beta1.TopologySentinel, Replicas: 3,
			Sentinel: &cachev1beta1.SentinelSpec{Replicas: 3, Quorum: 2},
		},
	}
	if l := buildSentinelStatefulSet(vc, false).Spec.Template.Labels; l[sentinelPlaneLabel] != componentSentinel {
		t.Fatalf("Sentinel pod template lacks %s: %v", sentinelPlaneLabel, l)
	}
	pod := func(name string, l map[string]string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", Labels: l}}
	}
	selector := func(objs ...client.Object) map[string]string {
		t.Helper()
		scheme := runtime.NewScheme()
		_ = clientgoscheme.AddToScheme(scheme)
		_ = cachev1beta1.AddToScheme(scheme)
		r := &ValkeyClusterReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build(), Scheme: scheme}
		if err := r.ensureSentinelService(context.Background(), vc); err != nil {
			t.Fatal(err)
		}
		var svc corev1.Service
		if err := r.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "s-sentinel"}, &svc); err != nil {
			t.Fatal(err)
		}
		return svc.Spec.Selector
	}
	data := pod("s-0", sentinelLabels(vc)) // same labels as an old Sentinel pod
	// Mid-rollout: one Sentinel still unlabeled, keep the old selector.
	if sel := selector(data, pod("s-sentinel-0", sentinelPodLabels(vc)), pod("s-sentinel-1", sentinelLabels(vc))); sel[sentinelPlaneLabel] != "" {
		t.Errorf("selector switched before every Sentinel pod was labeled: %v", sel)
	}
	// All Sentinels labeled (the data pod never is): switch.
	if sel := selector(data, pod("s-sentinel-0", sentinelPodLabels(vc)), pod("s-sentinel-1", sentinelPodLabels(vc))); sel[sentinelPlaneLabel] != componentSentinel {
		t.Errorf("selector should require %s once all Sentinel pods carry it: %v", sentinelPlaneLabel, sel)
	}
}
