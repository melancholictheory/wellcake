/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// --- restore init container: per-shard (Cluster) vs single-key ---

func TestBuildRestoreInitContainerPerShard(t *testing.T) {
	r := &cachev1beta1.RestoreSpec{
		S3:        &cachev1beta1.S3Spec{Bucket: "b", Region: "r", CredentialsSecret: "creds"},
		SourceKey: "backups/web-20260528-shard-{shard}.rdb",
	}

	// Cluster topology → per-shard script: ordinal from $HOSTNAME, masters-only
	// guard against the shard count, and {shard} substitution.
	cl := &cachev1beta1.ValkeyCluster{
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology:    cachev1beta1.TopologyCluster,
			Shards:      ptr.To[int32](3),
			RestoreFrom: r,
		},
	}
	got := buildRestoreInitContainer(cl)
	script := got.Command[len(got.Command)-1]
	for _, want := range []string{`ORD="${HOSTNAME##*-}"`, `-ge 3`, `s/{shard}/$ORD/g`} {
		if !strings.Contains(script, want) {
			t.Errorf("cluster restore script missing %q:\n%s", want, script)
		}
	}

	// Single-shard topology → verbatim key, no per-shard logic.
	rep := &cachev1beta1.ValkeyCluster{
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology:    cachev1beta1.TopologyReplication,
			RestoreFrom: &cachev1beta1.RestoreSpec{S3: r.S3, SourceKey: "backups/web.rdb"},
		},
	}
	repScript := rep.Spec.RestoreFrom.SourceKey
	gotRep := buildRestoreInitContainer(rep)
	rs := gotRep.Command[len(gotRep.Command)-1]
	if strings.Contains(rs, "HOSTNAME##") || strings.Contains(rs, "{shard}") {
		t.Errorf("single-shard restore script must not have per-shard logic:\n%s", rs)
	}
	if !strings.Contains(rs, repScript) {
		t.Errorf("single-shard restore script missing verbatim key %q:\n%s", repScript, rs)
	}
}

// --- renderValkeyConf: profile / persistence / maxmemory matrix ---

func TestRenderValkeyConfProfiles(t *testing.T) {
	withMem := func(p cachev1beta1.Profile, lim string) *cachev1beta1.ValkeyCluster {
		vc := minimalCR()
		vc.Spec.Topology = cachev1beta1.TopologyStandalone
		vc.Spec.Profile = p
		if lim != "" {
			vc.Spec.Resources.Limits = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse(lim)}
		}
		return vc
	}

	// Cache: allkeys-lru eviction, persistence off, maxmemory ~60% of limit.
	cache := renderValkeyConf(withMem(cachev1beta1.ProfileCache, "1Gi"), "")
	for _, want := range []string{"maxmemory-policy allkeys-lru", "appendonly no", "maxmemory 644245094b"} {
		if !strings.Contains(cache, want) {
			t.Errorf("cache conf missing %q\n%s", want, cache)
		}
	}

	// Durable: noeviction, AOF on with save rules.
	durable := renderValkeyConf(withMem(cachev1beta1.ProfileDurable, "1Gi"), "")
	for _, want := range []string{"maxmemory-policy noeviction", "appendonly yes", "save 3600 1"} {
		if !strings.Contains(durable, want) {
			t.Errorf("durable conf missing %q\n%s", want, durable)
		}
	}

	// No memory limit → no maxmemory directive (operator must not invent one).
	noLimit := renderValkeyConf(withMem(cachev1beta1.ProfileCache, ""), "")
	if strings.Contains(noLimit, "\nmaxmemory ") {
		t.Errorf("no memory limit must not emit maxmemory\n%s", noLimit)
	}

	// Password → requirepass + masterauth.
	withPass := renderValkeyConf(withMem(cachev1beta1.ProfileCache, "1Gi"), "s3cr3t")
	if !strings.Contains(withPass, `requirepass "s3cr3t"`) || !strings.Contains(withPass, `masterauth "s3cr3t"`) {
		t.Errorf("password conf must set requirepass + masterauth\n%s", withPass)
	}

	// spec.config overrides the computed defaults.
	ovr := withMem(cachev1beta1.ProfileCache, "1Gi")
	ovr.Spec.Config = map[string]string{"maxmemory-policy": "volatile-lru"}
	if c := renderValkeyConf(ovr, ""); !strings.Contains(c, "maxmemory-policy volatile-lru") {
		t.Errorf("spec.config must override maxmemory-policy\n%s", c)
	}
}

// --- Sentinel topology builders ---

func sentinelCR() *cachev1beta1.ValkeyCluster {
	vc := minimalCR()
	vc.Spec.Topology = cachev1beta1.TopologySentinel
	vc.Spec.Sentinel = &cachev1beta1.SentinelSpec{Replicas: 3}
	return vc
}

func TestRenderSentinelConf(t *testing.T) {
	vc := sentinelCR()

	// Default quorum = replicas/2 + 1 = 2 for 3 replicas.
	conf := renderSentinelConf(vc, "")
	if !strings.Contains(conf, "mymaster") || !strings.Contains(conf, "5000") {
		t.Errorf("sentinel conf missing monitor/down-after\n%s", conf)
	}
	if !strings.Contains(conf, "-0.") { // primary points at pod-0
		t.Errorf("sentinel must monitor pod-0 as primary\n%s", conf)
	}
	// quorum is the trailing number on the monitor line; for 3 replicas → 2.
	if !strings.Contains(conf, " 2\n") {
		t.Errorf("default quorum for 3 replicas should be 2\n%s", conf)
	}

	// Explicit quorum override.
	vc.Spec.Sentinel.Quorum = 3
	if c := renderSentinelConf(vc, ""); !strings.Contains(c, " 3\n") {
		t.Errorf("explicit quorum 3 not honored\n%s", c)
	}

	// Password → sentinel auth-user (dedicated ACL user) + auth-pass + requirepass.
	if c := renderSentinelConf(sentinelCR(), "pw"); !strings.Contains(c, `auth-pass mymaster "pw"`) || !strings.Contains(c, `requirepass "pw"`) {
		t.Errorf("sentinel auth not rendered\n%s", c)
	}
	if c := renderSentinelConf(sentinelCR(), "pw"); !strings.Contains(c, "auth-user mymaster sentinel-user") {
		t.Errorf("sentinel must authenticate to master as the dedicated ACL user\n%s", c)
	}
	if c := renderSentinelConf(sentinelCR(), `abc"def`); !strings.Contains(c, `auth-pass mymaster "abc\"def"`) || !strings.Contains(c, `requirepass "abc\"def"`) {
		t.Errorf("sentinel auth password must be escaped\n%s", c)
	}

	// TLS → tls-port + plaintext disabled + tls-replication.
	tlsCR := sentinelCR()
	tlsCR.Spec.TLS = &cachev1beta1.TLSSpec{Enabled: true}
	if c := renderSentinelConf(tlsCR, ""); !strings.Contains(c, "tls-replication yes") || !strings.Contains(c, "port 0") {
		t.Errorf("sentinel TLS block missing\n%s", c)
	}
}

func TestBuildSentinelStatefulSet(t *testing.T) {
	vc := sentinelCR()
	vc.Spec.Image = "valkey/valkey:8.0"
	sts := buildSentinelStatefulSet(vc, false)

	if sts.Spec.Replicas == nil || *sts.Spec.Replicas != 3 {
		t.Errorf("sentinel STS replicas = %v, want 3", sts.Spec.Replicas)
	}
	// Default (non-proactive) keeps RollingUpdate.
	if sts.Spec.UpdateStrategy.Type != appsv1.RollingUpdateStatefulSetStrategyType {
		t.Errorf("sentinel STS strategy = %v, want RollingUpdate", sts.Spec.UpdateStrategy.Type)
	}
	// Opting into the proactive rollout flips the Sentinel STS to OnDelete.
	if got := buildSentinelStatefulSet(vc, true).Spec.UpdateStrategy.Type; got != appsv1.OnDeleteStatefulSetStrategyType {
		t.Errorf("proactive sentinel STS strategy = %v, want OnDelete", got)
	}
	// Image falls back to the Valkey image when Sentinel.Image is empty.
	if img := sts.Spec.Template.Spec.Containers[0].Image; img != "valkey/valkey:8.0" {
		t.Errorf("sentinel image = %q, want fallback to spec.image", img)
	}
	// A dedicated Sentinel image is honored when set.
	vc.Spec.Sentinel.Image = "valkey/valkey:8.1"
	if img := buildSentinelStatefulSet(vc, false).Spec.Template.Spec.Containers[0].Image; img != "valkey/valkey:8.1" {
		t.Errorf("sentinel image override = %q, want valkey/valkey:8.1", img)
	}

	// TLS mounts the tls volume.
	tlsCR := sentinelCR()
	tlsCR.Spec.TLS = &cachev1beta1.TLSSpec{Enabled: true}
	hasTLSVol := false
	for _, v := range buildSentinelStatefulSet(tlsCR, false).Spec.Template.Spec.Volumes {
		if v.Name == tlsVolumeName {
			hasTLSVol = true
		}
	}
	if !hasTLSVol {
		t.Errorf("TLS sentinel STS must mount the tls volume")
	}
}

// --- Cluster bootstrap Job ---

func TestBuildBootstrapJob(t *testing.T) {
	vc := minimalCR()
	vc.Spec.Topology = cachev1beta1.TopologyCluster
	vc.Spec.Shards = ptr.To[int32](3)
	vc.Spec.ReplicasPerShard = ptr.To[int32](1) // total = 3 * (1+1) = 6

	s := jobScript(buildBootstrapJob(vc, "", "c-bootstrap"))
	// One node entry per pod in the create command.
	if n := strings.Count(s, "-headless.ns.svc.cluster.local:6379"); n < 6 {
		t.Errorf("bootstrap should reference 6 nodes, found %d\n%s", n, s)
	}
	for _, want := range []string{"--cluster create", "--cluster-replicas 1", "--cluster-yes", "ping"} {
		if !strings.Contains(s, want) {
			t.Errorf("bootstrap script missing %q\n%s", want, s)
		}
	}

	// Password → -a flag in the command + env on the container.
	job := buildBootstrapJob(vc, "pw", "c-bootstrap")
	if !strings.Contains(jobScript(job), "-a \"$VALKEY_PASSWORD\"") {
		t.Errorf("password bootstrap must pass -a")
	}
	hasEnv := false
	for _, e := range job.Spec.Template.Spec.Containers[0].Env {
		if e.Name == "VALKEY_PASSWORD" {
			hasEnv = true
		}
	}
	if !hasEnv {
		t.Errorf("password bootstrap must inject VALKEY_PASSWORD env")
	}

	// TLS → --tls args + 6380.
	tlsCR := vc.DeepCopy()
	tlsCR.Spec.TLS = &cachev1beta1.TLSSpec{Enabled: true}
	if s := jobScript(buildBootstrapJob(tlsCR, "", "c-bootstrap")); !strings.Contains(s, "--tls") || !strings.Contains(s, ":6380") {
		t.Errorf("TLS bootstrap must use --tls and port 6380\n%s", s)
	}
}

// --- ACL fanout target resolution ---

func TestACLTargets(t *testing.T) {
	r := &ValkeyACLReconciler{}

	// Replication/Standalone/Sentinel: ACL is applied to the primary only
	// (it replicates to the replicas). Empty primary → no targets yet.
	repl := minimalCR()
	repl.Name = "web"
	repl.Spec.Topology = cachev1beta1.TopologyReplication
	if got := r.aclTargets(repl, 0); got != nil {
		t.Errorf("no primary yet should yield no targets, got %v", got)
	}
	repl.Status.Primary = "web-1"
	got := r.aclTargets(repl, 0)
	if len(got) != 1 || got[0] != "web-1.web-headless.ns.svc.cluster.local" {
		t.Errorf("replication target = %v, want [web-1.web-headless.ns.svc.cluster.local]", got)
	}

	// Cluster: ACL is not gossiped, so it must be applied to every node
	// (shards*(1+replicasPerShard) pods).
	cl := minimalCR()
	cl.Name = "shd"
	cl.Spec.Topology = cachev1beta1.TopologyCluster
	cl.Spec.Shards = ptr.To[int32](3)
	cl.Spec.ReplicasPerShard = ptr.To[int32](1) // total = 6
	if got := r.aclTargets(cl, 0); len(got) != 6 {
		t.Errorf("cluster fanout should hit 6 nodes, got %d: %v", len(got), got)
	}
}

// --- NetworkPolicy ---

func TestBuildNetworkPolicy(t *testing.T) {
	// Default: ingress on the Valkey port, same-namespace + self peers.
	np := buildNetworkPolicy(minimalCR())
	if len(np.Spec.Ingress) != 1 || len(np.Spec.Ingress[0].Ports) != 1 {
		t.Fatalf("default NP should have one ingress rule with one port: %+v", np.Spec)
	}
	if np.Spec.Ingress[0].Ports[0].Port.IntValue() != int(valkeyPort) {
		t.Errorf("default NP port = %v, want %d", np.Spec.Ingress[0].Ports[0].Port, valkeyPort)
	}
	if len(np.Spec.Ingress[0].From) < 2 {
		t.Errorf("default NP must allow same-ns + valkey-to-valkey peers, got %d", len(np.Spec.Ingress[0].From))
	}

	// Metrics enabled → exporter port also allowed.
	m := minimalCR()
	m.Spec.Metrics = &cachev1beta1.MetricsSpec{Enabled: true}
	if ports := buildNetworkPolicy(m).Spec.Ingress[0].Ports; len(ports) != 2 {
		t.Errorf("metrics NP should expose 2 ports (valkey + exporter), got %d", len(ports))
	}

	// TLS → ingress on the TLS port.
	t9 := minimalCR()
	t9.Spec.TLS = &cachev1beta1.TLSSpec{Enabled: true}
	if p := buildNetworkPolicy(t9).Spec.Ingress[0].Ports[0].Port.IntValue(); p != int(valkeyTLSPort) {
		t.Errorf("TLS NP port = %d, want %d", p, valkeyTLSPort)
	}
}
