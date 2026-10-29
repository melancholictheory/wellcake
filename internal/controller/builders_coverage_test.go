/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"context"
	"strconv"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

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

// With TLS, renderSentinelConf moves Sentinel to tls-port and sets `port 0`.
// The container port, both probes and the Service must follow it: nothing
// listens on the plain port, so probes there never pass (the pod never gets
// Ready and the liveness probe keeps restarting it) and the Service would send
// clients to a closed port.
func TestSentinelServesOnTLSPort(t *testing.T) {
	for _, tlsOn := range []bool{false, true} {
		vc := sentinelCR()
		vc.Spec.TLS = &cachev1beta1.TLSSpec{Enabled: tlsOn}
		want := int32(26379)
		if tlsOn {
			want = 26380
			if c := renderSentinelConf(vc, ""); !strings.Contains(c, "tls-port 26380\n") || !strings.Contains(c, "port 0\n") {
				t.Fatalf("TLS sentinel.conf must serve on tls-port 26380 only\n%s", c)
			}
		}

		c := buildSentinelStatefulSet(vc, false).Spec.Template.Spec.Containers[0]
		if len(c.Ports) != 1 || c.Ports[0].Name != componentSentinel || c.Ports[0].ContainerPort != want {
			t.Errorf("tls=%v: sentinel container ports = %+v, want %s:%d", tlsOn, c.Ports, componentSentinel, want)
		}
		// A named port resolves against the Sentinel container's own ports.
		resolve := func(p intstr.IntOrString) int32 {
			if p.Type == intstr.Int {
				return p.IntVal
			}
			for _, cp := range c.Ports {
				if cp.Name == p.StrVal {
					return cp.ContainerPort
				}
			}
			return 0
		}
		for name, p := range map[string]*corev1.Probe{"readiness": c.ReadinessProbe, "liveness": c.LivenessProbe} {
			if p == nil {
				t.Errorf("tls=%v: %s probe is missing", tlsOn, name)
				continue
			}
			if tlsOn {
				if p.Exec == nil || !strings.Contains(strings.Join(p.Exec.Command, " "), "valkey-cli -p "+strconv.Itoa(int(want))+" --tls --insecure") {
					t.Errorf("tls=%v: %s probe must PING over TLS on %d, got %+v", tlsOn, name, want, p)
				}
			} else if p.TCPSocket == nil || resolve(p.TCPSocket.Port) != want {
				t.Errorf("tls=%v: %s probe must dial %d, got %+v", tlsOn, name, want, p)
			}
		}

		scheme := newTestScheme(t)
		cl := fake.NewClientBuilder().WithScheme(scheme).Build()
		r := &ValkeyClusterReconciler{Client: cl, Scheme: scheme}
		if err := r.ensureSentinelService(context.Background(), vc); err != nil {
			t.Fatalf("ensureSentinelService: %v", err)
		}
		var svc corev1.Service
		if err := cl.Get(context.Background(), client.ObjectKey{Namespace: vc.Namespace, Name: sentinelStatefulSetName(vc)}, &svc); err != nil {
			t.Fatalf("get sentinel Service: %v", err)
		}
		if p := svc.Spec.Ports; len(p) != 1 || p[0].Port != want || resolve(p[0].TargetPort) != want {
			t.Errorf("tls=%v: sentinel Service ports = %+v, want port and targetPort %d", tlsOn, p, want)
		}
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

	// metricsFrom → the exporter port moves to its own rule, open to those peers only.
	monitoring := networkingv1.NetworkPolicyPeer{NamespaceSelector: &metav1.LabelSelector{
		MatchLabels: map[string]string{"kubernetes.io/metadata.name": "monitoring"},
	}}
	mf := minimalCR()
	mf.Spec.Metrics = &cachev1beta1.MetricsSpec{Enabled: true}
	mf.Spec.NetworkPolicy = &cachev1beta1.NetworkPolicySpec{
		Enabled:     true,
		MetricsFrom: []networkingv1.NetworkPolicyPeer{monitoring},
	}
	ing := buildNetworkPolicy(mf).Spec.Ingress
	if len(ing) != 2 {
		t.Fatalf("metricsFrom NP should have 2 ingress rules, got %d", len(ing))
	}
	for _, p := range ing[0].Ports {
		if p.Port.IntValue() == int(exporterPort) {
			t.Errorf("metricsFrom NP: exporter port must not be in the allowFrom rule: %+v", ing[0].Ports)
		}
	}
	if len(ing[1].Ports) != 1 || ing[1].Ports[0].Port.IntValue() != int(exporterPort) {
		t.Errorf("metricsFrom NP: second rule should only open the exporter port, got %+v", ing[1].Ports)
	}
	if !equality.Semantic.DeepEqual(ing[1].From, []networkingv1.NetworkPolicyPeer{monitoring}) {
		t.Errorf("metricsFrom NP: second rule peers = %+v, want only metricsFrom", ing[1].From)
	}

	// metricsFrom without metrics → no exporter rule at all.
	mf.Spec.Metrics = nil
	if ing := buildNetworkPolicy(mf).Spec.Ingress; len(ing) != 1 || len(ing[0].Ports) != 1 {
		t.Errorf("metricsFrom without metrics should keep one rule with the Valkey port only, got %+v", ing)
	}

	// Cluster → a second rule opens the bus port to the cluster's own pods
	// only. Without it, CLUSTER MEET and gossip are refused and the cluster
	// never forms.
	cb := minimalCR()
	cb.Spec.Topology = cachev1beta1.TopologyCluster
	cb.Spec.NetworkPolicy = &cachev1beta1.NetworkPolicySpec{Enabled: true}
	ing = buildNetworkPolicy(cb).Spec.Ingress
	if len(ing) != 2 {
		t.Fatalf("Cluster NP should have 2 ingress rules, got %d", len(ing))
	}
	for _, p := range ing[0].Ports {
		if p.Port.IntValue() == int(clusterBusPort) {
			t.Errorf("Cluster NP: bus port must not be open to the client peers: %+v", ing[0].Ports)
		}
	}
	self := []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{MatchLabels: labelsFor(cb)}}}
	if len(ing[1].Ports) != 1 || ing[1].Ports[0].Port.IntValue() != int(clusterBusPort) || !equality.Semantic.DeepEqual(ing[1].From, self) {
		t.Errorf("Cluster NP: second rule should open only %d to the cluster's pods, got %+v", clusterBusPort, ing[1])
	}

	// TLS → ingress on the TLS port.
	t9 := minimalCR()
	t9.Spec.TLS = &cachev1beta1.TLSSpec{Enabled: true}
	if p := buildNetworkPolicy(t9).Spec.Ingress[0].Ports[0].Port.IntValue(); p != int(valkeyTLSPort) {
		t.Errorf("TLS NP port = %d, want %d", p, valkeyTLSPort)
	}
}

// npRule is an ingress or egress rule: its peers and its ports.
type npRule struct {
	peers []networkingv1.NetworkPolicyPeer
	ports []networkingv1.NetworkPolicyPort
}

func ingressRules(np *networkingv1.NetworkPolicy) []npRule {
	out := make([]npRule, 0, len(np.Spec.Ingress))
	for _, r := range np.Spec.Ingress {
		out = append(out, npRule{r.From, r.Ports})
	}
	return out
}

func egressRules(np *networkingv1.NetworkPolicy) []npRule {
	out := make([]npRule, 0, len(np.Spec.Egress))
	for _, r := range np.Spec.Egress {
		out = append(out, npRule{r.To, r.Ports})
	}
	return out
}

func selectorMatches(sel *metav1.LabelSelector, set labels.Set) bool {
	s, err := metav1.LabelSelectorAsSelector(sel)
	return err == nil && s.Matches(set)
}

// npAllows reports whether one of the rules lets a same-namespace pod with
// these labels through on that port.
func npAllows(rules []npRule, set labels.Set, port int32) bool {
	for _, r := range rules {
		peerOK := false
		for _, peer := range r.peers {
			if peer.NamespaceSelector == nil && peer.IPBlock == nil && peer.PodSelector != nil && selectorMatches(peer.PodSelector, set) {
				peerOK = true
			}
		}
		if !peerOK {
			continue
		}
		for _, p := range r.ports {
			if p.Port.IntValue() == int(port) {
				return true
			}
		}
	}
	return false
}

func TestNetworkPolicyEgress(t *testing.T) {
	// Egress unset → ingress only.
	if np := buildNetworkPolicy(minimalCR()); !equality.Semantic.DeepEqual(np.Spec.PolicyTypes, []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}) || np.Spec.Egress != nil {
		t.Errorf("default NP must be ingress only, got %v / %+v", np.Spec.PolicyTypes, np.Spec.Egress)
	}

	vc := sentinelCR()
	vc.Spec.NetworkPolicy = &cachev1beta1.NetworkPolicySpec{
		Enabled: true,
		Egress:  &cachev1beta1.NetworkPolicyEgressSpec{Enabled: true},
	}
	np := buildNetworkPolicy(vc)
	if !equality.Semantic.DeepEqual(np.Spec.PolicyTypes, []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress}) {
		t.Fatalf("egress NP policy types = %v", np.Spec.PolicyTypes)
	}
	if len(np.Spec.Egress) != 2 {
		t.Fatalf("egress NP should have a DNS and a peer rule, got %+v", np.Spec.Egress)
	}

	// Default DNS: kube-dns in kube-system, UDP and TCP 53.
	dns := np.Spec.Egress[0]
	if len(dns.To) != 1 || dns.To[0].NamespaceSelector == nil || dns.To[0].PodSelector == nil ||
		dns.To[0].NamespaceSelector.MatchLabels[corev1.LabelMetadataName] != "kube-system" ||
		dns.To[0].PodSelector.MatchLabels["k8s-app"] != "kube-dns" {
		t.Errorf("default DNS peer should be kube-system/kube-dns, got %+v", dns.To)
	}
	if len(dns.Ports) != 2 || *dns.Ports[0].Protocol != corev1.ProtocolUDP || *dns.Ports[1].Protocol != corev1.ProtocolTCP ||
		dns.Ports[0].Port.IntValue() != 53 || dns.Ports[1].Port.IntValue() != 53 {
		t.Errorf("DNS rule should open UDP and TCP 53, got %+v", dns.Ports)
	}

	// Peer rule: the cluster's pods on peerPorts.
	self := []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{MatchLabels: labelsFor(vc)}}}
	if !equality.Semantic.DeepEqual(np.Spec.Egress[1].To, self) || !equality.Semantic.DeepEqual(np.Spec.Egress[1].Ports, peerPorts(vc)) {
		t.Errorf("egress peer rule = %+v, want the cluster's pods on %+v", np.Spec.Egress[1], peerPorts(vc))
	}

	// Custom DNS and extra rules, appended as they are; a port without a
	// protocol gets TCP, as the API server would default it.
	nodeLocal := networkingv1.NetworkPolicyPeer{IPBlock: &networkingv1.IPBlock{CIDR: "169.254.20.10/32"}}
	https := intstr.FromInt32(443)
	s3 := networkingv1.NetworkPolicyEgressRule{
		To:    []networkingv1.NetworkPolicyPeer{{IPBlock: &networkingv1.IPBlock{CIDR: "10.0.0.0/8"}}},
		Ports: []networkingv1.NetworkPolicyPort{{Port: &https}},
	}
	vc.Spec.NetworkPolicy.Egress.DNS = []networkingv1.NetworkPolicyPeer{nodeLocal}
	vc.Spec.NetworkPolicy.Egress.Extra = []networkingv1.NetworkPolicyEgressRule{s3}
	np = buildNetworkPolicy(vc)
	if len(np.Spec.Egress) != 3 {
		t.Fatalf("egress NP with extra should have 3 rules, got %+v", np.Spec.Egress)
	}
	if !equality.Semantic.DeepEqual(np.Spec.Egress[0].To, []networkingv1.NetworkPolicyPeer{nodeLocal}) {
		t.Errorf("custom DNS peers = %+v, want %+v", np.Spec.Egress[0].To, nodeLocal)
	}
	extra := np.Spec.Egress[2]
	if !equality.Semantic.DeepEqual(extra.To, s3.To) || extra.Ports[0].Port.IntValue() != 443 ||
		extra.Ports[0].Protocol == nil || *extra.Ports[0].Protocol != corev1.ProtocolTCP {
		t.Errorf("extra rule = %+v, want %+v with TCP", extra, s3)
	}
	if s3.Ports[0].Protocol != nil {
		t.Errorf("building the policy must not modify the spec's extra rules")
	}

	// Disabled egress → ingress only, whatever dns and extra hold.
	vc.Spec.NetworkPolicy.Egress.Enabled = false
	if np := buildNetworkPolicy(vc); len(np.Spec.PolicyTypes) != 1 || np.Spec.Egress != nil {
		t.Errorf("disabled egress must leave an ingress-only policy, got %v / %+v", np.Spec.PolicyTypes, np.Spec.Egress)
	}
}

// With egress on, both directions open the same Valkey-to-Valkey ports,
// whatever the topology and TLS: what a pod may send, its peers accept.
func TestNetworkPolicyPeerPortsBothWays(t *testing.T) {
	for _, topo := range []cachev1beta1.Topology{cachev1beta1.TopologyReplication, cachev1beta1.TopologySentinel, cachev1beta1.TopologyCluster} {
		for _, tlsOn := range []bool{false, true} {
			c := minimalCR()
			c.Spec.Topology = topo
			if tlsOn {
				c.Spec.TLS = &cachev1beta1.TLSSpec{Enabled: true}
			}
			c.Spec.NetworkPolicy = &cachev1beta1.NetworkPolicySpec{
				Enabled: true,
				Egress:  &cachev1beta1.NetworkPolicyEgressSpec{Enabled: true},
			}
			cnp := buildNetworkPolicy(c)
			set := labels.Set(labelsFor(c))
			for _, p := range peerPorts(c) {
				port := int32(p.Port.IntValue())
				if !npAllows(ingressRules(cnp), set, port) || !npAllows(egressRules(cnp), set, port) {
					t.Errorf("%s tls=%v: port %d must be open between the cluster's pods both ways: %+v", topo, tlsOn, port, cnp.Spec)
				}
			}
		}
	}
}

// The Cluster Jobs carry the cluster's labels, so the policy selects them:
// with egress on, a Job must still reach the nodes on the data port and the
// cluster bus, out of the Job and into the data pods.
func TestNetworkPolicyClusterJobs(t *testing.T) {
	vc := minimalCR()
	vc.Spec.Topology = cachev1beta1.TopologyCluster
	vc.Spec.Shards = ptr.To[int32](3)
	vc.Spec.ReplicasPerShard = ptr.To[int32](1)
	vc.Spec.NetworkPolicy = &cachev1beta1.NetworkPolicySpec{
		Enabled: true,
		Egress:  &cachev1beta1.NetworkPolicyEgressSpec{Enabled: true},
	}
	np := buildNetworkPolicy(vc)
	jobLabels := labels.Set(buildBootstrapJob(vc, "", "c-bootstrap").Spec.Template.Labels)
	dataLabels := labels.Set(buildStatefulSet(vc, "", false).Spec.Template.Labels)

	if !selectorMatches(&np.Spec.PodSelector, jobLabels) || !selectorMatches(&np.Spec.PodSelector, dataLabels) {
		t.Fatalf("the policy must select the bootstrap Job and the data pods")
	}
	for _, port := range []int32{valkeyPort, clusterBusPort} {
		if !npAllows(ingressRules(np), jobLabels, port) {
			t.Errorf("ingress must accept the bootstrap Job on %d: %+v", port, np.Spec.Ingress)
		}
		if !npAllows(egressRules(np), dataLabels, port) {
			t.Errorf("egress must let the bootstrap Job reach the data pods on %d: %+v", port, np.Spec.Egress)
		}
	}
}

// spec.resources only sizes the Valkey container: the Sentinel container and
// the exporter sidecar take their own, and stay without any when unset.
func TestSentinelAndExporterResources(t *testing.T) {
	vc := sentinelCR()
	vc.Spec.Resources = corev1.ResourceRequirements{
		Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")},
	}
	vc.Spec.Metrics = &cachev1beta1.MetricsSpec{Enabled: true}

	if got := buildSentinelStatefulSet(vc, false).Spec.Template.Spec.Containers[0].Resources; !equality.Semantic.DeepEqual(got, corev1.ResourceRequirements{}) {
		t.Errorf("unset spec.sentinel.resources: sentinel container resources = %v, want none", got)
	}
	if got := buildExporter(vc).Resources; !equality.Semantic.DeepEqual(got, corev1.ResourceRequirements{}) {
		t.Errorf("unset spec.metrics.resources: exporter resources = %v, want none", got)
	}

	sentinelRes := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("50m"), corev1.ResourceMemory: resource.MustParse("64Mi")},
		Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("128Mi")},
	}
	exporterRes := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("20m"), corev1.ResourceMemory: resource.MustParse("32Mi")},
		Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("64Mi")},
	}
	vc.Spec.Sentinel.Resources = sentinelRes
	vc.Spec.Metrics.Resources = exporterRes

	if got := buildSentinelStatefulSet(vc, false).Spec.Template.Spec.Containers[0].Resources; !equality.Semantic.DeepEqual(got, sentinelRes) {
		t.Errorf("sentinel container resources = %v, want %v", got, sentinelRes)
	}
	if got := buildExporter(vc).Resources; !equality.Semantic.DeepEqual(got, exporterRes) {
		t.Errorf("exporter resources = %v, want %v", got, exporterRes)
	}
}

// The Sentinel pods follow the data pods onto their nodes (node selector,
// tolerations, node affinity) and take the image pull policy, but keep the
// operator's pod anti-affinity and spread: the user's pod-level terms select
// the data pods.
func TestSentinelPodScheduling(t *testing.T) {
	vc := sentinelCR()
	vc.Spec.ImagePullPolicy = corev1.PullAlways
	vc.Spec.NodeSelector = map[string]string{"pool": "valkey"}
	vc.Spec.Tolerations = []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "valkey", Effect: corev1.TaintEffectNoSchedule}}
	nodeAffinity := &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{
				{Key: "topology.kubernetes.io/zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"a", "b", "c"}},
			}}},
		},
	}
	vc.Spec.Affinity = &corev1.Affinity{
		NodeAffinity: nodeAffinity,
		PodAntiAffinity: &corev1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{TopologyKey: topologyKeyHostname}},
		},
	}
	vc.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{MaxSkew: 1, TopologyKey: topologyKeyHostname}}

	pod := buildSentinelStatefulSet(vc, false).Spec.Template.Spec
	if !equality.Semantic.DeepEqual(pod.NodeSelector, vc.Spec.NodeSelector) {
		t.Errorf("sentinel nodeSelector = %v, want %v", pod.NodeSelector, vc.Spec.NodeSelector)
	}
	if !equality.Semantic.DeepEqual(pod.Tolerations, vc.Spec.Tolerations) {
		t.Errorf("sentinel tolerations = %v, want %v", pod.Tolerations, vc.Spec.Tolerations)
	}
	if pod.Affinity == nil || !equality.Semantic.DeepEqual(pod.Affinity.NodeAffinity, nodeAffinity) {
		t.Errorf("sentinel node affinity = %v, want %v", pod.Affinity, nodeAffinity)
	}
	if !equality.Semantic.DeepEqual(pod.Affinity.PodAntiAffinity, defaultAntiAffinity(vc).PodAntiAffinity) {
		t.Errorf("sentinel pod anti-affinity = %v, want the operator default", pod.Affinity.PodAntiAffinity)
	}
	if !equality.Semantic.DeepEqual(pod.TopologySpreadConstraints, defaultTopologySpread(vc)) {
		t.Errorf("sentinel spread = %v, want the operator default", pod.TopologySpreadConstraints)
	}
	for _, c := range append(pod.InitContainers, pod.Containers...) {
		if c.ImagePullPolicy != corev1.PullAlways {
			t.Errorf("%s imagePullPolicy = %q, want Always", c.Name, c.ImagePullPolicy)
		}
	}

	// Without spec.affinity the Sentinels keep just the default anti-affinity.
	vc.Spec.Affinity = nil
	if a := buildSentinelStatefulSet(vc, false).Spec.Template.Spec.Affinity; a.NodeAffinity != nil {
		t.Errorf("sentinel node affinity without spec.affinity = %v, want none", a.NodeAffinity)
	}
}
