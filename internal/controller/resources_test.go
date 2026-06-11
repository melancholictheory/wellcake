/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// minimalCR returns a CR pre-populated with the same defaults the API
// machinery would apply, so test cases focus on what they're actually
// changing.
func minimalCR() *cachev1beta1.ValkeyCluster {
	return &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology:        cachev1beta1.TopologyReplication,
			Profile:         cachev1beta1.ProfileCache,
			Image:           "valkey/valkey:8.0",
			ImagePullPolicy: corev1.PullIfNotPresent,
			Replicas:        3,
		},
	}
}

func TestApplyDefaults(t *testing.T) {
	tests := []struct {
		name          string
		in            *cachev1beta1.ValkeyCluster
		wantTopology  cachev1beta1.Topology
		wantProfile   cachev1beta1.Profile
		wantReplicas  int32
		wantStorageOK bool
	}{
		{
			name:          "empty fills replication+cache",
			in:            &cachev1beta1.ValkeyCluster{},
			wantTopology:  cachev1beta1.TopologyReplication,
			wantProfile:   cachev1beta1.ProfileCache,
			wantReplicas:  3,
			wantStorageOK: false, // Cache profile doesn't auto-fill Storage
		},
		{
			name: "standalone defaults to 1 replica",
			in: &cachev1beta1.ValkeyCluster{
				Spec: cachev1beta1.ValkeyClusterSpec{Topology: cachev1beta1.TopologyStandalone},
			},
			wantTopology: cachev1beta1.TopologyStandalone,
			wantProfile:  cachev1beta1.ProfileCache,
			wantReplicas: 1,
		},
		{
			name: "durable fills Storage with both mode",
			in: &cachev1beta1.ValkeyCluster{
				Spec: cachev1beta1.ValkeyClusterSpec{Profile: cachev1beta1.ProfileDurable},
			},
			wantTopology:  cachev1beta1.TopologyReplication,
			wantProfile:   cachev1beta1.ProfileDurable,
			wantReplicas:  3,
			wantStorageOK: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.in.Default()
			if tc.in.Spec.Topology != tc.wantTopology {
				t.Errorf("Topology = %q, want %q", tc.in.Spec.Topology, tc.wantTopology)
			}
			if tc.in.Spec.Profile != tc.wantProfile {
				t.Errorf("Profile = %q, want %q", tc.in.Spec.Profile, tc.wantProfile)
			}
			if tc.in.Spec.Replicas != tc.wantReplicas {
				t.Errorf("Replicas = %d, want %d", tc.in.Spec.Replicas, tc.wantReplicas)
			}
			if (tc.in.Spec.Storage != nil) != tc.wantStorageOK {
				t.Errorf("Storage set = %v, want %v", tc.in.Spec.Storage != nil, tc.wantStorageOK)
			}
		})
	}
}

func TestTotalReplicas(t *testing.T) {
	tests := []struct {
		name string
		in   *cachev1beta1.ValkeyCluster
		want int32
	}{
		{
			name: "replication uses spec.replicas",
			in: &cachev1beta1.ValkeyCluster{Spec: cachev1beta1.ValkeyClusterSpec{
				Topology: cachev1beta1.TopologyReplication, Replicas: 5,
			}},
			want: 5,
		},
		{
			name: "cluster multiplies shards by 1+replicasPerShard",
			in: &cachev1beta1.ValkeyCluster{Spec: cachev1beta1.ValkeyClusterSpec{
				Topology:         cachev1beta1.TopologyCluster,
				Shards:           ptr.To[int32](3),
				ReplicasPerShard: ptr.To[int32](2),
			}},
			want: 9,
		},
		{
			name: "cluster with zero replicas per shard equals shards",
			in: &cachev1beta1.ValkeyCluster{Spec: cachev1beta1.ValkeyClusterSpec{
				Topology: cachev1beta1.TopologyCluster,
				Shards:   ptr.To[int32](6),
			}},
			want: 6,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := totalReplicas(tc.in); got != tc.want {
				t.Errorf("totalReplicas = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestStatefulSetReplicasHoldsDuringScaleDown(t *testing.T) {
	// Cluster scaling down: spec wants 3 shards, cluster currently has 6 pods.
	// The StatefulSet must stay at 6 until runClusterScaleDown reshards.
	vc := &cachev1beta1.ValkeyCluster{
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology: cachev1beta1.TopologyCluster,
			Shards:   ptr.To[int32](3),
		},
		Status: cachev1beta1.ValkeyClusterStatus{LastAppliedReplicas: 6},
	}
	if got := statefulSetReplicas(vc); got != 6 {
		t.Errorf("statefulSetReplicas during scale-down = %d, want 6 (hold)", got)
	}

	// After the scale-down Job advances LastAppliedReplicas down to 3,
	// statefulSetReplicas should return the new spec value.
	vc.Status.LastAppliedReplicas = 3
	if got := statefulSetReplicas(vc); got != 3 {
		t.Errorf("statefulSetReplicas after scale-down = %d, want 3", got)
	}

	// Replication topology: never holds.
	repl := &cachev1beta1.ValkeyCluster{
		Spec:   cachev1beta1.ValkeyClusterSpec{Topology: cachev1beta1.TopologyReplication, Replicas: 3},
		Status: cachev1beta1.ValkeyClusterStatus{LastAppliedReplicas: 9},
	}
	if got := statefulSetReplicas(repl); got != 3 {
		t.Errorf("statefulSetReplicas Replication = %d, want 3", got)
	}
}

func TestComputeMaxmemory(t *testing.T) {
	tests := []struct {
		name  string
		limit string
		want  string
	}{
		{"no limit", "", ""},
		{"1Gi → 60%", "1Gi", "644245094b"},    // 1073741824 * 60 / 100
		{"2Gi → 60%", "2Gi", "1288490188b"},   // 2147483648 * 60 / 100
		{"100Mi → 60%", "100Mi", "62914560b"}, // 104857600 * 60 / 100
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			vc := minimalCR()
			if tc.limit != "" {
				vc.Spec.Resources.Limits = corev1.ResourceList{
					corev1.ResourceMemory: resource.MustParse(tc.limit),
				}
			}
			if got := computeMaxmemory(vc); got != tc.want {
				t.Errorf("computeMaxmemory(%q) = %q, want %q", tc.limit, got, tc.want)
			}
		})
	}
}

func TestPersistenceMode(t *testing.T) {
	tests := []struct {
		name    string
		profile cachev1beta1.Profile
		storage *cachev1beta1.StorageSpec
		want    string
	}{
		{"cache, no storage → none", cachev1beta1.ProfileCache, nil, "none"},
		{"durable, no storage → both", cachev1beta1.ProfileDurable, nil, "both"},
		{"explicit aof wins over profile", cachev1beta1.ProfileCache, &cachev1beta1.StorageSpec{Mode: "aof"}, "aof"},
		{"explicit none wins over profile", cachev1beta1.ProfileDurable, &cachev1beta1.StorageSpec{Mode: "none"}, "none"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			vc := minimalCR()
			vc.Spec.Profile = tc.profile
			vc.Spec.Storage = tc.storage
			if got := persistenceMode(vc); got != tc.want {
				t.Errorf("persistenceMode = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestConfigHashFromDataIsDeterministicAndSensitive(t *testing.T) {
	a := map[string]string{"valkey.conf": "port 6379\nmaxmemory 1g\n"}
	b := map[string]string{"valkey.conf": "port 6379\nmaxmemory 1g\n"}
	if configHashFromData(a) != configHashFromData(b) {
		t.Errorf("identical data produced different hashes")
	}
	// Different content → different hash.
	b["valkey.conf"] = "port 6379\nmaxmemory 2g\n"
	if configHashFromData(a) == configHashFromData(b) {
		t.Errorf("changed content produced the same hash")
	}
	// Key reordering does not matter — hash is over sorted keys.
	c := map[string]string{"b": "x", "a": "y"}
	d := map[string]string{"a": "y", "b": "x"}
	if configHashFromData(c) != configHashFromData(d) {
		t.Errorf("key-order changed the hash; should sort")
	}
}

func TestValkeyConfigArgQuotesSpecialCharacters(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{name: "plain", value: "secret", want: `"secret"`},
		{name: "double quote", value: `abc"def`, want: `"abc\"def"`},
		{name: "space backslash hash", value: `a b\c#d`, want: `"a b\\c#d"`},
		{name: "leading hash", value: `#comment`, want: `"#comment"`},
		{name: "newline", value: "line\nnext", want: `"line\nnext"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := valkeyConfigArg(tc.value); got != tc.want {
				t.Errorf("valkeyConfigArg(%q) = %q, want %q", tc.value, got, tc.want)
			}
		})
	}
}

func TestRenderValkeyConfHasProfileDefaults(t *testing.T) {
	tests := []struct {
		name           string
		profile        cachev1beta1.Profile
		wantPolicy     string
		wantPersistOff bool
		wantAOF        bool
	}{
		{"cache → allkeys-lru, no AOF", cachev1beta1.ProfileCache, "allkeys-lru", true, false},
		{"durable → noeviction, AOF on", cachev1beta1.ProfileDurable, "noeviction", false, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			vc := minimalCR()
			vc.Spec.Profile = tc.profile
			conf := renderValkeyConf(vc, "")
			if !strings.Contains(conf, "maxmemory-policy "+tc.wantPolicy) {
				t.Errorf("missing maxmemory-policy %s\n%s", tc.wantPolicy, conf)
			}
			if tc.wantAOF && !strings.Contains(conf, "appendonly yes") {
				t.Errorf("missing appendonly yes for durable")
			}
			if tc.wantPersistOff && !strings.Contains(conf, "appendonly no") {
				t.Errorf("missing appendonly no for cache")
			}
			// aclfile must always be present.
			if !strings.Contains(conf, "aclfile "+dataMountPath+"/users.acl") {
				t.Errorf("missing aclfile directive")
			}
		})
	}
}

func TestRenderInitScriptSeedsDefaultUserACL(t *testing.T) {
	// With auth on, the init script must seed users.acl with a passworded
	// default user — otherwise an empty aclfile resets default to nopass and
	// silently overrides requirepass.
	vc := minimalCR()
	vc.Spec.Auth = &cachev1beta1.AuthSpec{Enabled: true}
	script := renderInitScript(vc)

	for _, want := range []string{
		"users.acl",
		`printf '%s' "$VALKEY_PASSWORD"`,           // hash is computed from the exact Secret value
		"user default on #$PW_HASH ~* &* +@all",    // seeded default user carries the password hash
		"[ ! -s " + dataMountPath + "/users.acl ]", // only seed when empty (don't clobber ACL SAVE)
	} {
		if !strings.Contains(script, want) {
			t.Errorf("init script missing %q\n%s", want, script)
		}
	}
	if strings.Contains(script, "sed -n 's/^requirepass //p'") {
		t.Errorf("init script must not parse the escaped config password back out of runtime.conf\n%s", script)
	}
	// It must NOT blindly create an empty file in the auth case.
	if strings.Contains(script, "touch "+dataMountPath+"/users.acl") {
		t.Errorf("init script still touches an empty users.acl (the bug)\n%s", script)
	}
	// Replication topology does NOT seed the sentinel ACL user.
	if strings.Contains(script, "user sentinel-user") {
		t.Errorf("non-Sentinel topology must not seed the sentinel ACL user\n%s", script)
	}
}

func TestRenderInitScriptSeedsSentinelACLUser(t *testing.T) {
	// Sentinel topology with auth seeds a dedicated sentinel-user (all commands,
	// all channels, NO key glob → no data access) that Sentinel uses to reach
	// the master via `sentinel auth-user`.
	vc := minimalCR()
	vc.Spec.Topology = cachev1beta1.TopologySentinel
	vc.Spec.Sentinel = &cachev1beta1.SentinelSpec{Replicas: 3}
	vc.Spec.Auth = &cachev1beta1.AuthSpec{Enabled: true}
	script := renderInitScript(vc)

	if !strings.Contains(script, "user sentinel-user on #$PW_HASH &* +@all") {
		t.Errorf("Sentinel init script must seed the sentinel ACL user\n%s", script)
	}
	// No key glob (~) for the sentinel user — it must not read/write data.
	if strings.Contains(script, "user sentinel-user on #$PW_HASH ~* &* +@all") {
		t.Errorf("sentinel-user must not have key access (~*)\n%s", script)
	}
}

func TestRenderInitScriptNoPasswordLeavesACLEmpty(t *testing.T) {
	// Auth disabled: no requirepass, so the seed branch falls through to an
	// empty file and the default user stays nopass (intended).
	vc := minimalCR()
	vc.Spec.Auth = nil
	script := renderInitScript(vc)
	if !strings.Contains(script, ": > "+dataMountPath+"/users.acl") {
		t.Errorf("init script should fall back to an empty users.acl when no password\n%s", script)
	}
}

func TestRenderValkeyConfMutualTLS(t *testing.T) {
	vc := minimalCR()
	vc.Spec.TLS = &cachev1beta1.TLSSpec{Enabled: true}
	if got := renderValkeyConf(vc, ""); !strings.Contains(got, "tls-auth-clients optional") {
		t.Errorf("TLS without MutualTLS should keep client auth optional\n%s", got)
	}
	vc.Spec.TLS.MutualTLS = true
	if got := renderValkeyConf(vc, ""); !strings.Contains(got, "tls-auth-clients yes") {
		t.Errorf("MutualTLS=true must enforce client auth (tls-auth-clients yes)\n%s", got)
	}
}

func TestRenderValkeyConfEscapesPasswordArguments(t *testing.T) {
	vc := minimalCR()
	password := `abc" def\#ghi`
	conf := renderValkeyConf(vc, password)
	quoted := valkeyConfigArg(password)
	for _, want := range []string{
		"requirepass " + quoted,
		"masterauth " + quoted,
	} {
		if !strings.Contains(conf, want) {
			t.Errorf("missing escaped password directive %q\n%s", want, conf)
		}
	}
	if strings.Contains(conf, `requirepass abc"`) {
		t.Errorf("password must not be rendered as an unquoted config argument\n%s", conf)
	}
}

func TestInternalEndpoint(t *testing.T) {
	vc := minimalCR()
	vc.Name = "web"
	vc.Namespace = "apps"
	if got := internalEndpoint(vc); got != "web.apps.svc:6379" {
		t.Errorf("internalEndpoint = %q, want web.apps.svc:6379", got)
	}
	vc.Spec.TLS = &cachev1beta1.TLSSpec{Enabled: true}
	if got := internalEndpoint(vc); got != "web.apps.svc:6380" {
		t.Errorf("TLS internalEndpoint = %q, want web.apps.svc:6380", got)
	}
}

func TestRenderValkeyConfClusterDirectives(t *testing.T) {
	vc := minimalCR()
	vc.Spec.Topology = cachev1beta1.TopologyCluster
	vc.Spec.Shards = ptr.To[int32](3)
	vc.Spec.Profile = cachev1beta1.ProfileDurable

	conf := renderValkeyConf(vc, "secret")
	for _, want := range []string{
		"cluster-enabled yes",
		"cluster-config-file " + dataMountPath + "/nodes.conf",
		"cluster-node-timeout 5000",
		"cluster-require-full-coverage yes", // Durable requires it
		`requirepass "secret"`,
		`masterauth "secret"`,
	} {
		if !strings.Contains(conf, want) {
			t.Errorf("missing %q\n%s", want, conf)
		}
	}
}

func TestRenderValkeyConfReplBacklog(t *testing.T) {
	withLimit := func(topo cachev1beta1.Topology, lim string) *cachev1beta1.ValkeyCluster {
		vc := minimalCR()
		vc.Spec.Topology = topo
		vc.Spec.Resources.Limits = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse(lim)}
		return vc
	}

	// Replication with a small limit clamps to the 16MB floor.
	if conf := renderValkeyConf(withLimit(cachev1beta1.TopologyReplication, "64Mi"), ""); !strings.Contains(conf, "repl-backlog-size 16777216b") {
		t.Errorf("64Mi limit should clamp backlog to 16MB floor\n%s", conf)
	}
	// A large limit clamps to the 256MB ceiling.
	if conf := renderValkeyConf(withLimit(cachev1beta1.TopologyReplication, "32Gi"), ""); !strings.Contains(conf, "repl-backlog-size 268435456b") {
		t.Errorf("32Gi limit should clamp backlog to 256MB ceiling\n%s", conf)
	}
	// Mid-range scales to ~1/16 of the limit (2Gi → 128MB).
	if conf := renderValkeyConf(withLimit(cachev1beta1.TopologyReplication, "2Gi"), ""); !strings.Contains(conf, "repl-backlog-size 134217728b") {
		t.Errorf("2Gi limit should size backlog to 128MB\n%s", conf)
	}
	// Standalone never has replicas → no backlog line.
	if conf := renderValkeyConf(withLimit(cachev1beta1.TopologyStandalone, "2Gi"), ""); strings.Contains(conf, "repl-backlog-size") {
		t.Errorf("Standalone must not set repl-backlog-size\n%s", conf)
	}
	// Explicit user config wins over the computed default.
	vc := withLimit(cachev1beta1.TopologyReplication, "2Gi")
	vc.Spec.Config = map[string]string{"repl-backlog-size": "512mb"}
	conf := renderValkeyConf(vc, "")
	if !strings.Contains(conf, "repl-backlog-size 512mb") || strings.Contains(conf, "repl-backlog-size 134217728b") {
		t.Errorf("user repl-backlog-size override must win\n%s", conf)
	}
}

func TestBuildExporterTLS(t *testing.T) {
	envOf := func(c corev1.Container) map[string]string {
		m := map[string]string{}
		for _, e := range c.Env {
			m[e.Name] = e.Value
		}
		return m
	}

	// Plaintext: redis:// on 6379, no skip-verify.
	plainCR := minimalCR()
	plainCR.Spec.Metrics = &cachev1beta1.MetricsSpec{Enabled: true}
	plain := envOf(buildExporter(plainCR))
	if plain["REDIS_ADDR"] != "redis://localhost:6379" {
		t.Errorf("plaintext REDIS_ADDR = %q", plain["REDIS_ADDR"])
	}
	if _, ok := plain["REDIS_EXPORTER_SKIP_TLS_VERIFICATION"]; ok {
		t.Errorf("skip-verify must not be set without TLS")
	}

	// TLS: rediss:// on 6380, skip-verify on (self-signed CA, localhost).
	vc := minimalCR()
	vc.Spec.Metrics = &cachev1beta1.MetricsSpec{Enabled: true}
	vc.Spec.TLS = &cachev1beta1.TLSSpec{Enabled: true}
	tls := envOf(buildExporter(vc))
	if tls["REDIS_ADDR"] != "rediss://localhost:6380" {
		t.Errorf("TLS REDIS_ADDR = %q, want rediss://localhost:6380", tls["REDIS_ADDR"])
	}
	if tls["REDIS_EXPORTER_SKIP_TLS_VERIFICATION"] != "true" {
		t.Errorf("TLS exporter must set REDIS_EXPORTER_SKIP_TLS_VERIFICATION=true; got %q", tls["REDIS_EXPORTER_SKIP_TLS_VERIFICATION"])
	}
}

func TestBuildExporterAuthSecret(t *testing.T) {
	passwordSecretRef := func(c corev1.Container) string {
		for _, e := range c.Env {
			if e.Name == "REDIS_PASSWORD" && e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil {
				return e.ValueFrom.SecretKeyRef.Name
			}
		}
		return ""
	}

	// Operator-managed auth → generated <name>-auth Secret.
	gen := minimalCR()
	gen.Spec.Metrics = &cachev1beta1.MetricsSpec{Enabled: true}
	gen.Spec.Auth = &cachev1beta1.AuthSpec{Enabled: true}
	if got := passwordSecretRef(buildExporter(gen)); got != "test-auth" {
		t.Errorf("exporter password secret = %q, want test-auth", got)
	}

	// User-supplied existingSecret → exporter must reference it, not <name>-auth.
	// Otherwise the sidecar can't start (secret not found) and the pod never
	// becomes Ready — the sec-01-tls-auth regression.
	ext := minimalCR()
	ext.Spec.Metrics = &cachev1beta1.MetricsSpec{Enabled: true}
	ext.Spec.Auth = &cachev1beta1.AuthSpec{Enabled: true, ExistingSecret: "my-auth"}
	if got := passwordSecretRef(buildExporter(ext)); got != "my-auth" {
		t.Errorf("exporter password secret = %q, want my-auth (existingSecret)", got)
	}
}

func TestBuildStatefulSetConfigInitGetsAuthPasswordEnv(t *testing.T) {
	passwordSecretRef := func(vc *cachev1beta1.ValkeyCluster) string {
		sts := buildStatefulSet(vc, "h", false)
		for _, e := range sts.Spec.Template.Spec.InitContainers[0].Env {
			if e.Name == envValkeyPassword && e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil {
				return e.ValueFrom.SecretKeyRef.Name
			}
		}
		return ""
	}

	gen := minimalCR()
	gen.Spec.Auth = &cachev1beta1.AuthSpec{Enabled: true}
	if got := passwordSecretRef(gen); got != "test-auth" {
		t.Errorf("config-init password secret = %q, want generated test-auth", got)
	}

	ext := minimalCR()
	ext.Spec.Auth = &cachev1beta1.AuthSpec{Enabled: true, ExistingSecret: "my-auth"}
	if got := passwordSecretRef(ext); got != "my-auth" {
		t.Errorf("config-init password secret = %q, want existingSecret my-auth", got)
	}

	disabled := minimalCR()
	if got := passwordSecretRef(disabled); got != "" {
		t.Errorf("config-init should not get VALKEY_PASSWORD when auth is disabled, got secret %q", got)
	}
}

func TestBuildHeadlessServiceHasGossipPortOnlyForCluster(t *testing.T) {
	vc := minimalCR()
	svc := buildHeadlessService(vc)
	if hasPort(svc.Spec.Ports, "gossip") {
		t.Errorf("Replication headless should not expose gossip")
	}

	vc.Spec.Topology = cachev1beta1.TopologyCluster
	svc = buildHeadlessService(vc)
	if !hasPort(svc.Spec.Ports, "gossip") {
		t.Errorf("Cluster headless must expose gossip port")
	}
}

func hasPort(ports []corev1.ServicePort, name string) bool {
	for _, p := range ports {
		if p.Name == name {
			return true
		}
	}
	return false
}

func TestBuildStatefulSetParallelPodManagement(t *testing.T) {
	// Parallel avoids OrderedReady blocking the whole set on a single Pending pod.
	for _, topo := range []cachev1beta1.Topology{
		cachev1beta1.TopologyReplication, cachev1beta1.TopologyCluster, cachev1beta1.TopologySentinel,
	} {
		vc := minimalCR()
		vc.Spec.Topology = topo
		if topo == cachev1beta1.TopologyCluster {
			vc.Spec.Shards = ptr.To[int32](3)
		}
		sts := buildStatefulSet(vc, "h", false)
		if sts.Spec.PodManagementPolicy != appsv1.ParallelPodManagement {
			t.Errorf("topology %s: PodManagementPolicy = %q, want Parallel", topo, sts.Spec.PodManagementPolicy)
		}
	}
}

func TestBuildStatefulSetMemoryStorage(t *testing.T) {
	// storage.medium=Memory → no PVC template; data dir is a tmpfs emptyDir.
	vc := minimalCR()
	vc.Spec.Profile = cachev1beta1.ProfileCache
	vc.Spec.Storage = &cachev1beta1.StorageSpec{Medium: "Memory"}
	sts := buildStatefulSet(vc, "h", false)

	if len(sts.Spec.VolumeClaimTemplates) != 0 {
		t.Errorf("Memory medium must have NO volumeClaimTemplates, got %d", len(sts.Spec.VolumeClaimTemplates))
	}
	var dataVol *corev1.Volume
	for i := range sts.Spec.Template.Spec.Volumes {
		if sts.Spec.Template.Spec.Volumes[i].Name == dataVolumeName {
			dataVol = &sts.Spec.Template.Spec.Volumes[i]
		}
	}
	if dataVol == nil {
		t.Fatalf("expected a %q emptyDir volume in the pod template", dataVolumeName)
	}
	if dataVol.EmptyDir == nil || dataVol.EmptyDir.Medium != corev1.StorageMediumMemory {
		t.Errorf("data volume must be a tmpfs (medium=Memory) emptyDir, got %+v", dataVol.VolumeSource)
	}
	if dataVol.EmptyDir.SizeLimit == nil {
		t.Errorf("tmpfs data volume should have a sizeLimit guard rail")
	}

	// Default (Disk) keeps the PVC template.
	vcDisk := minimalCR()
	if sts := buildStatefulSet(vcDisk, "h", false); len(sts.Spec.VolumeClaimTemplates) != 1 {
		t.Errorf("default (Disk) medium must keep 1 volumeClaimTemplate, got %d", len(sts.Spec.VolumeClaimTemplates))
	}
}

func TestBuildStatefulSetInitContainerCount(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*cachev1beta1.ValkeyCluster)
		wantCount int
	}{
		{
			name:      "default → 1 init (config-init)",
			mutate:    func(*cachev1beta1.ValkeyCluster) {},
			wantCount: 1,
		},
		{
			name: "with restoreFrom → 2 init (restore + config-init)",
			mutate: func(vc *cachev1beta1.ValkeyCluster) {
				vc.Spec.RestoreFrom = &cachev1beta1.RestoreSpec{
					SourceKey: "backups/foo.rdb",
					S3: &cachev1beta1.S3Spec{
						Bucket:            "b",
						CredentialsSecret: "c",
					},
				}
			},
			wantCount: 2,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			vc := minimalCR()
			tc.mutate(vc)
			sts := buildStatefulSet(vc, "deadbeef", false)
			got := len(sts.Spec.Template.Spec.InitContainers)
			if got != tc.wantCount {
				t.Errorf("init containers = %d, want %d", got, tc.wantCount)
			}
			// configHash annotation is propagated.
			if sts.Spec.Template.Annotations[configHashAnnotation] != "deadbeef" {
				t.Errorf("missing config-hash annotation")
			}
		})
	}
}

// s4CR builds a Replication cluster that pulls from an external TLS primary
// signed by its own CA (S4 source-CA merge).
func s4CR() *cachev1beta1.ValkeyCluster {
	vc := minimalCR()
	vc.Spec.Topology = cachev1beta1.TopologyReplication
	vc.Spec.TLS = &cachev1beta1.TLSSpec{Enabled: true}
	vc.Spec.ReplicateFrom = &cachev1beta1.ReplicateFromSpec{
		Host:     "src-primary.dc2.svc.cluster.local",
		Port:     6380,
		TLS:      true,
		CASecret: &cachev1beta1.SourceCASecretRef{Name: "source-ca"},
	}
	return vc
}

func findVolume(vols []corev1.Volume, name string) *corev1.Volume {
	for i := range vols {
		if vols[i].Name == name {
			return &vols[i]
		}
	}
	return nil
}

func hasMount(mounts []corev1.VolumeMount, name string) bool {
	for i := range mounts {
		if mounts[i].Name == name {
			return true
		}
	}
	return false
}

func TestSourceCAMergeRendersCombinedBundle(t *testing.T) {
	vc := s4CR()

	// tls-ca-cert-file must point at the combined bundle on the data PVC, not the
	// cluster's own ca.crt — otherwise the outbound link to the source can't be
	// verified against the source CA.
	conf := renderValkeyConf(vc, "")
	if !strings.Contains(conf, "tls-ca-cert-file "+dataMountPath+"/ca-bundle.crt") {
		t.Errorf("tls-ca-cert-file must target the combined bundle\n%s", conf)
	}

	// The init script builds the bundle (local CA + source CA) and still wires up
	// replication + tls-replication.
	script := renderInitScript(vc)
	for _, want := range []string{
		"cat " + tlsMountPath + "/ca.crt " + sourceCAMountPath + "/ca.crt > " + dataMountPath + "/ca-bundle.crt",
		"replicaof src-primary.dc2.svc.cluster.local 6380",
		"tls-replication yes",
		"SOURCE_PASSWORD_ARG=$(printf '%s' \"$SOURCE_PASSWORD\" | valkey_config_arg)",
		"masterauth ${SOURCE_PASSWORD_ARG}",
	} {
		if !strings.Contains(script, want) {
			t.Errorf("init script missing %q\n%s", want, script)
		}
	}

	// Pod template carries the source-ca volume, and config-init mounts both the
	// local TLS Secret and the source CA so the merge can read both.
	sts := buildStatefulSet(vc, "h", false)
	vol := findVolume(sts.Spec.Template.Spec.Volumes, sourceCAVolumeName)
	if vol == nil || vol.Secret == nil {
		t.Fatalf("expected a %q secret volume", sourceCAVolumeName)
	}
	if vol.Secret.SecretName != "source-ca" {
		t.Errorf("source-ca volume secret = %q, want source-ca", vol.Secret.SecretName)
	}
	if len(vol.Secret.Items) != 1 || vol.Secret.Items[0].Key != "ca.crt" || vol.Secret.Items[0].Path != "ca.crt" {
		t.Errorf("source-ca volume must project key→ca.crt, got %+v", vol.Secret.Items)
	}
	var configInit *corev1.Container
	for i := range sts.Spec.Template.Spec.InitContainers {
		if sts.Spec.Template.Spec.InitContainers[i].Name == "config-init" {
			configInit = &sts.Spec.Template.Spec.InitContainers[i]
		}
	}
	if configInit == nil {
		t.Fatal("config-init container not found")
	}
	if !hasMount(configInit.VolumeMounts, tlsVolumeName) || !hasMount(configInit.VolumeMounts, sourceCAVolumeName) {
		t.Errorf("config-init must mount both %q and %q, got %+v", tlsVolumeName, sourceCAVolumeName, configInit.VolumeMounts)
	}
}

func TestSourceCAMergeCustomKeyProjection(t *testing.T) {
	vc := s4CR()
	vc.Spec.ReplicateFrom.CASecret.Key = "issuing-ca.pem"
	sts := buildStatefulSet(vc, "h", false)
	vol := findVolume(sts.Spec.Template.Spec.Volumes, sourceCAVolumeName)
	if vol == nil || vol.Secret == nil || len(vol.Secret.Items) != 1 {
		t.Fatalf("expected source-ca volume with one item, got %+v", vol)
	}
	// Whatever the configured key, it's projected to the fixed ca.crt path so the
	// merge command stays static.
	if vol.Secret.Items[0].Key != "issuing-ca.pem" || vol.Secret.Items[0].Path != "ca.crt" {
		t.Errorf("custom key must project issuing-ca.pem→ca.crt, got %+v", vol.Secret.Items[0])
	}
}

func TestSourceCAMergeOffWithoutCASecret(t *testing.T) {
	// replicateFrom + TLS but no caSecret (source shares the CA): no bundle, no
	// extra volume, tls-ca-cert-file stays the cluster's own ca.crt.
	vc := s4CR()
	vc.Spec.ReplicateFrom.CASecret = nil

	if sourceCAMergeEnabled(vc) {
		t.Fatal("merge must be disabled without caSecret")
	}
	if got := caCertPath(vc); got != tlsMountPath+"/ca.crt" {
		t.Errorf("caCertPath = %q, want cluster ca.crt", got)
	}
	if strings.Contains(renderInitScript(vc), "ca-bundle.crt") {
		t.Error("init script must not build a bundle without caSecret")
	}
	sts := buildStatefulSet(vc, "h", false)
	if findVolume(sts.Spec.Template.Spec.Volumes, sourceCAVolumeName) != nil {
		t.Error("no source-ca volume expected without caSecret")
	}
}

func TestSourceCAMergeRequiresLocalTLS(t *testing.T) {
	// caSecret without local TLS is rejected by CEL/webhook, but the helper must
	// be defensive: no local cert/key means no bundle (caCertPath stays default,
	// which is moot since TLS is off).
	vc := s4CR()
	vc.Spec.TLS = nil
	if sourceCAMergeEnabled(vc) {
		t.Error("merge must require local TLS")
	}
}

func TestParseClusterNodes(t *testing.T) {
	// One healthy 3-shard cluster, each primary with one replica.
	raw := `aaa11 demo-0.demo-headless.demo.svc.cluster.local:6379@16379,demo-0.demo-headless.demo.svc.cluster.local myself,master - 0 0 1 connected 0-5460
bbb22 demo-1.demo-headless.demo.svc.cluster.local:6379@16379,demo-1.demo-headless.demo.svc.cluster.local master - 0 0 2 connected 5461-10922
ccc33 demo-2.demo-headless.demo.svc.cluster.local:6379@16379,demo-2.demo-headless.demo.svc.cluster.local master - 0 0 3 connected 10923-16383
ddd44 demo-3.demo-headless.demo.svc.cluster.local:6379@16379,demo-3.demo-headless.demo.svc.cluster.local slave aaa11 0 0 1 connected
eee55 demo-4.demo-headless.demo.svc.cluster.local:6379@16379,demo-4.demo-headless.demo.svc.cluster.local slave bbb22 0 0 2 connected
fff66 demo-5.demo-headless.demo.svc.cluster.local:6379@16379,demo-5.demo-headless.demo.svc.cluster.local slave,fail ccc33 0 0 3 disconnected
`
	nodes := parseClusterNodes(raw)
	if len(nodes) != 6 {
		t.Fatalf("expected 6 nodes, got %d", len(nodes))
	}
	if nodes[0].podName != "demo-0" || !nodes[0].isMaster() {
		t.Errorf("first node should be master demo-0; got %+v", nodes[0])
	}
	if !nodes[3].isHealthy() {
		t.Errorf("ddd44 should be healthy")
	}
	if nodes[5].isHealthy() {
		t.Errorf("fff66 should NOT be healthy (slave,fail + disconnected)")
	}
	if got := slotsCount(nodes[0].slots); got != 5461 {
		t.Errorf("demo-0 slot count = %d, want 5461", got)
	}
}

func TestSlotsCountHandlesRangesAndSingles(t *testing.T) {
	tests := []struct {
		name   string
		tokens []string
		want   int32
	}{
		{"single range", []string{"0-5460"}, 5461},
		{"single slot", []string{"42"}, 1},
		{"mixed", []string{"100", "200-202", "9000"}, 5},
		{"empty", nil, 0},
		{"garbage skipped", []string{"abc", "1-2", "[100->-id]"}, 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := slotsCount(tc.tokens); got != tc.want {
				t.Errorf("slotsCount(%v) = %d, want %d", tc.tokens, got, tc.want)
			}
		})
	}
}

func TestSummarizeShards(t *testing.T) {
	shards := []cachev1beta1.ShardStatus{
		{Index: 0, Health: cachev1beta1.ShardHealthReady},
		{Index: 1, Health: cachev1beta1.ShardHealthReady},
		{Index: 2, Health: cachev1beta1.ShardHealthDegraded},
		{Index: 3, Health: cachev1beta1.ShardHealthDown},
	}
	ready, bad := summarizeShards(shards)
	if ready != 2 {
		t.Errorf("ready = %d, want 2", ready)
	}
	if len(bad) != 2 || bad[0] != "2:Degraded" || bad[1] != "3:Down" {
		t.Errorf("bad = %v, want [2:Degraded 3:Down]", bad)
	}
}

func TestBuildStatefulSetPropagatesRestartToken(t *testing.T) {
	vc := minimalCR()
	vc.Annotations = map[string]string{restartAnnotation: "2026-05-28T10:00:00Z"}
	sts := buildStatefulSet(vc, "hash", false)
	if got := sts.Spec.Template.Annotations[restartedAtPodAnnotation]; got != "2026-05-28T10:00:00Z" {
		t.Errorf("restart token not propagated to pod template: %q", got)
	}

	// No restart annotation → no restart pod-template annotation (so steady
	// reconciles don't roll the StatefulSet).
	plain := buildStatefulSet(minimalCR(), "hash", false)
	if _, ok := plain.Spec.Template.Annotations[restartedAtPodAnnotation]; ok {
		t.Errorf("unexpected restart annotation on a cluster without a restart request")
	}
}

func TestBuildReshardJobRebalances(t *testing.T) {
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology: cachev1beta1.TopologyCluster,
			Image:    "valkey/valkey:9.0",
			Shards:   ptr.To[int32](3),
		},
	}
	s := jobScript(buildReshardJob(vc, "", "c-reshard"))
	for _, want := range []string{"--cluster rebalance", "--cluster-use-empty-masters", "--cluster-yes"} {
		if !strings.Contains(s, want) {
			t.Errorf("reshard job script missing %q\n%s", want, s)
		}
	}
}

func TestPickFailoverTarget(t *testing.T) {
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "web"},
		Spec:       cachev1beta1.ValkeyClusterSpec{Topology: cachev1beta1.TopologyReplication, Replicas: 3},
	}
	survey := []podRole{
		{ordinal: 0, reachable: true, role: "master"},
		{ordinal: 1, reachable: true, role: "slave", offset: 100},
		{ordinal: 2, reachable: true, role: "slave", offset: 200},
	}

	// Auto: highest-offset replica wins.
	if tgt, ok := pickFailoverTarget(vc, survey, ""); !ok || tgt.ordinal != 2 {
		t.Errorf("auto target = %+v ok=%v, want ordinal 2", tgt, ok)
	}
	// Explicit valid replica.
	if tgt, ok := pickFailoverTarget(vc, survey, "web-1"); !ok || tgt.ordinal != 1 {
		t.Errorf("explicit target = %+v ok=%v, want ordinal 1", tgt, ok)
	}
	// Explicit target that is the current master → invalid.
	if _, ok := pickFailoverTarget(vc, survey, "web-0"); ok {
		t.Errorf("promoting the current master must be rejected")
	}
	// Non-existent pod → invalid.
	if _, ok := pickFailoverTarget(vc, survey, "web-9"); ok {
		t.Errorf("unknown target pod must be rejected")
	}
	// No reachable replicas → no target.
	down := []podRole{{ordinal: 0, reachable: true, role: "master"}, {ordinal: 1, reachable: false, role: "slave"}}
	if _, ok := pickFailoverTarget(vc, down, ""); ok {
		t.Errorf("no reachable replica should yield no target")
	}
}

// downElapsed gates the failover debounce: a replica is promoted only after the
// primary has been unreachable for >= the threshold (chaos C-5: no premature
// failover of a briefly-busy primary).
func TestDownElapsed(t *testing.T) {
	now := time.Now()
	// Below threshold → not yet promotable.
	if downElapsed(now.Add(-5*time.Second), now, 10*time.Second) {
		t.Errorf("primary down 5s (< 10s) must NOT be promoted yet")
	}
	// At / past threshold → promotable.
	if !downElapsed(now.Add(-10*time.Second), now, 10*time.Second) {
		t.Errorf("primary down 10s (>= 10s) must be promotable")
	}
	// The production default behaves the same.
	if downElapsed(now.Add(-(failoverDownAfter - time.Second)), now, failoverDownAfter) {
		t.Errorf("primary down just under %s must NOT be promoted yet", failoverDownAfter)
	}
	if !downElapsed(now.Add(-2*failoverDownAfter), now, failoverDownAfter) {
		t.Errorf("primary down past %s must be promotable", failoverDownAfter)
	}
}

// pickAuthoritativeMaster must choose by DATA first so a restarted, empty
// self-master never wins over the master that still holds the keyspace — the
// core of the cha-03 failover data-loss fix.
func TestPickAuthoritativeMaster(t *testing.T) {
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "web"},
		Spec:       cachev1beta1.ValkeyClusterSpec{Topology: cachev1beta1.TopologyReplication, Replicas: 3},
	}

	// Split brain: empty restarted self-master (pod-0, the old Status.Primary)
	// vs a promoted data-holding master (pod-1). Data wins despite pod-0 being
	// Status.Primary — otherwise the empty node would re-wipe the cluster.
	vc.Status.Primary = "web-0"
	masters := []podRole{
		{ordinal: 0, reachable: true, role: roleMaster, keys: 0},
		{ordinal: 1, reachable: true, role: roleMaster, keys: 500},
	}
	if m := pickAuthoritativeMaster(vc, masters); m.ordinal != 1 {
		t.Errorf("authoritative master = ordinal %d, want 1 (most data, not empty Status.Primary)", m.ordinal)
	}

	// Equal data → prefer the pod that is already Status.Primary (stability).
	vc.Status.Primary = "web-2"
	eq := []podRole{
		{ordinal: 0, reachable: true, role: roleMaster, keys: 500},
		{ordinal: 2, reachable: true, role: roleMaster, keys: 500},
	}
	if m := pickAuthoritativeMaster(vc, eq); m.ordinal != 2 {
		t.Errorf("equal-data authoritative master = ordinal %d, want 2 (Status.Primary)", m.ordinal)
	}

	// Equal data, none is Status.Primary → lowest ordinal.
	vc.Status.Primary = ""
	if m := pickAuthoritativeMaster(vc, eq); m.ordinal != 0 {
		t.Errorf("tiebreak authoritative master = ordinal %d, want 0 (lowest ordinal)", m.ordinal)
	}
}

func TestSetConditionReadinessContract(t *testing.T) {
	var conds []metav1.Condition
	old := metav1.NewTime(time.Now().Add(-time.Hour))

	setCondition(&conds, metav1.Condition{
		Type: cachev1beta1.ConditionAvailable, Status: metav1.ConditionTrue,
		Reason: "Ready", Message: "a", ObservedGeneration: 1, LastTransitionTime: old,
	})
	if len(conds) != 1 || !conds[0].LastTransitionTime.Time.Equal(old.Time) {
		t.Fatalf("initial LastTransitionTime = %v, want %v", conds[0].LastTransitionTime, old)
	}

	// Same Status, but Message/ObservedGeneration advance and a fresh timestamp
	// is offered. The readiness contract: LastTransitionTime must NOT move (no
	// flap), while the other fields reconcile to the latest values.
	setCondition(&conds, metav1.Condition{
		Type: cachev1beta1.ConditionAvailable, Status: metav1.ConditionTrue,
		Reason: "Ready", Message: "b", ObservedGeneration: 2, LastTransitionTime: metav1.Now(),
	})
	if !conds[0].LastTransitionTime.Time.Equal(old.Time) {
		t.Errorf("LastTransitionTime flapped without a status change: %v != %v", conds[0].LastTransitionTime, old)
	}
	if conds[0].Message != "b" {
		t.Errorf("Message = %q, want b", conds[0].Message)
	}
	if conds[0].ObservedGeneration != 2 {
		t.Errorf("ObservedGeneration = %d, want 2 (must advance even without a status flip)", conds[0].ObservedGeneration)
	}

	// A real status flip advances LastTransitionTime.
	setCondition(&conds, metav1.Condition{
		Type: cachev1beta1.ConditionAvailable, Status: metav1.ConditionFalse,
		Reason: "PodsNotReady", Message: "c", ObservedGeneration: 3, LastTransitionTime: metav1.Now(),
	})
	if conds[0].LastTransitionTime.Time.Equal(old.Time) {
		t.Errorf("LastTransitionTime should advance on a status flip")
	}
	if conds[0].Status != metav1.ConditionFalse {
		t.Errorf("Status = %v, want False", conds[0].Status)
	}
}

func TestBuildPDBHonoursSpec(t *testing.T) {
	vc := minimalCR()
	vc.Spec.Replicas = 3
	pdb := buildPDB(vc)
	if pdb.Spec.MaxUnavailable == nil || pdb.Spec.MaxUnavailable.IntVal != 1 {
		t.Errorf("default PDB MaxUnavailable should be 1, got %+v", pdb.Spec.MaxUnavailable)
	}

	// MinAvailable wins over default MaxUnavailable when set.
	minVal := intstr.FromInt(2)
	vc.Spec.PodDisruptionBudget = &cachev1beta1.PDBSpec{Enabled: true, MinAvailable: &minVal}
	pdb = buildPDB(vc)
	if pdb.Spec.MinAvailable == nil || pdb.Spec.MaxUnavailable != nil {
		t.Errorf("MinAvailable should be set and MaxUnavailable cleared; got min=%v max=%v",
			pdb.Spec.MinAvailable, pdb.Spec.MaxUnavailable)
	}
}

// jobScript returns the /bin/sh -c script of the Job's single container.
func jobScript(job *batchv1.Job) string {
	cmd := job.Spec.Template.Spec.Containers[0].Command
	return cmd[len(cmd)-1]
}

func TestBuildScaleUpJobRebalanceGatedByAutoReshard(t *testing.T) {
	mk := func(auto bool) string {
		vc := &cachev1beta1.ValkeyCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
			Spec: cachev1beta1.ValkeyClusterSpec{
				Topology:    cachev1beta1.TopologyCluster,
				Image:       "valkey/valkey:9.0",
				Shards:      ptr.To[int32](6),
				AutoReshard: auto,
			},
			Status: cachev1beta1.ValkeyClusterStatus{LastAppliedReplicas: 3},
		}
		return jobScript(buildScaleUpJob(vc, "", "c-scaleup"))
	}
	if s := mk(true); !strings.Contains(s, "--cluster add-node") {
		t.Errorf("scale-up must add-node; script:\n%s", s)
	}
	if s := mk(true); !strings.Contains(s, "--cluster rebalance") || !strings.Contains(s, "--cluster-use-empty-masters") {
		t.Errorf("autoReshard=true scale-up must rebalance empty masters; script:\n%s", s)
	}
	// Regression: the rebalance must retry until every master owns slots
	// (cluster_size reaches the shard count). A single rebalance run can race
	// the gossip propagation of freshly-added empty masters and no-op with
	// "No rebalancing needed", silently leaving the new nodes empty. The loop
	// keyed on cluster_size guards against that.
	if s := mk(true); !strings.Contains(s, "cluster_size:") || !strings.Contains(s, "want_masters=6") {
		t.Errorf("autoReshard scale-up must loop rebalance until cluster_size >= shards; script:\n%s", s)
	}
	if s := mk(false); strings.Contains(s, "--cluster rebalance") {
		t.Errorf("autoReshard=false scale-up must NOT rebalance; script:\n%s", s)
	}
}

func TestBuildScaleDownJobRebalanceGatedByAutoReshard(t *testing.T) {
	mk := func(auto bool) string {
		vc := &cachev1beta1.ValkeyCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
			Spec: cachev1beta1.ValkeyClusterSpec{
				Topology:    cachev1beta1.TopologyCluster,
				Image:       "valkey/valkey:9.0",
				Shards:      ptr.To[int32](3),
				AutoReshard: auto,
			},
			Status: cachev1beta1.ValkeyClusterStatus{LastAppliedReplicas: 6},
		}
		return jobScript(buildScaleDownJob(vc, "", "c-scaledown"))
	}
	// Reshard-away + del-node happen regardless of autoReshard — that's the
	// data-safe teardown of leaving masters.
	if s := mk(false); !strings.Contains(s, "--cluster reshard") || !strings.Contains(s, "--cluster del-node") {
		t.Errorf("scale-down must reshard-away and del-node; script:\n%s", s)
	}
	if s := mk(true); !strings.Contains(s, "--cluster rebalance") {
		t.Errorf("autoReshard=true scale-down must rebalance survivors; script:\n%s", s)
	}
	if s := mk(false); strings.Contains(s, "--cluster rebalance") {
		t.Errorf("autoReshard=false scale-down must NOT rebalance; script:\n%s", s)
	}
}

// TestReshardScriptsUseASMFlag verifies the C3/ASM wiring (ADR 0001): every
// reshard path detects the Valkey version at runtime and appends $ASM_FLAG to the
// rebalance/reshard calls, so on 9.1+ slot moves go through Atomic Slot Migration
// (CLUSTER MIGRATESLOTS) instead of the interruptible key-by-key MIGRATE. The flag
// is empty on < 9.1, keeping the classic path.
func TestValkeyImageAtLeast(t *testing.T) {
	tests := []struct {
		image    string
		maj, min int
		want     bool
	}{
		{"valkey/valkey:8.0", 9, 0, false},
		{"valkey/valkey:8.0", 9, 1, false},
		{"valkey/valkey:9.0", 9, 0, true},
		{"valkey/valkey:9.0", 9, 1, false},
		{"valkey/valkey:9.1", 9, 1, true},
		{"valkey/valkey:9.1.0", 9, 1, true},
		{"valkey/valkey:10.0", 9, 1, true},
		{"valkey/valkey:9", 9, 0, true},  // bare major == x.0
		{"valkey/valkey:9", 9, 1, false}, // 9.0 < 9.1
		// Registry host:port prefix must not be mistaken for the tag.
		{"valkey/valkey:9.1.0", 9, 1, true},
		{"myreg:5000/valkey/valkey:9.1", 9, 1, true},
		{"myreg:5000/valkey/valkey", 9, 1, false}, // no tag
		// Unparseable → conservative false (never emit a directive that could
		// fatal a pod that turns out to be old).
		{"valkey/valkey:latest", 9, 1, false},
		{"valkey/valkey@sha256:deadbeef", 9, 1, false},
		{"valkey/valkey:9.1-rc1", 9, 1, true},
	}
	for _, tc := range tests {
		if got := valkeyImageAtLeast(tc.image, tc.maj, tc.min); got != tc.want {
			t.Errorf("valkeyImageAtLeast(%q, %d, %d) = %v, want %v", tc.image, tc.maj, tc.min, got, tc.want)
		}
	}
}

// TestRenderValkeyConfVersionGatedDirectives covers the upstream-sourced 9.x
// resilience config (lesson_upstream_contrib): cluster-allow-replica-migration is
// version-agnostic; shutdown-on-sigterm failover needs >= 9.0; tls-auto-reload
// needs >= 9.1 + TLS. Emitting a 9.x directive/value on 8.x fatals the pod, so the
// gate is load-bearing.
func TestRenderValkeyConfVersionGatedDirectives(t *testing.T) {
	mk := func(image string, tls bool) *cachev1beta1.ValkeyCluster {
		vc := minimalCR()
		vc.Spec.Topology = cachev1beta1.TopologyCluster
		vc.Spec.Shards = ptr.To[int32](3)
		vc.Spec.Image = image
		if tls {
			vc.Spec.TLS = &cachev1beta1.TLSSpec{Enabled: true}
		}
		return vc
	}
	// cluster-allow-replica-migration no — every Cluster, any version (closes the
	// C3/C4 emptied-primary auto-demote race).
	for _, img := range []string{"valkey/valkey:8.0", "valkey/valkey:9.1"} {
		if !strings.Contains(renderValkeyConf(mk(img, false), ""), "cluster-allow-replica-migration no") {
			t.Errorf("%s: must always disable replica auto-migration for Cluster", img)
		}
	}
	// shutdown-on-sigterm failover — only >= 9.0 (8.x fatals on the value).
	if c := renderValkeyConf(mk("valkey/valkey:8.0", false), ""); strings.Contains(c, "shutdown-on-sigterm") {
		t.Errorf("8.0 must NOT set shutdown-on-sigterm failover (fatal on 8.x)\n%s", c)
	}
	if c := renderValkeyConf(mk("valkey/valkey:9.0", false), ""); !strings.Contains(c, "shutdown-on-sigterm failover") {
		t.Errorf("9.0 must set shutdown-on-sigterm failover\n%s", c)
	}
	// tls-auto-reload-interval — only >= 9.1 AND TLS on.
	if c := renderValkeyConf(mk("valkey/valkey:9.1", false), ""); strings.Contains(c, "tls-auto-reload-interval") {
		t.Errorf("no TLS → no tls-auto-reload-interval\n%s", c)
	}
	if c := renderValkeyConf(mk("valkey/valkey:9.0", true), ""); strings.Contains(c, "tls-auto-reload-interval") {
		t.Errorf("9.0 (< 9.1) must NOT set tls-auto-reload-interval (fatal on 9.0)\n%s", c)
	}
	if c := renderValkeyConf(mk("valkey/valkey:9.1", true), ""); !strings.Contains(c, "tls-auto-reload-interval 3600") {
		t.Errorf("9.1 + TLS must set tls-auto-reload-interval\n%s", c)
	}
	// Non-Cluster topology gets none of the cluster-* directives.
	std := minimalCR()
	std.Spec.Image = "valkey/valkey:9.1"
	if c := renderValkeyConf(std, ""); strings.Contains(c, "shutdown-on-sigterm") || strings.Contains(c, "cluster-allow-replica-migration") {
		t.Errorf("non-Cluster must not get cluster directives\n%s", c)
	}
}

func TestRenderValkeyConfReplicaValidityFactor(t *testing.T) {
	mk := func(profile cachev1beta1.Profile, topo cachev1beta1.Topology, image string) *cachev1beta1.ValkeyCluster {
		vc := minimalCR()
		vc.Spec.Topology = topo
		vc.Spec.Shards = ptr.To[int32](3)
		vc.Spec.Profile = profile
		if image != "" {
			vc.Spec.Image = image
		}
		return vc
	}

	// Cache is availability-first: disable the replica-validity gate so a stale
	// replica can still win an election (avoids a stuck shard). Any Valkey
	// version — old, stable directive, no version gate.
	for _, img := range []string{"valkey/valkey:8.0", "valkey/valkey:9.1"} {
		if c := renderValkeyConf(mk(cachev1beta1.ProfileCache, cachev1beta1.TopologyCluster, img), ""); !strings.Contains(c, "cluster-replica-validity-factor 0") {
			t.Errorf("%s Cache/Cluster must set cluster-replica-validity-factor 0\n%s", img, c)
		}
	}

	// Durable keeps the default gate (nothing emitted): promoting an
	// arbitrarily-stale replica would silently lose acknowledged writes.
	if c := renderValkeyConf(mk(cachev1beta1.ProfileDurable, cachev1beta1.TopologyCluster, ""), ""); strings.Contains(c, "cluster-replica-validity-factor") {
		t.Errorf("Durable/Cluster must NOT override the validity gate\n%s", c)
	}

	// Only Cluster topology gets the directive.
	if c := renderValkeyConf(mk(cachev1beta1.ProfileCache, cachev1beta1.TopologyReplication, ""), ""); strings.Contains(c, "cluster-replica-validity-factor") {
		t.Errorf("non-Cluster must not get cluster-replica-validity-factor\n%s", c)
	}

	// spec.config overrides the operator default (rendered last → wins).
	over := mk(cachev1beta1.ProfileCache, cachev1beta1.TopologyCluster, "")
	over.Spec.Config = map[string]string{"cluster-replica-validity-factor": "10"}
	if c := renderValkeyConf(over, ""); !strings.Contains(c, "cluster-replica-validity-factor 10") {
		t.Errorf("spec.config must be able to override the validity factor\n%s", c)
	}
}

func TestReshardScriptsUseASMFlag(t *testing.T) {
	mkVC := func(perShard bool) *cachev1beta1.ValkeyCluster {
		vc := &cachev1beta1.ValkeyCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
			Spec: cachev1beta1.ValkeyClusterSpec{
				Topology:    cachev1beta1.TopologyCluster,
				Image:       "valkey/valkey:9.1",
				Shards:      ptr.To[int32](3),
				AutoReshard: true,
			},
			Status: cachev1beta1.ValkeyClusterStatus{LastAppliedReplicas: 6},
		}
		if perShard {
			vc.Spec.PerShardWorkload = ptr.To(true)
		}
		return vc
	}
	// The runtime gate must key on valkey_version >= 9.1 and yield the CLI flag.
	assertSnippet := func(t *testing.T, label, s string) {
		t.Helper()
		for _, want := range []string{
			`ASM_FLAG=""`,
			"valkey_version",
			"--cluster-use-atomic-slot-migration",
			"$ASM_FLAG",
		} {
			if !strings.Contains(s, want) {
				t.Errorf("%s: missing %q\n%s", label, want, s)
			}
		}
	}

	// Single-STS scale-up (rebalance), scale-down (reshard + survivor rebalance),
	// manual reshard.
	up := mkVC(false)
	up.Status.LastAppliedReplicas = 3
	assertSnippet(t, "scale-up", jobScript(buildScaleUpJob(up, "", "c-up")))
	assertSnippet(t, "scale-down", jobScript(buildScaleDownJob(mkVC(false), "", "c-down")))
	assertSnippet(t, "manual-reshard", jobScript(buildReshardJob(mkVC(false), "", "c-reshard")))

	// Per-shard (ADR 0005) scale-up and scale-down go through the shard scripts.
	psUp := mkVC(true)
	psUp.Status.LastAppliedReplicas = 3
	assertSnippet(t, "per-shard scale-up", jobScript(buildScaleUpJob(psUp, "", "c-up")))
	assertSnippet(t, "per-shard scale-down", jobScript(buildScaleDownJob(mkVC(true), "", "c-down")))

	// The flag must ride ON the rebalance/reshard call, not float loose.
	down := jobScript(buildScaleDownJob(mkVC(false), "", "c-down"))
	if !strings.Contains(down, "--cluster-slots 16384$ASM_FLAG") {
		t.Errorf("scale-down reshard must append $ASM_FLAG to the reshard call:\n%s", down)
	}
}

// Regression for the cha-02 scale-down data-loss: the leaving-node entries must
// be bare FQDNs (no :port), or the `$2 ~ p` match against CLUSTER NODES never
// fires, the node is silently "skipped", and the Job exits 0 without resharding
// — after which the operator shrinks the StatefulSet and deletes a pod that
// still owns slots. The script must also (a) target pod-0's own id (`myself`)
// for the reshard, never a leaving node, and (b) refuse del-node while the node
// still owns slots.
func TestBuildScaleDownJobIsDataSafe(t *testing.T) {
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology: cachev1beta1.TopologyCluster,
			Image:    "valkey/valkey:8.0",
			Shards:   ptr.To[int32](3),
		},
		Status: cachev1beta1.ValkeyClusterStatus{LastAppliedReplicas: 5},
	}
	s := jobScript(buildScaleDownJob(vc, "", "c-scaledown"))

	// Leaving hosts (ordinals 3,4) must be bare FQDNs with NO :port suffix, or
	// the `$2 ~ p` match against CLUSTER NODES never fires. (The cluster contact
	// endpoint c-0:port legitimately keeps its port — that's the reshard/del-node
	// target, not a match pattern.)
	if !strings.Contains(s, "c-3.c-headless.ns.svc.cluster.local") ||
		!strings.Contains(s, "c-4.c-headless.ns.svc.cluster.local") {
		t.Errorf("scale-down 5->3 must target leaving ordinals 3 and 4; script:\n%s", s)
	}
	if strings.Contains(s, "c-3.c-headless.ns.svc.cluster.local:") ||
		strings.Contains(s, "c-4.c-headless.ns.svc.cluster.local:") {
		t.Errorf("leaving hosts must be bare FQDNs (no :port) so the CLUSTER NODES match fires; script:\n%s", s)
	}
	// Reshard target is pod-0's own (myself) id — never a leaving master.
	if !strings.Contains(s, "awk '/myself/ {print $1; exit}'") {
		t.Errorf("reshard target must be pod-0's own (myself) id; script:\n%s", s)
	}
	// Safety gate: never del-node a master that still owns slots.
	if !strings.Contains(s, "still owns slots") || !strings.Contains(s, "refusing del-node") {
		t.Errorf("scale-down must refuse del-node while the node still owns slots; script:\n%s", s)
	}
}

func assertContainerRestricted(t *testing.T, who string, sc *corev1.SecurityContext) {
	t.Helper()
	if sc == nil {
		t.Errorf("%s: nil container securityContext", who)
		return
	}
	if sc.AllowPrivilegeEscalation == nil || *sc.AllowPrivilegeEscalation {
		t.Errorf("%s: allowPrivilegeEscalation must be false", who)
	}
	if sc.RunAsNonRoot == nil || !*sc.RunAsNonRoot {
		t.Errorf("%s: runAsNonRoot must be true", who)
	}
	if sc.SeccompProfile == nil || sc.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Errorf("%s: seccompProfile must be RuntimeDefault", who)
	}
	dropsAll := false
	if sc.Capabilities != nil {
		for _, c := range sc.Capabilities.Drop {
			if c == "ALL" {
				dropsAll = true
			}
		}
	}
	if !dropsAll {
		t.Errorf("%s: capabilities must drop ALL", who)
	}
}

func assertPodTemplateRestricted(t *testing.T, spec corev1.PodSpec) {
	t.Helper()
	ps := spec.SecurityContext
	if ps == nil {
		t.Fatal("nil pod securityContext")
	}
	if ps.RunAsNonRoot == nil || !*ps.RunAsNonRoot {
		t.Error("pod runAsNonRoot must be true")
	}
	if ps.SeccompProfile == nil || ps.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Error("pod seccompProfile must be RuntimeDefault")
	}
	if ps.RunAsUser == nil || *ps.RunAsUser != 1000 || ps.FSGroup == nil || *ps.FSGroup != 1000 {
		t.Error("pod must keep fsGroup/runAsUser 1000")
	}
	if len(spec.InitContainers) == 0 || len(spec.Containers) == 0 {
		t.Fatal("expected init + main containers")
	}
	for _, c := range spec.InitContainers {
		assertContainerRestricted(t, "init/"+c.Name, c.SecurityContext)
	}
	for _, c := range spec.Containers {
		assertContainerRestricted(t, c.Name, c.SecurityContext)
	}
}

func TestPodAndContainerSecurityContextRestrictedDefaults(t *testing.T) {
	// Main StatefulSet, metrics on so the exporter sidecar is covered too.
	vc := minimalCR()
	vc.Spec.Metrics = &cachev1beta1.MetricsSpec{Enabled: true}
	sts := buildStatefulSet(vc, "h", false)
	if got := len(sts.Spec.Template.Spec.Containers); got < 2 {
		t.Fatalf("expected valkey + exporter containers, got %d", got)
	}
	assertPodTemplateRestricted(t, sts.Spec.Template.Spec)

	// Sentinel StatefulSet.
	assertPodTemplateRestricted(t, buildSentinelStatefulSet(sentinelCR(), false).Spec.Template.Spec)

	// User-provided contexts replace the defaults verbatim.
	custom := minimalCR()
	custom.Spec.PodSecurityContext = &corev1.PodSecurityContext{RunAsUser: ptr.To[int64](2000)}
	custom.Spec.ContainerSecurityContext = &corev1.SecurityContext{Privileged: ptr.To(true)}
	cs := buildStatefulSet(custom, "h", false)
	if ps := cs.Spec.Template.Spec.SecurityContext; ps == nil || ps.RunAsUser == nil || *ps.RunAsUser != 2000 {
		t.Errorf("user podSecurityContext not honored: %+v", ps)
	}
	for _, c := range cs.Spec.Template.Spec.Containers {
		if c.SecurityContext == nil || c.SecurityContext.Privileged == nil || !*c.SecurityContext.Privileged {
			t.Errorf("%s: user containerSecurityContext not honored", c.Name)
		}
	}
}
