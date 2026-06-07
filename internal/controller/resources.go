/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

const (
	valkeyPort     int32 = 6379
	valkeyTLSPort  int32 = 6380
	clusterBusPort int32 = 16379
	exporterPort   int32 = 9121
	// tlsAutoReloadIntervalSec is the Valkey 9.1+ background TLS-reload cadence
	// (fallback to the operator's watch-driven reload — see renderValkeyConf).
	tlsAutoReloadIntervalSec = 3600
	configVolumeName         = "config"
	dataVolumeName           = "data"
	tlsVolumeName            = "tls"
	// sourceCAVolumeName/Path carry the source cluster's CA bundle (S4) into the
	// config-init container so it can be merged with the local CA into the
	// combined trust bundle on the data PVC.
	sourceCAVolumeName   = "source-ca"
	sourceCAMountPath    = "/etc/valkey/source-ca"
	configHashAnnotation = "valkey.wellcake.io/config-hash"
	// Operator-honored request annotations on the ValkeyCluster CR. Each carries
	// an opaque token (the plugin writes a timestamp); the reconciler acts once
	// per distinct token and records the handled token in status.
	restartAnnotation        = "valkey.wellcake.io/restart"
	restartedAtPodAnnotation = "valkey.wellcake.io/restarted-at"
	reshardAnnotation        = "valkey.wellcake.io/reshard"
	failoverAnnotation       = "valkey.wellcake.io/failover"
	failoverTargetAnnotation = "valkey.wellcake.io/failover-target"
	rotatePasswordAnnotation = "valkey.wellcake.io/rotate-password"
	hibernateAnnotation      = "valkey.wellcake.io/hibernate" // "true" → scale to 0, keep PVCs
	// proactiveRolloutAnnotation opts a single cluster into ADR 0004 proactive
	// rolling restart ("true" → OnDelete + operator-driven rollout). Per-cluster
	// (no global flag / CRD change) and consistent with the other opt-in
	// annotations above; lets it ride out behind a CR toggle while it soaks.
	proactiveRolloutAnnotation = "valkey.wellcake.io/proactive-rollout"
	configMountPath            = "/etc/valkey"
	dataMountPath              = "/data"
	tlsMountPath               = "/etc/valkey/tls"
	defaultDataSize            = "10Gi"
	defaultCacheDataSize       = "1Gi"
	componentLabel             = "app.kubernetes.io/component"
	nameLabel                  = "app.kubernetes.io/name"
	instanceLabel              = "app.kubernetes.io/instance"
	partOfLabel                = "app.kubernetes.io/part-of"
	managedByLabel             = "app.kubernetes.io/managed-by"

	// Shared literals used across job/pod/script builders.
	shellCmd            = "/bin/sh"
	metricsPortName     = "metrics"
	backupVolumeName    = "backup"
	envValkeyPassword   = "VALKEY_PASSWORD"
	envAWSAccessKey     = "AWS_ACCESS_KEY_ID"
	envAWSSecretKey     = "AWS_SECRET_ACCESS_KEY"
	envAWSDefaultRegion = "AWS_DEFAULT_REGION"
	envS3EndpointURL    = "ENDPOINT_URL"
	workVolumeName      = "work"
	defaultAWSCLIImage  = "amazon/aws-cli:2.15.0"
	// s3EndpointURLFlag is appended to aws-cli calls when a custom S3 endpoint
	// is set; the value is read at runtime from the ENDPOINT_URL env var.
	s3EndpointURLFlag = ` --endpoint-url "$ENDPOINT_URL"`
	// noAuthWarningArgs is appended to in-container valkey-cli invocations when
	// auth is enabled, reading the password from the VALKEY_PASSWORD env var.
	noAuthWarningArgs = ` -a "$VALKEY_PASSWORD" --no-auth-warning`

	// Persistence modes (spec.storage.mode / profile-derived).
	storageModeAOF  = "aof"
	storageModeBoth = "both"
	storageModeNone = "none"

	// Shared identifiers / well-known values.
	secretKeyPassword = "password"
	appValkey         = "valkey"
	operatorName      = "valkey-operator" // part-of / managed-by label value
	valkeyConfFile    = "valkey.conf"
	componentSentinel = "sentinel"
	reasonReady       = "Ready"
	valueTrue         = "true"
	// Replication roles as reported by INFO replication / CLUSTER NODES.
	roleMaster = "master"
	roleSlave  = "slave"
	// topologyKeyHostname is the node-level failure domain for pod (anti-)affinity.
	topologyKeyHostname = "kubernetes.io/hostname"
	// Prometheus metric label names reused across collectors.
	labelNamespace = "namespace"
	labelCluster   = "cluster"
	labelResult    = "result"
)

// usesPersistence is deliberately always true: emptyDir is unsafe because
// ephemeral-storage pressure can trigger pod eviction (kubelet evicts on
// nodefs.available). For Valkey that would mean an availability hit on an
// in-memory service. We always provision a PVC; for Cache profile it's small
// (1Gi by default) and persistence directives are disabled in valkey.conf so
// nothing is written to it.

func labelsFor(vc *cachev1beta1.ValkeyCluster) map[string]string {
	return map[string]string{
		nameLabel:      appValkey,
		instanceLabel:  vc.Name,
		partOfLabel:    operatorName,
		managedByLabel: operatorName,
		componentLabel: strings.ToLower(string(vc.Spec.Topology)),
	}
}

func headlessServiceName(vc *cachev1beta1.ValkeyCluster) string {
	return fmt.Sprintf("%s-headless", vc.Name)
}

func clientServiceName(vc *cachev1beta1.ValkeyCluster) string { return vc.Name }

// internalEndpoint is the in-cluster client endpoint (host:port) of the client
// Service, surfaced in status for consumers (and wrappers like Crossplane).
func internalEndpoint(vc *cachev1beta1.ValkeyCluster) string {
	port := valkeyPort
	if tlsEnabled(vc) {
		port = valkeyTLSPort
	}
	return fmt.Sprintf("%s.%s.svc:%d", clientServiceName(vc), vc.Namespace, port)
}

func statefulSetName(vc *cachev1beta1.ValkeyCluster) string { return vc.Name }

func configMapName(vc *cachev1beta1.ValkeyCluster) string {
	return fmt.Sprintf("%s-config", vc.Name)
}

func passwordSecretName(vc *cachev1beta1.ValkeyCluster) string {
	return fmt.Sprintf("%s-auth", vc.Name)
}

// hibernated reports whether the cluster is requested to be hibernated
// (scaled to zero while keeping PVCs) via the hibernate annotation.
func hibernated(vc *cachev1beta1.ValkeyCluster) bool {
	return vc.Annotations[hibernateAnnotation] == valueTrue
}

func tlsSecretName(vc *cachev1beta1.ValkeyCluster) string {
	if vc.Spec.TLS != nil && vc.Spec.TLS.ExistingSecret != "" {
		return vc.Spec.TLS.ExistingSecret
	}
	return fmt.Sprintf("%s-tls", vc.Name)
}

// configHashFromData returns a deterministic short hash of a ConfigMap.Data map,
// used to roll the StatefulSet pods whenever valkey.conf or entrypoint.sh changes.
// The auth password is intentionally NOT included — it lives in a Secret and is
// loaded at process start via env/mount; rotating it is handled separately.
func configHashFromData(data map[string]string) string {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte{0})
		h.Write([]byte(data[k]))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

func generatePassword(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b)[:n], nil
}

func buildHeadlessService(vc *cachev1beta1.ValkeyCluster) *corev1.Service {
	port := valkeyPort
	if tlsEnabled(vc) {
		port = valkeyTLSPort
	}
	ports := []corev1.ServicePort{{
		Name:       appValkey,
		Port:       port,
		TargetPort: intstr.FromInt32(port),
		Protocol:   corev1.ProtocolTCP,
	}}
	if vc.Spec.Topology == cachev1beta1.TopologyCluster {
		ports = append(ports, corev1.ServicePort{
			Name:       "gossip",
			Port:       clusterBusPort,
			TargetPort: intstr.FromInt32(clusterBusPort),
			Protocol:   corev1.ProtocolTCP,
		})
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      headlessServiceName(vc),
			Namespace: vc.Namespace,
			Labels:    labelsFor(vc),
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			PublishNotReadyAddresses: true,
			Selector:                 labelsFor(vc),
			Ports:                    ports,
		},
	}
}

func buildClientService(vc *cachev1beta1.ValkeyCluster) *corev1.Service {
	port := valkeyPort
	if tlsEnabled(vc) {
		port = valkeyTLSPort
	}
	ports := []corev1.ServicePort{{
		Name:       appValkey,
		Port:       port,
		TargetPort: intstr.FromInt32(port),
		Protocol:   corev1.ProtocolTCP,
	}}
	if metricsEnabled(vc) {
		ports = append(ports, corev1.ServicePort{
			Name:       metricsPortName,
			Port:       exporterPort,
			TargetPort: intstr.FromInt32(exporterPort),
			Protocol:   corev1.ProtocolTCP,
		})
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clientServiceName(vc),
			Namespace: vc.Namespace,
			Labels:    labelsFor(vc),
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: labelsFor(vc),
			Ports:    ports,
		},
	}
}

func buildConfigMap(vc *cachev1beta1.ValkeyCluster, password string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName(vc),
			Namespace: vc.Namespace,
			Labels:    labelsFor(vc),
		},
		Data: map[string]string{
			valkeyConfFile: renderValkeyConf(vc, password),
		},
	}
}

// valkeyImageAtLeast reports whether the configured Valkey image tag is at least
// maj.min. Used to gate config directives (or directive VALUES) that only exist on
// newer Valkey — rendering an unknown directive/value makes valkey-server fatal at
// startup (verified: 8.0 dies on `shutdown-on-sigterm failover` and on
// `tls-auto-reload-interval`). Parsed conservatively: a digest pin, a non-numeric
// tag (latest/stable) or a missing tag returns false (assume old → omit the new
// directive), so an unparseable image never yields a pod that won't boot. Users on
// an exotic-but-new image can still set the directive via spec.config.
func valkeyImageAtLeast(image string, maj, min int) bool {
	if i := strings.IndexByte(image, '@'); i >= 0 {
		image = image[:i] // drop digest
	}
	colon := strings.LastIndexByte(image, ':')
	// A ':' before the last '/' is a registry host:port, not a tag.
	if colon < 0 || colon < strings.LastIndexByte(image, '/') {
		return false
	}
	parts := strings.SplitN(image[colon+1:], ".", 3)
	gotMaj, err := strconv.Atoi(parts[0])
	if err != nil {
		return false
	}
	gotMin := 0
	if len(parts) > 1 {
		// Take leading digits of the minor (tolerate suffixes like "1-rc1").
		m := parts[1]
		j := 0
		for j < len(m) && m[j] >= '0' && m[j] <= '9' {
			j++
		}
		gotMin, _ = strconv.Atoi(m[:j])
	}
	if gotMaj != maj {
		return gotMaj > maj
	}
	return gotMin >= min
}

func renderValkeyConf(vc *cachev1beta1.ValkeyCluster, password string) string {
	var b strings.Builder
	if tlsEnabled(vc) {
		fmt.Fprintf(&b, "port 0\n")
		fmt.Fprintf(&b, "tls-port %d\n", valkeyTLSPort)
		fmt.Fprintf(&b, "tls-cluster yes\n")
		fmt.Fprintf(&b, "tls-cert-file %s/tls.crt\n", tlsMountPath)
		fmt.Fprintf(&b, "tls-key-file %s/tls.key\n", tlsMountPath)
		fmt.Fprintf(&b, "tls-ca-cert-file %s\n", caCertPath(vc))
		fmt.Fprintf(&b, "tls-replication yes\n")
		authClients := "optional"
		if vc.Spec.TLS.MutualTLS {
			authClients = "yes" // enforce mTLS: clients must present a CA-signed cert
		}
		fmt.Fprintf(&b, "tls-auth-clients %s\n", authClients)
		// Valkey 9.1+ reloads TLS material in a background thread on this interval
		// (seconds) — a belt-and-suspenders fallback to the operator's own
		// watch-driven CONFIG SET reload (AR5): if the operator is down during a
		// cert-manager renewal, the in-process timer still picks the new cert up
		// within the hour. Older Valkey lacks the directive and would fatal on it,
		// so gate on >= 9.1. Both paths just re-read the same mounted files.
		if valkeyImageAtLeast(vc.Spec.Image, 9, 1) {
			fmt.Fprintf(&b, "tls-auto-reload-interval %d\n", tlsAutoReloadIntervalSec)
		}
	} else {
		fmt.Fprintf(&b, "port %d\n", valkeyPort)
	}
	fmt.Fprintf(&b, "bind 0.0.0.0\n")
	fmt.Fprintf(&b, "protected-mode no\n")
	fmt.Fprintf(&b, "dir %s\n", dataMountPath)
	if password != "" {
		fmt.Fprintf(&b, "requirepass %s\n", password)
		fmt.Fprintf(&b, "masterauth %s\n", password)
	}

	// Persist ACL state on the data PVC so users survive pod restarts.
	// `ACL SAVE` (called by ValkeyACLReconciler) writes here; Valkey loads
	// this file at startup. Without an aclfile, ACL changes are in-memory
	// only and lost on every restart — making the ValkeyACL CRD useless in
	// practice.
	fmt.Fprintf(&b, "aclfile %s/users.acl\n", dataMountPath)

	// Cluster-mode directives.
	if vc.Spec.Topology == cachev1beta1.TopologyCluster {
		fmt.Fprintf(&b, "cluster-enabled yes\n")
		fmt.Fprintf(&b, "cluster-config-file %s/nodes.conf\n", dataMountPath)
		// 5s, not the upstream 15s default: inside a k8s cluster the pod
		// network is low-latency, so a tighter timeout detects a dead shard
		// primary (and triggers failover) much faster without meaningful
		// false-positive risk. Overridable via spec.config.
		fmt.Fprintf(&b, "cluster-node-timeout 5000\n")
		// Durable profile requires full coverage; Cache prefers availability.
		if vc.Spec.Profile == cachev1beta1.ProfileDurable {
			fmt.Fprintf(&b, "cluster-require-full-coverage yes\n")
		} else {
			fmt.Fprintf(&b, "cluster-require-full-coverage no\n")
		}
		// Disable replica auto-migration for managed clusters. Default `yes`
		// (historical) lets a replica jump to an orphaned primary AND makes a
		// primary that loses its last slots (e.g. drained during scale-down)
		// auto-demote to a replica of whoever took those slots — so the operator's
		// view of the topology diverges from the live cluster mid-reshard (root of
		// C3/C4). With `no` the emptied primary stays an empty primary (role does
		// not flip, dbsize does not grow): scale-down is deterministic — drain
		// slots (ASM), then CLUSTER FORGET + remove the node. Old directive, works
		// on all versions. Confirmed by upstream maintainer (valkey-operator #216).
		// Overridable via spec.config.
		fmt.Fprintf(&b, "cluster-allow-replica-migration no\n")
		// Valkey 9.0+: on SIGTERM a cluster primary does a graceful manual failover
		// to an up-to-date replica before shutting down — a node-local safety net
		// for OUT-OF-BAND descheduling (node drain / eviction / preemption /
		// `kubectl delete pod`) that the operator's proactive rollout does not see.
		// Only fires if this node is still a primary, so it never races a failover
		// the operator already performed. Needs terminationGracePeriodSeconds >=
		// cluster-manual-failover-timeout (default 5s; the pod default 30s covers
		// it). The `failover` value is 9.0+ (8.x fatals on it), so gate.
		// Overridable via spec.config.
		if valkeyImageAtLeast(vc.Spec.Image, 9, 0) {
			fmt.Fprintf(&b, "shutdown-on-sigterm failover\n")
		}
	}

	// Profile-driven persistence defaults; explicit Storage.Mode overrides.
	mode := persistenceMode(vc)
	switch mode {
	case storageModeAOF:
		fmt.Fprintf(&b, "appendonly yes\nsave \"\"\n")
	case storageModeBoth:
		fmt.Fprintf(&b, "appendonly yes\nsave 3600 1 300 100 60 10000\n")
	case storageModeNone:
		fmt.Fprintf(&b, "appendonly no\nsave \"\"\n")
	default: // rdb
		fmt.Fprintf(&b, "appendonly no\nsave 3600 1 300 100 60 10000\n")
	}

	// Profile-driven defaults for eviction & maxmemory unless overridden by Config.
	cfg := vc.Spec.Config
	if cfg == nil {
		cfg = map[string]string{}
	}
	if _, ok := cfg["maxmemory-policy"]; !ok {
		if vc.Spec.Profile == cachev1beta1.ProfileDurable {
			cfg["maxmemory-policy"] = "noeviction"
		} else {
			cfg["maxmemory-policy"] = "allkeys-lru"
		}
	}
	if _, ok := cfg["maxmemory"]; !ok {
		if mm := computeMaxmemory(vc); mm != "" {
			cfg["maxmemory"] = mm
		}
	}
	// Replication backlog. The upstream 1MB default is far too small for a
	// k8s deployment: pods are restarted routinely (rollouts, evictions,
	// rescheduling), and a 1MB backlog overflows in seconds under load, so a
	// reconnecting replica misses the PSYNC window and does a FULL resync
	// (BGSAVE + RDB transfer + CoW spike on the primary). Sizing it to the
	// instance keeps reconnects as cheap partial resyncs. Only topologies
	// that actually have replicas allocate a backlog (Standalone never does),
	// so this is a no-op for single-node clusters. Overridable via spec.config.
	if _, ok := cfg["repl-backlog-size"]; !ok {
		if vc.Spec.Topology != cachev1beta1.TopologyStandalone {
			if bl := computeReplBacklog(vc); bl != "" {
				cfg["repl-backlog-size"] = bl
			}
		}
	}

	keys := make([]string, 0, len(cfg))
	for k := range cfg {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&b, "%s %s\n", k, cfg[k])
	}
	return b.String()
}

// persistenceMode returns the effective persistence mode for this cluster.
func persistenceMode(vc *cachev1beta1.ValkeyCluster) string {
	// A tmpfs data dir (storage.medium=Memory) is ephemeral RAM — persisting
	// RDB/AOF there is pointless (lost on restart) and wastes memory, so force
	// persistence off regardless of the requested mode.
	if usesMemoryStorage(vc) {
		return storageModeNone
	}
	if vc.Spec.Storage != nil && vc.Spec.Storage.Mode != "" {
		return vc.Spec.Storage.Mode
	}
	if vc.Spec.Profile == cachev1beta1.ProfileDurable {
		return storageModeBoth
	}
	return storageModeNone
}

// computeMaxmemory returns 60% of the memory limit as a maxmemory value.
// Leaves room for fragmentation, COW during fork, and replication buffers.
func computeMaxmemory(vc *cachev1beta1.ValkeyCluster) string {
	lim, ok := vc.Spec.Resources.Limits[corev1.ResourceMemory]
	if !ok || lim.IsZero() {
		return ""
	}
	bytes := lim.Value() * 60 / 100
	if bytes <= 0 {
		return ""
	}
	return fmt.Sprintf("%db", bytes)
}

// computeReplBacklog sizes the replication backlog to the instance instead of
// leaving the 1MB upstream default. It returns ~1/16 of the memory limit,
// clamped to [16MB, 256MB]: large enough that a routine pod restart reconnects
// via partial resync rather than a full RDB transfer, small enough that it
// stays a minor slice of the footprint even on dev-sized instances (the
// backlog counts toward used_memory/RSS). Returns "" if no memory limit is
// set, in which case the upstream default stands.
func computeReplBacklog(vc *cachev1beta1.ValkeyCluster) string {
	lim, ok := vc.Spec.Resources.Limits[corev1.ResourceMemory]
	if !ok || lim.IsZero() {
		return ""
	}
	const minBytes = 16 * 1024 * 1024
	const maxBytes = 256 * 1024 * 1024
	// ~1/16 of the memory limit, clamped to [minBytes, maxBytes].
	bytes := min(max(lim.Value()/16, minBytes), maxBytes)
	return fmt.Sprintf("%db", bytes)
}

// renderInitScript produces a shell script that the initContainer runs to
// compose the per-pod runtime config from the shared valkey.conf ConfigMap.
// Putting this in an initContainer (rather than as a script inside the
// ConfigMap, which is an anti-pattern for CVE scanners and immutable-image
// policies) keeps the runtime image clean and the script trivial.
//   - Replication/Standalone: pod-0 is primary, others add `replicaof pod-0`.
//     The operator's failover loop later may rewrite roles via REPLICAOF.
//   - Cluster: no replicaof — bootstrap Job calls `valkey-cli --cluster create`
//     once all pods are up; here we only announce stable DNS for gossip.
func renderInitScript(vc *cachev1beta1.ValkeyCluster) string {
	port := valkeyPort
	if tlsEnabled(vc) {
		port = valkeyTLSPort
	}
	headless := headlessServiceName(vc)
	stsName := statefulSetName(vc)

	// Compose the runtime config and seed the ACL file. Valkey applies
	// `requirepass` first, then loads `aclfile`, and the aclfile is
	// authoritative — so an empty users.acl resets the `default` user to
	// `nopass` and silently overrides requirepass (default user ends up with no
	// password). When a password is configured we therefore seed the default
	// user (carrying that password) into users.acl, but only when the file is
	// absent/empty so a persisted `ACL SAVE` (written by ValkeyACLReconciler) is
	// never clobbered on restart. With no password we leave it empty so the
	// default user stays nopass, matching auth-disabled intent. The file must
	// exist regardless — Valkey refuses to start if aclfile points at nothing.
	// For Sentinel topology, seed a dedicated least-data-exposure ACL user that
	// Sentinel uses to reach the master (sentinel auth-user). It gets all
	// commands and all pub/sub channels (Sentinel needs INFO/REPLICAOF/CONFIG
	// REWRITE/CLIENT KILL/SCRIPT KILL plus the __sentinel__:hello channel) but
	// NO key glob — so it cannot read or write your data, unlike the default
	// user. (S1 hardening; tightening to the minimal per-command set is a
	// follow-up that needs e2e failover validation.)
	sentinelACL := ""
	if vc.Spec.Topology == cachev1beta1.TopologySentinel {
		sentinelACL = fmt.Sprintf("    echo \"user %s on >$PW &* +@all\" >> %s/users.acl\n",
			sentinelACLUser, dataMountPath)
	}
	common := fmt.Sprintf(`set -eu
cp %[1]s/valkey.conf %[2]s/runtime.conf
if [ ! -s %[2]s/users.acl ]; then
  PW=$(sed -n 's/^requirepass //p' %[2]s/runtime.conf | head -n1)
  if [ -n "$PW" ]; then
    echo "user default on >$PW ~* &* +@all" > %[2]s/users.acl
%[3]s  else
    : > %[2]s/users.acl
  fi
fi
`, configMountPath, dataMountPath, sentinelACL)

	// Multi-region: every pod (including pod-0) replicates from an external
	// primary. Local primary/replica entrypoint logic is bypassed.
	// The source password (if any) is delivered via the SOURCE_PASSWORD env
	// var so it never lands in the ConfigMap.
	if vc.Spec.ReplicateFrom != nil {
		extPort := vc.Spec.ReplicateFrom.Port
		if extPort == 0 {
			extPort = 6379
		}
		tlsLine := ""
		if vc.Spec.ReplicateFrom.TLS {
			tlsLine = "echo \"tls-replication yes\" >> " + dataMountPath + "/runtime.conf\n"
		}
		// S4: when the source primary is signed by its own CA, build a combined
		// trust bundle (local CA + source CA) on the data PVC. renderValkeyConf
		// already points tls-ca-cert-file at this bundle (caCertPath), so the file
		// only needs to exist before valkey-server starts — the config-init
		// container completes first, guaranteeing that.
		caMergeLine := ""
		if sourceCAMergeEnabled(vc) {
			caMergeLine = fmt.Sprintf("cat %s/%s %s/%s > %s\n",
				tlsMountPath, secretKeyTLSCACert, sourceCAMountPath, secretKeyTLSCACert, caCertPath(vc))
		}
		return common + fmt.Sprintf(`echo "replicaof %[1]s %[2]d" >> %[3]s/runtime.conf
if [ -n "${SOURCE_PASSWORD:-}" ]; then
  echo "masterauth ${SOURCE_PASSWORD}" >> %[3]s/runtime.conf
fi
%[4]s%[5]s`, vc.Spec.ReplicateFrom.Host, extPort, dataMountPath, tlsLine, caMergeLine)
	}

	if vc.Spec.Topology == cachev1beta1.TopologyCluster {
		// The headless service that backs a pod's stable DNS differs by layout:
		// single-STS pods sit behind the one cluster headless; per-shard pods
		// (ADR 0005) sit behind their shard's headless, whose name equals the
		// pod's StatefulSet name = HOSTNAME with the trailing "-<ordinal>" removed
		// (${HOSTNAME%-*}). The cluster-announce-hostname MUST match the pod's real
		// DNS or cross-shard MOVED redirects and replica links resolve nowhere.
		svcPart := headless
		if perShardEnabled(vc) {
			svcPart = "${HOSTNAME%-*}"
		}
		return common + fmt.Sprintf(`ANNOUNCE_HOST="${HOSTNAME}.%[1]s.${POD_NAMESPACE}.svc.cluster.local"
{
  echo "cluster-announce-hostname ${ANNOUNCE_HOST}"
  echo "cluster-preferred-endpoint-type hostname"
  echo "cluster-announce-port %[2]d"
  echo "cluster-announce-bus-port %[3]d"
} >> %[4]s/runtime.conf
`, svcPart, port, clusterBusPort, dataMountPath)
	}

	return common + fmt.Sprintf(`ORDINAL="${HOSTNAME##*-}"
if [ "$ORDINAL" != "0" ]; then
  echo "replicaof %[1]s-0.%[2]s.${POD_NAMESPACE}.svc.cluster.local %[3]d" >> %[4]s/runtime.conf
fi
`, stsName, headless, port, dataMountPath)
}

// totalReplicas returns the spec-desired pod count: the value we want once
// the cluster has converged. Cluster topology computes it from
// shards*(1+replicasPerShard); others use Spec.Replicas directly.
func totalReplicas(vc *cachev1beta1.ValkeyCluster) int32 {
	if vc.Spec.Topology != cachev1beta1.TopologyCluster {
		return vc.Spec.Replicas
	}
	var shards, perShard int32
	if vc.Spec.Shards != nil {
		shards = *vc.Spec.Shards
	}
	if vc.Spec.ReplicasPerShard != nil {
		perShard = *vc.Spec.ReplicasPerShard
	}
	return shards * (1 + perShard)
}

// statefulSetReplicas returns the count we actually set on the StatefulSet.
// For Cluster topology during scale-down we hold the pod count at the
// previously-known size (Status.LastAppliedReplicas) until the operator's
// scale-down Job has reshard'ed slots away and del-node'd the leaving pods.
// Otherwise the StatefulSet controller would happily delete pods that still
// own slots, which is a data-loss bug.
func statefulSetReplicas(vc *cachev1beta1.ValkeyCluster) int32 {
	desired := totalReplicas(vc)
	if vc.Spec.Topology == cachev1beta1.TopologyCluster &&
		vc.Status.LastAppliedReplicas > desired {
		return vc.Status.LastAppliedReplicas
	}
	return desired
}

// updateStrategyFor selects the StatefulSet rollout strategy. OnDelete hands the
// rollout to the operator (ADR 0004 proactive failover); otherwise the STS
// controller drives a RollingUpdate.
func updateStrategyFor(proactive bool) appsv1.StatefulSetUpdateStrategy {
	if proactive {
		return appsv1.StatefulSetUpdateStrategy{Type: appsv1.OnDeleteStatefulSetStrategyType}
	}
	return appsv1.StatefulSetUpdateStrategy{Type: appsv1.RollingUpdateStatefulSetStrategyType}
}

func buildStatefulSet(vc *cachev1beta1.ValkeyCluster, configHash string, proactive bool) *appsv1.StatefulSet {
	labels := labelsFor(vc)
	replicas := statefulSetReplicas(vc)
	podAnnotations := map[string]string{}
	if configHash != "" {
		podAnnotations[configHashAnnotation] = configHash
	}
	// Restart request: propagate the CR's restart token into the pod template so
	// a new token rolls the StatefulSet (same mechanism as config-hash). The
	// value living in the template makes this idempotent — same token, no roll.
	if v := vc.Annotations[restartAnnotation]; v != "" {
		podAnnotations[restartedAtPodAnnotation] = v
	}
	port := valkeyPort
	if tlsEnabled(vc) {
		port = valkeyTLSPort
	}

	envVars := []corev1.EnvVar{
		{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
	}
	// Multi-region: surface the source primary's password to the init script
	// via env so it can compose masterauth without leaking through ConfigMap.
	if r := vc.Spec.ReplicateFrom; r != nil && r.PasswordSecret != nil {
		key := r.PasswordSecret.Key
		if key == "" {
			key = secretKeyPassword
		}
		envVars = append(envVars, corev1.EnvVar{
			Name: "SOURCE_PASSWORD",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: r.PasswordSecret.Name},
					Key:                  key,
				},
			},
		})
	}

	containerPorts := []corev1.ContainerPort{{
		Name:          appValkey,
		ContainerPort: port,
		Protocol:      corev1.ProtocolTCP,
	}}
	if vc.Spec.Topology == cachev1beta1.TopologyCluster {
		containerPorts = append(containerPorts, corev1.ContainerPort{
			Name:          "gossip",
			ContainerPort: clusterBusPort,
			Protocol:      corev1.ProtocolTCP,
		})
	}

	// Init containers: optional restore step (download an RDB from S3 onto
	// the data PVC) followed by the config-init step that generates the
	// per-pod runtime.conf. The restore step is idempotent — it skips when
	// /data/dump.rdb already exists, so a re-create of the pod doesn't
	// reset state, and re-restore requires manually deleting the file
	// from a debug pod.
	var initContainers []corev1.Container
	if vc.Spec.RestoreFrom != nil && vc.Spec.RestoreFrom.S3 != nil {
		initContainers = append(initContainers, buildRestoreInitContainer(vc))
	}
	configInitMounts := []corev1.VolumeMount{
		{Name: configVolumeName, MountPath: configMountPath, ReadOnly: true},
		{Name: dataVolumeName, MountPath: dataMountPath},
	}
	// S4: config-init builds the combined CA bundle, so it needs to read both the
	// local cluster CA (TLS Secret) and the source cluster CA (source-ca Secret).
	if sourceCAMergeEnabled(vc) {
		configInitMounts = append(configInitMounts,
			corev1.VolumeMount{Name: tlsVolumeName, MountPath: tlsMountPath, ReadOnly: true},
			corev1.VolumeMount{Name: sourceCAVolumeName, MountPath: sourceCAMountPath, ReadOnly: true},
		)
	}
	initContainers = append(initContainers, corev1.Container{
		Name:            "config-init",
		Image:           vc.Spec.Image,
		ImagePullPolicy: vc.Spec.ImagePullPolicy,
		Command:         []string{shellCmd, "-c", renderInitScript(vc)},
		Env:             envVars,
		VolumeMounts:    configInitMounts,
	})

	valkeyContainer := corev1.Container{
		Name:            appValkey,
		Image:           vc.Spec.Image,
		ImagePullPolicy: vc.Spec.ImagePullPolicy,
		Command:         []string{"valkey-server"},
		Args:            []string{dataMountPath + "/runtime.conf"},
		Env:             envVars,
		Ports:           containerPorts,
		Resources:       vc.Spec.Resources,
		VolumeMounts: []corev1.VolumeMount{
			{Name: configVolumeName, MountPath: configMountPath, ReadOnly: true},
			{Name: dataVolumeName, MountPath: dataMountPath},
		},
		ReadinessProbe: tcpProbe(port, 5, 5),
		LivenessProbe:  tcpProbe(port, 15, 20),
	}
	if tlsEnabled(vc) {
		valkeyContainer.VolumeMounts = append(valkeyContainer.VolumeMounts, corev1.VolumeMount{
			Name: tlsVolumeName, MountPath: tlsMountPath, ReadOnly: true,
		})
	}

	containers := []corev1.Container{valkeyContainer}
	if metricsEnabled(vc) {
		containers = append(containers, buildExporter(vc))
	}

	volumes := []corev1.Volume{
		{
			Name: configVolumeName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: configMapName(vc)},
					Items: []corev1.KeyToPath{
						{Key: valkeyConfFile, Path: valkeyConfFile},
					},
				},
			},
		},
	}
	if tlsEnabled(vc) {
		volumes = append(volumes, corev1.Volume{
			Name: tlsVolumeName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: tlsSecretName(vc)},
			},
		})
	}
	// S4: project the source cluster's CA Secret to a fixed file name
	// (<mount>/ca.crt) regardless of the configured key, so the init script's
	// merge command stays static.
	if sourceCAMergeEnabled(vc) {
		key := vc.Spec.ReplicateFrom.CASecret.Key
		if key == "" {
			key = secretKeyTLSCACert
		}
		volumes = append(volumes, corev1.Volume{
			Name: sourceCAVolumeName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: vc.Spec.ReplicateFrom.CASecret.Name,
					Items:      []corev1.KeyToPath{{Key: key, Path: secretKeyTLSCACert}},
				},
			},
		})
	}

	affinity := vc.Spec.Affinity
	if affinity == nil {
		affinity = defaultAntiAffinity(vc)
	}

	tsc := vc.Spec.TopologySpreadConstraints
	if len(tsc) == 0 {
		tsc = defaultTopologySpread(vc)
	}

	// Data dir: PVC by default. With storage.medium=Memory (Cache-only) it's a
	// tmpfs emptyDir instead — RAM-backed, so it counts against the pod memory
	// limit, NOT ephemeral-storage, and therefore does NOT trip the nodefs
	// eviction that makes plain disk-backed emptyDir unsafe. No PVC means
	// no StorageClass dependency, no PV-bind latency, no orphaned volumes on scale
	// — at the cost of data NOT surviving a pod restart (fine for Cache).
	var pvcs []corev1.PersistentVolumeClaim
	if usesMemoryStorage(vc) {
		volumes = append(volumes, corev1.Volume{
			Name: dataVolumeName,
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{
				Medium:    corev1.StorageMediumMemory,
				SizeLimit: memoryDataSizeLimit(vc),
			}},
		})
	} else {
		pvcs = []corev1.PersistentVolumeClaim{buildDataPVC(vc)}
	}

	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      statefulSetName(vc),
			Namespace: vc.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: headlessServiceName(vc),
			// Parallel pod management: with OrderedReady a single Pending pod
			// (e.g. its PVC stuck on a cordoned node) blocks every later pod and
			// all rolling updates / scale-ups for the whole set. Valkey tolerates
			// parallel start — replicas issue `replicaof pod-0` with retry, Cluster
			// pods are independent until bootstrap, Sentinels self-discover — so we
			// don't need strict ordinal ordering. (Immutable on existing STS;
			// pre-existing clusters keep OrderedReady until recreated.)
			PodManagementPolicy: appsv1.ParallelPodManagement,
			// UpdateStrategy: by default RollingUpdate (the STS controller bounces
			// pods on a template change). With proactive rolling restart (ADR 0004)
			// opted in we switch to OnDelete so the operator owns the rollout —
			// rolling replicas first and promoting a fresh replica BEFORE deleting
			// the old primary pod (see rollout.go), shrinking the config-rollout
			// unavailability window from the reactive ~15-20s to ~0.
			UpdateStrategy: updateStrategyFor(proactive),
			Selector:       &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: podAnnotations},
				Spec: corev1.PodSpec{
					InitContainers:            initContainers,
					Containers:                containers,
					Volumes:                   volumes,
					NodeSelector:              vc.Spec.NodeSelector,
					Tolerations:               vc.Spec.Tolerations,
					Affinity:                  affinity,
					TopologySpreadConstraints: tsc,
					SecurityContext: &corev1.PodSecurityContext{
						FSGroup:   ptr.To[int64](1000),
						RunAsUser: ptr.To[int64](1000),
					},
				},
			},
			VolumeClaimTemplates: pvcs,
		},
	}
}

func buildPDB(vc *cachev1beta1.ValkeyCluster) *policyv1.PodDisruptionBudget {
	maxU := intstr.FromInt(1)
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      vc.Name + "-pdb",
			Namespace: vc.Namespace,
			Labels:    labelsFor(vc),
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			Selector:       &metav1.LabelSelector{MatchLabels: labelsFor(vc)},
			MaxUnavailable: &maxU,
		},
	}
	if spec := vc.Spec.PodDisruptionBudget; spec != nil {
		if spec.MinAvailable != nil {
			pdb.Spec.MinAvailable = spec.MinAvailable
			pdb.Spec.MaxUnavailable = nil
		} else if spec.MaxUnavailable != nil {
			pdb.Spec.MaxUnavailable = spec.MaxUnavailable
		}
	}
	return pdb
}

func pdbEnabled(vc *cachev1beta1.ValkeyCluster) bool {
	if vc.Spec.PodDisruptionBudget == nil {
		return vc.Spec.Replicas > 1
	}
	return vc.Spec.PodDisruptionBudget.Enabled
}

func buildNetworkPolicy(vc *cachev1beta1.ValkeyCluster) *networkingv1.NetworkPolicy {
	ports := []networkingv1.NetworkPolicyPort{}
	proto := corev1.ProtocolTCP
	port := intstr.FromInt32(valkeyPort)
	if tlsEnabled(vc) {
		port = intstr.FromInt32(valkeyTLSPort)
	}
	ports = append(ports, networkingv1.NetworkPolicyPort{Protocol: &proto, Port: &port})
	if metricsEnabled(vc) {
		mp := intstr.FromInt32(exporterPort)
		ports = append(ports, networkingv1.NetworkPolicyPort{Protocol: &proto, Port: &mp})
	}

	peers := []networkingv1.NetworkPolicyPeer{}
	if vc.Spec.NetworkPolicy != nil && len(vc.Spec.NetworkPolicy.AllowFrom) > 0 {
		peers = vc.Spec.NetworkPolicy.AllowFrom
	} else {
		// Same-namespace peers by default (default-deny + scoped allow).
		peers = append(peers, networkingv1.NetworkPolicyPeer{
			PodSelector: &metav1.LabelSelector{},
		})
	}
	// Always allow Valkey-to-Valkey for replication and gossip.
	peers = append(peers, networkingv1.NetworkPolicyPeer{
		PodSelector: &metav1.LabelSelector{MatchLabels: labelsFor(vc)},
	})

	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      vc.Name + "-allow",
			Namespace: vc.Namespace,
			Labels:    labelsFor(vc),
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: labelsFor(vc)},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			Ingress: []networkingv1.NetworkPolicyIngressRule{{
				From:  peers,
				Ports: ports,
			}},
		},
	}
}

func networkPolicyEnabled(vc *cachev1beta1.ValkeyCluster) bool {
	return vc.Spec.NetworkPolicy != nil && vc.Spec.NetworkPolicy.Enabled
}

// buildRestoreInitContainer renders an aws-cli init container that downloads
// the configured S3 object onto the data PVC if /data/dump.rdb is missing.
// Valkey's normal startup will load the RDB on first run; after that
// dump.rdb is rewritten by RDB/AOF as usual, so the init is a no-op on
// subsequent restarts unless the file is removed by an operator.
func buildRestoreInitContainer(vc *cachev1beta1.ValkeyCluster) corev1.Container {
	r := vc.Spec.RestoreFrom
	image := r.AWSCLIImage
	if image == "" {
		image = defaultAWSCLIImage
	}
	endpointFlag := ""
	if r.S3.Endpoint != "" {
		endpointFlag = s3EndpointURLFlag
	}
	var script string
	if vc.Spec.Topology == cachev1beta1.TopologyCluster {
		// Cluster restore is per-shard: each master ordinal (0..shards-1) pulls
		// its own snapshot. SourceKey must contain a `{shard}` placeholder, which
		// we replace with the pod ordinal (derived from $HOSTNAME). Replica
		// ordinals (>= shards) skip the download — they resync from their master
		// once the cluster is (manually) assembled. See docs/runbook.md.
		shards := int32(0)
		if vc.Spec.Shards != nil {
			shards = *vc.Spec.Shards
		}
		script = fmt.Sprintf(`set -eu
if [ -f %[1]s/dump.rdb ]; then
  echo "dump.rdb already present; skipping restore"
  exit 0
fi
ORD="${HOSTNAME##*-}"
if [ "$ORD" -ge %[5]d ]; then
  echo "ordinal $ORD is a replica (>= %[5]d shards); skipping restore, will resync from master"
  exit 0
fi
KEY=$(echo "%[3]s" | sed "s/{shard}/$ORD/g")
echo "restoring shard $ORD from s3://%[2]s/$KEY"
aws s3 cp "s3://%[2]s/$KEY" %[1]s/dump.rdb%[4]s
echo "restore done: $(stat -c %%s %[1]s/dump.rdb 2>/dev/null || wc -c <%[1]s/dump.rdb) bytes"
`, dataMountPath, r.S3.Bucket, r.SourceKey, endpointFlag, shards)
	} else {
		script = fmt.Sprintf(`set -eu
if [ -f %[1]s/dump.rdb ]; then
  echo "dump.rdb already present; skipping restore"
  exit 0
fi
echo "restoring from s3://%[2]s/%[3]s"
aws s3 cp s3://%[2]s/%[3]s %[1]s/dump.rdb%[4]s
echo "restore done: $(stat -c %%s %[1]s/dump.rdb 2>/dev/null || wc -c <%[1]s/dump.rdb) bytes"
`, dataMountPath, r.S3.Bucket, r.SourceKey, endpointFlag)
	}

	env := []corev1.EnvVar{
		{
			Name: envAWSAccessKey,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: r.S3.CredentialsSecret},
					Key:                  envAWSAccessKey,
				},
			},
		},
		{
			Name: envAWSSecretKey,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: r.S3.CredentialsSecret},
					Key:                  envAWSSecretKey,
				},
			},
		},
		{Name: envAWSDefaultRegion, Value: r.S3.Region},
	}
	if r.S3.Endpoint != "" {
		env = append(env, corev1.EnvVar{Name: envS3EndpointURL, Value: r.S3.Endpoint})
	}
	return corev1.Container{
		Name:    "restore",
		Image:   image,
		Command: []string{shellCmd, "-c", script},
		Env:     env,
		VolumeMounts: []corev1.VolumeMount{
			{Name: dataVolumeName, MountPath: dataMountPath},
		},
	}
}

func defaultTopologySpread(vc *cachev1beta1.ValkeyCluster) []corev1.TopologySpreadConstraint {
	sel := &metav1.LabelSelector{MatchLabels: map[string]string{instanceLabel: vc.Name}}
	return []corev1.TopologySpreadConstraint{
		{
			MaxSkew:           1,
			TopologyKey:       "topology.kubernetes.io/zone",
			WhenUnsatisfiable: corev1.ScheduleAnyway,
			LabelSelector:     sel,
		},
	}
}

func buildExporter(vc *cachev1beta1.ValkeyCluster) corev1.Container {
	image := vc.Spec.Metrics.Image
	if image == "" {
		image = "oliver006/redis_exporter:v1.62.0"
	}
	scheme := "redis"
	if tlsEnabled(vc) {
		scheme = "rediss"
	}
	port := valkeyPort
	if tlsEnabled(vc) {
		port = valkeyTLSPort
	}
	env := []corev1.EnvVar{
		{Name: "REDIS_ADDR", Value: fmt.Sprintf("%s://localhost:%d", scheme, port)},
	}
	if tlsEnabled(vc) {
		// The exporter scrapes the Valkey listener over TLS (plaintext is
		// disabled when TLS is on). The server presents a cert-manager
		// self-signed cert; since the exporter talks to localhost inside the
		// same pod, skip verification rather than mounting the CA bundle.
		// Without this the exporter cannot connect and every redis_* series
		// disappears — silently breaking all data-plane alerts on TLS clusters.
		env = append(env, corev1.EnvVar{Name: "REDIS_EXPORTER_SKIP_TLS_VERIFICATION", Value: valueTrue})
	}
	if vc.Spec.Auth != nil && vc.Spec.Auth.Enabled {
		// Honor a user-supplied auth Secret: when auth.existingSecret is set the
		// operator never creates <name>-auth, so referencing it here leaves the
		// exporter container unable to start ("secret not found") — and since
		// it's a sidecar, the whole pod never becomes Ready. (Same handling as
		// the backup CronJob.)
		secretName := passwordSecretName(vc)
		if vc.Spec.Auth.ExistingSecret != "" {
			secretName = vc.Spec.Auth.ExistingSecret
		}
		env = append(env, corev1.EnvVar{
			Name: "REDIS_PASSWORD",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
					Key:                  secretKeyPassword,
				},
			},
		})
	}
	return corev1.Container{
		Name:  metricsPortName,
		Image: image,
		Env:   env,
		Ports: []corev1.ContainerPort{{Name: metricsPortName, ContainerPort: exporterPort}},
	}
}

// usesMemoryStorage reports whether the data dir is a tmpfs emptyDir (RAM)
// rather than a PVC (storage.medium=Memory; Cache-only ephemeral data).
func usesMemoryStorage(vc *cachev1beta1.ValkeyCluster) bool {
	return vc.Spec.Storage != nil && vc.Spec.Storage.Medium == "Memory"
}

// memoryDataSizeLimit bounds the tmpfs data dir. The data dir is near-empty for
// Cache (persistence directives off), so this is a guard rail, not a sizing knob;
// it caps how much RAM the tmpfs may consume before writes fail. Uses
// storage.size when set, else the small Cache default.
func memoryDataSizeLimit(vc *cachev1beta1.ValkeyCluster) *resource.Quantity {
	size := resource.MustParse(defaultCacheDataSize)
	if vc.Spec.Storage != nil && !vc.Spec.Storage.Size.IsZero() {
		size = vc.Spec.Storage.Size
	}
	return &size
}

func buildDataPVC(vc *cachev1beta1.ValkeyCluster) corev1.PersistentVolumeClaim {
	// Default size depends on profile: Cache needs only a small PVC (no persistence
	// writes to disk, but we still want a real volume to avoid ephemeral eviction).
	defSize := defaultDataSize
	if vc.Spec.Profile == cachev1beta1.ProfileCache {
		defSize = defaultCacheDataSize
	}
	size := resource.MustParse(defSize)
	var scName *string
	if vc.Spec.Storage != nil {
		if !vc.Spec.Storage.Size.IsZero() {
			size = vc.Spec.Storage.Size
		}
		scName = vc.Spec.Storage.StorageClassName
	}
	return corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: dataVolumeName, Labels: labelsFor(vc)},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: scName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: size},
			},
		},
	}
}

func defaultAntiAffinity(vc *cachev1beta1.ValkeyCluster) *corev1.Affinity {
	return &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{{
				Weight: 100,
				PodAffinityTerm: corev1.PodAffinityTerm{
					TopologyKey: topologyKeyHostname,
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{instanceLabel: vc.Name},
					},
				},
			}},
		},
	}
}

func tcpProbe(port int32, initial, period int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(port)},
		},
		InitialDelaySeconds: initial,
		PeriodSeconds:       period,
		TimeoutSeconds:      3,
		FailureThreshold:    5,
	}
}

func tlsEnabled(vc *cachev1beta1.ValkeyCluster) bool {
	return vc.Spec.TLS != nil && vc.Spec.TLS.Enabled
}

// sourceCAMergeEnabled reports whether this cluster pulls from an external TLS
// primary signed by a separate CA (S4). When true the operator merges that CA
// with the local cluster CA into one trust bundle, because Valkey reads a single
// global tls-ca-cert-file for both internal replication and the outbound link to
// the source. Requires local TLS (the pods need their own cert/key to speak
// tls-replication) — enforced by CEL and the webhook, re-checked here.
func sourceCAMergeEnabled(vc *cachev1beta1.ValkeyCluster) bool {
	r := vc.Spec.ReplicateFrom
	return r != nil && r.TLS && r.CASecret != nil && tlsEnabled(vc)
}

// caCertPath is the path Valkey's tls-ca-cert-file points at. Normally it's the
// cluster's own ca.crt from the TLS Secret; under S4 source-CA merge it's the
// combined bundle on the data PVC (local CA + source CA), built at pod init.
// Both renderValkeyConf and the live TLS reload (CONFIG SET tls-ca-cert-file)
// must agree on this, or a local cert renewal would reset trust off the bundle
// and silently break the source replication link.
func caCertPath(vc *cachev1beta1.ValkeyCluster) string {
	if sourceCAMergeEnabled(vc) {
		return dataMountPath + "/ca-bundle.crt"
	}
	return tlsMountPath + "/" + secretKeyTLSCACert
}

func metricsEnabled(vc *cachev1beta1.ValkeyCluster) bool {
	return vc.Spec.Metrics != nil && vc.Spec.Metrics.Enabled
}

func condStatus(ok bool) metav1.ConditionStatus {
	if ok {
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}

// setCondition upserts a condition with a stable readiness contract that
// Crossplane's provider-kubernetes (via kstatus) depends on:
//   - LastTransitionTime moves ONLY when Status actually flips, so a steady
//     condition does not churn its timestamp every reconcile (no flapping);
//   - Reason, Message and ObservedGeneration are always reconciled to the
//     latest values, so a spec bump that doesn't flip the condition still
//     advances the condition's observedGeneration (kstatus reads it to decide
//     whether the reported status is current).
//
// This is exactly the semantics of apimachinery's meta.SetStatusCondition;
// delegating keeps us aligned with the canonical, well-tested implementation
// instead of re-deriving the flip rules by hand.
func setCondition(conds *[]metav1.Condition, c metav1.Condition) {
	meta.SetStatusCondition(conds, c)
}
