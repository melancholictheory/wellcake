/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// podRef is a single Cluster data pod: its FQDN plus where it sits. shard is -1
// on the single-StatefulSet layout (no per-shard identity). This is the one
// source of truth for "the data pods of a Cluster", so callers don't hand-build
// FQDNs that assume a layout.
type podRef struct {
	host  string
	shard int32
	ord   int32
}

// clusterDataPods enumerates every Cluster data pod FQDN, hiding the
// single-StatefulSet vs per-shard layout difference (ADR 0005).
func clusterDataPods(vc *cachev1beta1.ValkeyCluster) []podRef {
	ns := vc.Namespace
	if perShardEnabled(vc) {
		perSts := 1 + replicasPerShardOf(vc)
		out := make([]podRef, 0, shardCountOf(vc)*perSts)
		for s := range shardCountOf(vc) {
			sts, hl := shardStsName(vc, s), shardHeadlessName(vc, s)
			for o := range perSts {
				out = append(out, podRef{
					host:  fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local", sts, o, hl, ns),
					shard: s, ord: o,
				})
			}
		}
		return out
	}
	sts, hl, total := statefulSetName(vc), headlessServiceName(vc), totalReplicas(vc)
	out := make([]podRef, 0, total)
	for i := range total {
		out = append(out, podRef{
			host:  fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local", sts, i, hl, ns),
			shard: -1, ord: i,
		})
	}
	return out
}

// clusterAnyPodHost returns one data pod FQDN to dial for cluster-wide queries
// (CLUSTER INFO/NODES) — pod-0 single-STS, or shard-0 pod-0 per-shard.
func clusterAnyPodHost(vc *cachev1beta1.ValkeyCluster) string {
	pods := clusterDataPods(vc)
	if len(pods) == 0 {
		return ""
	}
	return pods[0].host
}

// podFQDN builds the in-cluster FQDN for a pod given just its name. The headless
// Service backing the pod's DNS differs by layout: per-shard (ADR 0005) a pod
// "<cluster>-sh<i>-<ord>" sits behind headless "<cluster>-sh<i>" (its name minus
// the trailing "-<ord>"); otherwise behind the single cluster headless. Used by
// sites that learn pod NAMES from CLUSTER NODES and must dial them back.
func podFQDN(vc *cachev1beta1.ValkeyCluster, podName string) string {
	hl := headlessServiceName(vc)
	if perShardEnabled(vc) {
		if i := strings.LastIndex(podName, "-"); i > 0 {
			hl = podName[:i]
		}
	}
	return fmt.Sprintf("%s.%s.%s.svc.cluster.local", podName, hl, vc.Namespace)
}

// shardCountOf returns the configured shard count (0 if unset).
func shardCountOf(vc *cachev1beta1.ValkeyCluster) int32 {
	if vc.Spec.Shards != nil {
		return *vc.Spec.Shards
	}
	return 0
}

// ensureShardWorkload creates/updates the per-shard headless Services and
// StatefulSets and returns the total Ready pods across all shards (the
// readiness signal the single-STS path gets from one STS).
func (r *ValkeyClusterReconciler) ensureShardWorkload(ctx context.Context, vc *cachev1beta1.ValkeyCluster, configHash string) (int32, error) {
	proactive := proactiveRolloutEnabled(vc)
	var ready int32
	for s := range shardCountOf(vc) {
		if err := r.ensureShardHeadlessService(ctx, vc, s); err != nil {
			return 0, fmt.Errorf("shard %d headless service: %w", s, err)
		}
		sts, err := r.applyStatefulSet(ctx, vc, buildShardStatefulSet(vc, s, configHash, proactive))
		if err != nil {
			return 0, fmt.Errorf("shard %d statefulset: %w", s, err)
		}
		ready += sts.Status.ReadyReplicas
	}
	if err := r.ensurePVCSize(ctx, vc); err != nil {
		return 0, err
	}
	return ready, nil
}

func (r *ValkeyClusterReconciler) ensureShardHeadlessService(ctx context.Context, vc *cachev1beta1.ValkeyCluster, shard int32) error {
	desired := buildShardHeadlessService(vc, shard)
	if err := controllerutil.SetControllerReference(vc, desired, r.Scheme); err != nil {
		return err
	}
	return r.applyService(ctx, desired)
}

// oldShardsOf derives the shard count the cluster currently has from the
// applied pod count (LastAppliedReplicas = oldShards*(1+replicasPerShard)).
func oldShardsOf(vc *cachev1beta1.ValkeyCluster) int32 {
	perSts := 1 + replicasPerShardOf(vc)
	if perSts == 0 {
		return 0
	}
	return vc.Status.LastAppliedReplicas / perSts
}

// shardPodHosts returns a shard's pod FQDNs. Works for shards outside the
// CURRENT [0, shards) range too (leaving shards during scale-down), so callers
// can address pods the spec no longer wants.
func shardPodHosts(vc *cachev1beta1.ValkeyCluster, shard int32) []string {
	sts, hl := shardStsName(vc, shard), shardHeadlessName(vc, shard)
	n := 1 + replicasPerShardOf(vc)
	out := make([]string, n)
	for o := range n {
		out[o] = fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local", sts, o, hl, vc.Namespace)
	}
	return out
}

// buildShardScaleUpScript adds each NEW shard (master + replicas) to the cluster
// and rebalances slots onto the now-empty new masters (when autoReshard).
func buildShardScaleUpScript(vc *cachev1beta1.ValkeyCluster, port int32, tlsArgs, pwArg string) string {
	existingHost := shardPodHosts(vc, 0)[0]
	existing := fmt.Sprintf("%s:%d", existingHost, port)

	var b strings.Builder
	b.WriteString("#!/bin/sh\nset -eu\n")
	// in_cluster <fqdn> — is a node already a member? (idempotency: a retried Job
	// must not re-add a node, which errors "node is not empty").
	fmt.Fprintf(&b, "in_cluster() { valkey-cli%s%s -h %s -p %d cluster nodes | grep -q \"$1\"; }\n",
		pwArg, tlsArgs, existingHost, port)
	// Wait for every new shard pod to answer PING.
	for s := oldShardsOf(vc); s < shardCountOf(vc); s++ {
		for _, h := range shardPodHosts(vc, s) {
			fmt.Fprintf(&b, "until valkey-cli%s%s -h %s -p %d ping >/dev/null 2>&1; do echo waiting %s; sleep 2; done\n",
				pwArg, tlsArgs, h, port, h)
		}
	}
	for s := oldShardsOf(vc); s < shardCountOf(vc); s++ {
		hosts := shardPodHosts(vc, s)
		master := fmt.Sprintf("%s:%d", hosts[0], port)
		fmt.Fprintf(&b, "in_cluster %s || valkey-cli%s%s --cluster add-node %s %s\n", hosts[0], pwArg, tlsArgs, master, existing)
		for _, rep := range hosts[1:] {
			fmt.Fprintf(&b, "MID=$(valkey-cli%s%s -h %s -p %d cluster myid | tr -d '\\r')\n", pwArg, tlsArgs, hosts[0], port)
			fmt.Fprintf(&b, "in_cluster %s || valkey-cli%s%s --cluster add-node %s:%d %s --cluster-slave --cluster-master-id \"$MID\"\n",
				rep, pwArg, tlsArgs, rep, port, master)
		}
	}
	if vc.Spec.AutoReshard {
		b.WriteString(asmDetectSnippet(pwArg, tlsArgs, existingHost, port))
		fmt.Fprintf(&b, "want_masters=%d\n", shardCountOf(vc))
		fmt.Fprintf(&b,
			"for attempt in $(seq 1 12); do\n"+
				"  valkey-cli%[1]s%[2]s --cluster rebalance %[3]s --cluster-use-empty-masters$ASM_FLAG --cluster-yes || true\n"+
				"  size=$(valkey-cli%[1]s%[2]s -h %[4]s -p %[5]d cluster info | tr -d '\\r' | awk -F: '/^cluster_size:/{print $2}')\n"+
				"  [ \"${size:-0}\" -ge \"$want_masters\" ] && break\n  sleep 5\ndone\n",
			pwArg, tlsArgs, existing, existingHost, port)
	}
	return b.String()
}

// buildShardScaleDownScript reshards each LEAVING shard's slots onto shard-0's
// master, then del-nodes the leaving shard's pods. The ownership gate refuses to
// del-node a master that still owns slots (data-loss guard). The operator
// deletes the leaving shard StatefulSets after the Job succeeds.
func buildShardScaleDownScript(vc *cachev1beta1.ValkeyCluster, port int32, tlsArgs, pwArg string) string {
	hostBase := shardPodHosts(vc, 0)[0]

	var b strings.Builder
	b.WriteString("#!/bin/sh\nset -eu\n")
	fmt.Fprintf(&b, "KEEP_ID=$(valkey-cli%s%s -h %s -p %d cluster myid | tr -d '\\r')\n", pwArg, tlsArgs, hostBase, port)
	b.WriteString(asmDetectSnippet(pwArg, tlsArgs, hostBase, port))
	// Start from a clean slot configuration: a leftover open slot from an earlier
	// interrupted reshard makes the next `--cluster reshard` refuse to run ("Please
	// fix your cluster problems before resharding"). Close them up front.
	fmt.Fprintf(&b, "valkey-cli%s%s --cluster fix %s:%d --cluster-yes || true\n", pwArg, tlsArgs, hostBase, port)
	for s := shardCountOf(vc); s < oldShardsOf(vc); s++ {
		hosts := shardPodHosts(vc, s)
		master := hosts[0]
		fmt.Fprintf(&b, "echo '--- leaving shard %d ---'\n", s)
		fmt.Fprintf(&b, "L_ID=$(valkey-cli%s%s -h %s -p %d cluster myid | tr -d '\\r')\n", pwArg, tlsArgs, master, port)
		// Drain the leaving master's slots onto the kept master. A single reshard
		// of a live master can be interrupted (a MIGRATE leaves a slot in
		// importing/migrating state); retry, running `--cluster fix` to close any
		// open slots between attempts, until the leaving master owns nothing.
		fmt.Fprintf(&b, "for try in 1 2 3 4 5 6; do\n")
		fmt.Fprintf(&b, "  valkey-cli%s%s --cluster reshard %s:%d --cluster-from \"$L_ID\" --cluster-to \"$KEEP_ID\" --cluster-slots 16384$ASM_FLAG --cluster-yes || true\n",
			pwArg, tlsArgs, hostBase, port)
		fmt.Fprintf(&b, "  OWNED=$(valkey-cli%s%s -h %s -p %d cluster nodes | awk -v id=\"$L_ID\" '$1==id' | sed -e 's/.* connected//' -e 's/[[:space:]]//g')\n",
			pwArg, tlsArgs, hostBase, port)
		fmt.Fprintf(&b, "  [ -z \"$OWNED\" ] && break\n")
		fmt.Fprintf(&b, "  echo \"reshard incomplete (owns [$OWNED]); fixing open slots and retrying\"\n")
		fmt.Fprintf(&b, "  valkey-cli%s%s --cluster fix %s:%d --cluster-yes || true\n  sleep 3\ndone\n", pwArg, tlsArgs, hostBase, port)
		fmt.Fprintf(&b, "if [ -n \"$OWNED\" ]; then echo \"FATAL: shard %d master still owns slots [$OWNED]; refusing del-node\"; exit 1; fi\n", s)
		// del-node replicas first, then the (now slot-free) master.
		for _, rep := range hosts[1:] {
			fmt.Fprintf(&b, "RID=$(valkey-cli%s%s -h %s -p %d cluster myid | tr -d '\\r'); valkey-cli%s%s --cluster del-node %s:%d \"$RID\" || true\n",
				pwArg, tlsArgs, rep, port, pwArg, tlsArgs, hostBase, port)
		}
		fmt.Fprintf(&b, "valkey-cli%s%s --cluster del-node %s:%d \"$L_ID\"\n", pwArg, tlsArgs, hostBase, port)
	}
	return b.String()
}

// deleteLeavingShards removes the StatefulSet + headless Service of every shard
// the spec no longer wants ([shards, oldShards)). Called only after the
// scale-down Job has resharded their slots away and del-node'd their pods.
func (r *ValkeyClusterReconciler) deleteLeavingShards(ctx context.Context, vc *cachev1beta1.ValkeyCluster) error {
	for s := shardCountOf(vc); s < oldShardsOf(vc); s++ {
		sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: shardStsName(vc, s), Namespace: vc.Namespace}}
		if err := r.Delete(ctx, sts); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete shard %d sts: %w", s, err)
		}
		svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: shardHeadlessName(vc, s), Namespace: vc.Namespace}}
		if err := r.Delete(ctx, svc); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete shard %d svc: %w", s, err)
		}
	}
	return nil
}

// buildClusterOpJob wraps a cluster-management shell script into a one-shot Job
// (password env + TLS mount when configured). Shared by the per-shard scale
// up/down paths (and a natural home for the other cluster Jobs later).
func buildClusterOpJob(vc *cachev1beta1.ValkeyCluster, name, containerName, password, script string) *batchv1.Job {
	var env []corev1.EnvVar
	if password != "" {
		env = append(env, corev1.EnvVar{
			Name: envValkeyPassword,
			ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: passwordSecretName(vc)},
				Key:                  secretKeyPassword,
			}},
		})
	}
	var volumes []corev1.Volume
	var mounts []corev1.VolumeMount
	if tlsEnabled(vc) {
		volumes = append(volumes, corev1.Volume{
			Name:         tlsVolumeName,
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: tlsSecretName(vc)}},
		})
		mounts = append(mounts, corev1.VolumeMount{Name: tlsVolumeName, MountPath: tlsMountPath, ReadOnly: true})
	}
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: vc.Namespace, Labels: labelsFor(vc)},
		Spec: batchv1.JobSpec{
			BackoffLimit:            ptr.To[int32](3),
			TTLSecondsAfterFinished: ptr.To[int32](600),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labelsFor(vc)},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyOnFailure,
					Containers: []corev1.Container{{
						Name:         containerName,
						Image:        vc.Spec.Image,
						Command:      []string{shellCmd, "-c", script},
						Env:          env,
						VolumeMounts: mounts,
					}},
					Volumes: volumes,
				},
			},
		},
	}
}

// buildShardCreateCmds renders the per-shard cluster-formation shell: create a
// masters-only cluster from each shard's pod-0, then `add-node --cluster-slave`
// each shard's remaining pods as replicas of THAT shard's master — so the
// slot-owning master and its replicas live in the same shard STS (and thus,
// thanks to shard-aware anti-affinity, on different nodes).
func buildShardCreateCmds(vc *cachev1beta1.ValkeyCluster, pods []podRef, port int32, tlsArgs, pwArg string) string {
	masters := map[int32]string{}
	replicas := map[int32][]string{}
	for _, p := range pods {
		if p.ord == 0 {
			masters[p.shard] = p.host
		} else {
			replicas[p.shard] = append(replicas[p.shard], p.host)
		}
	}

	var b strings.Builder
	ms := make([]string, 0, shardCountOf(vc))
	for s := range shardCountOf(vc) {
		ms = append(ms, fmt.Sprintf("%s:%d", masters[s], port))
	}
	fmt.Fprintf(&b, "valkey-cli%s%s --cluster create %s --cluster-replicas 0 --cluster-yes\n",
		pwArg, tlsArgs, strings.Join(ms, " "))

	for s := range shardCountOf(vc) {
		for _, rep := range replicas[s] {
			fmt.Fprintf(&b, "MID=$(valkey-cli%s%s -h %s -p %d cluster myid)\n", pwArg, tlsArgs, masters[s], port)
			fmt.Fprintf(&b, "valkey-cli%s%s --cluster add-node %s:%d %s:%d --cluster-slave --cluster-master-id \"$MID\"\n",
				pwArg, tlsArgs, rep, port, masters[s], port)
		}
	}
	return b.String()
}

// shardLabel carries the shard index on a pod/STS so per-shard workloads can be
// selected and shard-aware anti-affinity expressed. Only set on the per-shard
// path (ADR 0005); absent on the single-StatefulSet layout.
const shardLabel = "cache.wellcake.io/shard"

// perShardEnabled reports whether this Cluster renders one StatefulSet per shard
// (ADR 0005, opt-in spec.perShardWorkload). Only meaningful for Cluster topology.
func perShardEnabled(vc *cachev1beta1.ValkeyCluster) bool {
	return vc.Spec.Topology == cachev1beta1.TopologyCluster &&
		vc.Spec.PerShardWorkload != nil && *vc.Spec.PerShardWorkload
}

func replicasPerShardOf(vc *cachev1beta1.ValkeyCluster) int32 {
	if vc.Spec.ReplicasPerShard != nil {
		return *vc.Spec.ReplicasPerShard
	}
	return 0
}

// shardStsName / shardHeadlessName name the per-shard StatefulSet and its
// headless Service: "<cluster>-sh<shard>". Pod DNS is then
// "<cluster>-sh<shard>-<ordinal>.<cluster>-sh<shard>.<ns>.svc.cluster.local".
func shardStsName(vc *cachev1beta1.ValkeyCluster, shard int32) string {
	return fmt.Sprintf("%s-sh%d", vc.Name, shard)
}

func shardHeadlessName(vc *cachev1beta1.ValkeyCluster, shard int32) string {
	return fmt.Sprintf("%s-sh%d", vc.Name, shard)
}

// shardLabelsFor returns the cluster labels plus the shard index.
func shardLabelsFor(vc *cachev1beta1.ValkeyCluster, shard int32) map[string]string {
	l := labelsFor(vc)
	l[shardLabel] = strconv.Itoa(int(shard))
	return l
}

// buildShardStatefulSet renders the StatefulSet for a SINGLE shard
// (1 primary + replicasPerShard replicas) by specialising the base single-STS
// template: per-shard name, headless service, replica count, shard label, and —
// the whole point of ADR 0005 — REQUIRED shard-aware anti-affinity that a single
// shared pod template cannot express. The container/volume/init spec is reused
// verbatim from buildStatefulSet so the two layouts stay in lockstep.
func buildShardStatefulSet(vc *cachev1beta1.ValkeyCluster, shard int32, configHash string, proactive bool) *appsv1.StatefulSet {
	sts := buildStatefulSet(vc, configHash, proactive)
	sl := shardLabelsFor(vc, shard)

	sts.Name = shardStsName(vc, shard)
	sts.Labels = sl
	sts.Spec.ServiceName = shardHeadlessName(vc, shard)
	sts.Spec.Replicas = ptr.To(1 + replicasPerShardOf(vc))
	sts.Spec.Selector = &metav1.LabelSelector{MatchLabels: sl}
	sts.Spec.Template.Labels = sl

	// Apply shard-aware anti-affinity unless the user pinned their own affinity.
	if vc.Spec.Affinity == nil {
		sts.Spec.Template.Spec.Affinity = shardAntiAffinity(vc, shard)
	}
	return sts
}

// shardAntiAffinity keeps a shard's primary+replicas off the same node
// (REQUIRED — a node failure must not take a whole shard with no replica to
// promote), while leaving DIFFERENT shards free to co-locate. Expressible only
// because each shard has its own pod template (its own STS) — the single-STS
// layout shares one template and so can only spread by instance, not by shard.
func shardAntiAffinity(vc *cachev1beta1.ValkeyCluster, shard int32) *corev1.Affinity {
	sel := &metav1.LabelSelector{MatchLabels: map[string]string{
		instanceLabel: vc.Name,
		shardLabel:    strconv.Itoa(int(shard)),
	}}
	return &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
				TopologyKey:   topologyKeyHostname,
				LabelSelector: sel,
			}},
		},
	}
}

// buildShardHeadlessService is the per-shard headless Service backing one shard's
// StatefulSet (stable pod DNS + gossip), scoped by the shard label.
func buildShardHeadlessService(vc *cachev1beta1.ValkeyCluster, shard int32) *corev1.Service {
	svc := buildHeadlessService(vc)
	sl := shardLabelsFor(vc, shard)
	svc.Name = shardHeadlessName(vc, shard)
	svc.Labels = sl
	svc.Spec.Selector = sl
	return svc
}
