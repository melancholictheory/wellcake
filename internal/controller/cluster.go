/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// reconcileCluster brings up Cluster-topology objects: shared Services, ConfigMap,
// StatefulSet (replicas = shards * (1+replicasPerShard)) and a one-shot bootstrap
// Job running `valkey-cli --cluster create` once all pods are Ready.
func (r *ValkeyClusterReconciler) reconcileCluster(ctx context.Context, vc *cachev1beta1.ValkeyCluster) (ctrl.Result, error) {
	if vc.Spec.Shards == nil || *vc.Spec.Shards < 3 {
		return r.setPhase(ctx, vc,
			"InvalidSpec", "shards must be set to at least 3 for Cluster topology")
	}

	password, ready, err := r.ensureClusterWorkload(ctx, vc)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Scheduled backups: the Cluster dump script fans out per shard (see backup.go),
	// but reconcileCluster previously never created the CronJob — so a Cluster with
	// backup.enabled silently never backed up. Ensure it here like the other paths.
	if err := r.ensureBackupCronJob(ctx, vc); err != nil {
		return ctrl.Result{}, fmt.Errorf("backup cronjob: %w", err)
	}

	want := totalReplicas(vc)
	allReady := ready == want && want > 0

	// Bootstrap is a one-time operation; once status says initialized, never re-run.
	if !vc.Status.ClusterInitialized && allReady {
		result, err := r.bootstrapOrRestore(ctx, vc, password)
		if err != nil || !result.IsZero() {
			return result, err
		}
	}

	// Scale-up: if the spec asks for more replicas than the cluster currently
	// knows about (Status.LastAppliedReplicas), and all desired pods are
	// already Ready, run a one-shot add-node Job. Slot rebalance is gated
	// by spec.autoReshard.
	if vc.Status.ClusterInitialized && allReady && vc.Status.LastAppliedReplicas < want {
		result, err := r.runClusterScaleUp(ctx, vc, password)
		if err != nil || !result.IsZero() {
			return result, err
		}
	}

	// Scale-down: if the spec wants fewer replicas than the cluster currently
	// has (Status.LastAppliedReplicas), run the scale-down Job that reshards
	// slots away from the leaving masters and del-nodes them. The StatefulSet
	// is held at the old size by statefulSetReplicas() until this Job
	// succeeds — otherwise pods owning slots would be deleted and lose data.
	if vc.Status.ClusterInitialized && vc.Status.LastAppliedReplicas > want {
		result, err := r.runClusterScaleDown(ctx, vc, password)
		if err != nil || !result.IsZero() {
			return result, err
		}
	}

	// Manual reshard request (valkey.wellcake.io/reshard): run a one-off rebalance
	// once per distinct token. Only when the cluster is initialized and all
	// pods are Ready so the rebalance sees a stable membership.
	if vc.Status.ClusterInitialized && allReady {
		if tok := vc.Annotations[reshardAnnotation]; tok != "" && tok != vc.Status.LastReshardToken {
			result, err := r.runClusterReshard(ctx, vc, password, tok)
			if err != nil || !result.IsZero() {
				return result, err
			}
		}
	}

	// Proactive rolling restart (ADR 0004): when opted in and the cluster is in
	// steady state, the operator drives a config rollout itself under OnDelete —
	// rolling each shard's replicas first, then CLUSTER FAILOVER to a fresh
	// replica before restarting the old master pod, so every slot stays served.
	if res, handled, rerr := r.maybeDriveClusterRollout(ctx, vc, password, want); handled {
		return res, rerr
	}

	// Per-shard observation: once initialized, run CLUSTER NODES from pod-0
	// and store the parsed view in status.ShardDetails. We requeue every 30s
	// so the status stays fresh — cluster membership only changes on
	// bootstrap/scale/failover, so this is cheap.
	var shardDetails []cachev1beta1.ShardStatus
	if vc.Status.ClusterInitialized && allReady {
		shardDetails = r.surveyCluster(ctx, vc, password)
	}

	res, err := r.updateClusterStatus(ctx, vc, ready, allReady, shardDetails)
	if err == nil && vc.Status.ClusterInitialized {
		res.RequeueAfter = 30 * time.Second
	}
	return res, err
}

// maybeDriveClusterRollout advances the proactive rolling restart (ADR 0004) one
// step when opted in and the cluster is in steady state (initialized, not
// mid-scale). Returns handled=true (with a fast requeue) while a rollout is in
// flight so the caller skips the steady-state survey/requeue; handled=false when
// there is nothing to roll and normal reconciliation should continue.
func (r *ValkeyClusterReconciler) maybeDriveClusterRollout(
	ctx context.Context, vc *cachev1beta1.ValkeyCluster, password string, want int32,
) (ctrl.Result, bool, error) {
	if !proactiveRolloutEnabled(vc) || !vc.Status.ClusterInitialized || vc.Status.LastAppliedReplicas != want {
		return ctrl.Result{}, false, nil
	}
	rolling, err := r.driveClusterRollout(ctx, vc, password)
	if err != nil {
		return ctrl.Result{}, true, err
	}
	if rolling {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, true, nil
	}
	return ctrl.Result{}, false, nil
}

// ensureClusterWorkload reconciles the static prerequisites for a Cluster-topology
// object — password Secret, headless+client Services, ConfigMap, StatefulSet, PDB
// and NetworkPolicy — and returns the resolved password plus the live StatefulSet.
// Split out of reconcileCluster to keep that function's branching tractable.
// ensureClusterWorkload provisions the Cluster data plane and returns the total
// number of Ready data pods. The single-StatefulSet layout reads it from the one
// STS; the per-shard layout (ADR 0005, spec.perShardWorkload) creates one STS +
// headless Service per shard and sums their Ready counts.
func (r *ValkeyClusterReconciler) ensureClusterWorkload(ctx context.Context, vc *cachev1beta1.ValkeyCluster) (string, int32, error) {
	password, err := r.ensurePasswordSecret(ctx, vc)
	if err != nil {
		return "", 0, fmt.Errorf("password secret: %w", err)
	}
	// Cluster-wide client Service (selects every pod by instance label) is shared
	// by both layouts.
	if err := r.ensureClientService(ctx, vc); err != nil {
		return "", 0, fmt.Errorf("client service: %w", err)
	}
	configHash, err := r.ensureConfigMap(ctx, vc, password)
	if err != nil {
		return "", 0, fmt.Errorf("configmap: %w", err)
	}

	var ready int32
	if perShardEnabled(vc) {
		// Per-shard headless Services are created inside ensureShardWorkload; the
		// single cluster-wide headless Service is not needed.
		ready, err = r.ensureShardWorkload(ctx, vc, configHash)
		if err != nil {
			return "", 0, fmt.Errorf("shard workload: %w", err)
		}
	} else {
		if err := r.ensureHeadlessService(ctx, vc); err != nil {
			return "", 0, fmt.Errorf("headless service: %w", err)
		}
		sts, err := r.ensureStatefulSet(ctx, vc, configHash)
		if err != nil {
			return "", 0, fmt.Errorf("statefulset: %w", err)
		}
		ready = sts.Status.ReadyReplicas
	}

	if err := r.ensurePDB(ctx, vc); err != nil {
		return "", 0, fmt.Errorf("pdb: %w", err)
	}
	if err := r.ensureNetworkPolicy(ctx, vc); err != nil {
		return "", 0, fmt.Errorf("networkpolicy: %w", err)
	}
	return password, ready, nil
}

// bootstrapOrRestore picks the first-time cluster-formation path: the guided
// restore path when spec.restoreFrom is set (data PVCs already hold restored
// RDBs, so `--cluster create` would abort on non-empty nodes), otherwise the
// normal `--cluster create` bootstrap Job.
func (r *ValkeyClusterReconciler) bootstrapOrRestore(ctx context.Context, vc *cachev1beta1.ValkeyCluster, password string) (ctrl.Result, error) {
	if vc.Spec.RestoreFrom != nil {
		return r.reconcileClusterRestore(ctx, vc, password)
	}
	return r.runClusterBootstrap(ctx, vc, password)
}

// reconcileClusterRestore handles the bootstrap phase when spec.restoreFrom is
// set on a Cluster topology. We must NOT run `valkey-cli --cluster create`:
// each master's data PVC already holds a restored RDB (placed by the per-shard
// restore init container), and `--cluster create` aborts on non-empty nodes.
// Instead the operator runs an assembly Job that reconstructs the cluster from
// the backup's slot manifest (CLUSTER ADDSLOTSRANGE / SET-CONFIG-EPOCH / MEET /
// REPLICATE), so the SAME slot→shard map is restored (C2). It then adopts the
// cluster (flips ClusterInitialized) once it reports cluster_state:ok.
func (r *ValkeyClusterReconciler) reconcileClusterRestore(ctx context.Context, vc *cachev1beta1.ValkeyCluster, password string) (ctrl.Result, error) {
	return r.runClusterRestoreAssembly(ctx, vc, password)
}

// adoptFormedCluster flips Status.ClusterInitialized for a cluster that is
// already formed on the data plane — used both to detect a manual restore
// assembly and (AR2) to re-adopt a live cluster whose status flag was lost,
// instead of re-running `--cluster create`.
func (r *ValkeyClusterReconciler) adoptFormedCluster(ctx context.Context, vc *cachev1beta1.ValkeyCluster, reason, msg string) (ctrl.Result, error) {
	patch := client.MergeFrom(vc.DeepCopy())
	vc.Status.ClusterInitialized = true
	vc.Status.LastAppliedReplicas = totalReplicas(vc)
	setCondition(&vc.Status.Conditions, metav1.Condition{
		Type:               cachev1beta1.ConditionClusterInitialized,
		Status:             metav1.ConditionTrue,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: vc.Generation,
		LastTransitionTime: metav1.Now(),
	})
	return ctrl.Result{}, r.Status().Patch(ctx, vc, patch)
}

// clusterFormed reports whether pod-0 sees a fully-formed cluster
// (cluster_state:ok with all 16384 slots assigned). Used to detect completion
// of a manual cluster-restore assembly without ever running `--cluster create`.
func (r *ValkeyClusterReconciler) clusterFormed(ctx context.Context, vc *cachev1beta1.ValkeyCluster, password string) bool {
	port := valkeyPort
	if tlsEnabled(vc) {
		port = valkeyTLSPort
	}
	host := clusterAnyPodHost(vc)
	c := dialReplClient(ctx, host, port, password, tlsEnabled(vc), loadMTLSClientCert(ctx, r, vc), 3*time.Second)
	if c == nil {
		return false
	}
	defer c.close()
	out, err := c.rdb.Do(ctx, "CLUSTER", "INFO").Text()
	if err != nil {
		return false
	}
	return strings.Contains(out, "cluster_state:ok") &&
		strings.Contains(out, "cluster_slots_assigned:16384")
}

// runClusterBootstrap creates an idempotent Job that runs `valkey-cli --cluster create`
// against all pods. The Job stays around (TTLSecondsAfterFinished=600) so users can
// inspect it; on Succeeded we flip Status.ClusterInitialized and stop trying.
func (r *ValkeyClusterReconciler) runClusterBootstrap(ctx context.Context, vc *cachev1beta1.ValkeyCluster, password string) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// AR2: idempotency keyed on the actual data plane, not just the status flag.
	// If the cluster is already formed (e.g. Status.ClusterInitialized was lost —
	// CR re-applied without status, subresource wiped, restore of an old CR),
	// re-adopt it instead of running `--cluster create` again. `--cluster create`
	// on already-clustered nodes would fail (BootstrapFailed); never re-bootstrap
	// a live cluster.
	if r.clusterFormed(ctx, vc, password) {
		log.Info("existing formed cluster detected, adopting (status flag was missing)", "name", vc.Name)
		return r.adoptFormedCluster(ctx, vc, "AdoptedExisting",
			"cluster already formed on the data plane; adopted without re-bootstrap")
	}

	name := vc.Name + "-bootstrap"
	key := client.ObjectKey{Namespace: vc.Namespace, Name: name}

	var existing batchv1.Job
	err := r.Get(ctx, key, &existing)
	if apierrors.IsNotFound(err) {
		job := buildBootstrapJob(vc, password, name)
		if err := controllerutil.SetControllerReference(vc, job, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		log.Info("creating cluster bootstrap job", "name", name)
		return ctrl.Result{}, r.Create(ctx, job)
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	switch {
	case existing.Status.Succeeded > 0:
		log.Info("cluster bootstrap job succeeded", "name", name)
		bootstrapTotal.WithLabelValues(vc.Namespace, vc.Name, "success").Inc()
		// Flip the condition; the outer reconcile will pick up the flag next pass.
		patch := client.MergeFrom(vc.DeepCopy())
		vc.Status.ClusterInitialized = true
		vc.Status.LastAppliedReplicas = totalReplicas(vc)
		setCondition(&vc.Status.Conditions, metav1.Condition{
			Type:               cachev1beta1.ConditionClusterInitialized,
			Status:             metav1.ConditionTrue,
			Reason:             "BootstrapSucceeded",
			Message:            fmt.Sprintf("Job %s completed", name),
			ObservedGeneration: vc.Generation,
			LastTransitionTime: metav1.Now(),
		})
		return ctrl.Result{}, r.Status().Patch(ctx, vc, patch)
	case existing.Status.Failed > *existing.Spec.BackoffLimit:
		bootstrapTotal.WithLabelValues(vc.Namespace, vc.Name, "failure").Inc()
		return r.setPhase(ctx, vc,
			"BootstrapFailed", fmt.Sprintf("Job %s failed; inspect kubectl logs job/%s", name, name))
	default:
		// Still running; come back later. The Job pod state-change will requeue us via Owns().
		return ctrl.Result{}, nil
	}
}

// buildBootstrapJob renders a one-shot Job that runs `valkey-cli --cluster create`.
// The Job has a TTL so a successful run is cleaned up automatically.
func buildBootstrapJob(vc *cachev1beta1.ValkeyCluster, password, name string) *batchv1.Job {
	labels := labelsFor(vc)
	port := valkeyPort
	tlsArgs := ""
	if tlsEnabled(vc) {
		port = valkeyTLSPort
		tlsArgs = fmt.Sprintf(" --tls --cert %s/tls.crt --key %s/tls.key --cacert %s/ca.crt --insecure", tlsMountPath, tlsMountPath, tlsMountPath)
	}

	// Data pod FQDNs, layout-agnostic (single-STS or per-shard, ADR 0005).
	pods := clusterDataPods(vc)

	pwArg := ""
	if password != "" {
		pwArg = noAuthWarningArgs
	}

	// Pre-flight: wait until every pod answers PING. Without this the Job races the
	// StatefulSet rollout and `--cluster create` reports "node not ready".
	waitCmd := strings.Builder{}
	waitCmd.WriteString("for n in")
	for _, p := range pods {
		waitCmd.WriteString(" ")
		waitCmd.WriteString(p.host)
	}
	waitCmd.WriteString("; do\n")
	waitCmd.WriteString("  until valkey-cli")
	if password != "" {
		waitCmd.WriteString(noAuthWarningArgs)
	}
	waitCmd.WriteString(tlsArgs)
	fmt.Fprintf(&waitCmd, " -h $n -p %d ping >/dev/null 2>&1; do\n", port)
	waitCmd.WriteString("    echo waiting for $n; sleep 2;\n")
	waitCmd.WriteString("  done\n")
	waitCmd.WriteString("done\n")

	var clusterCmds string
	if perShardEnabled(vc) {
		// Per-shard: --cluster-replicas can't honour our shard→STS grouping (it
		// pairs by host arbitrarily), so create a masters-only cluster from each
		// shard's pod-0, then attach each shard's replicas explicitly.
		clusterCmds = buildShardCreateCmds(vc, pods, port, tlsArgs, pwArg)
	} else {
		nodes := make([]string, len(pods))
		for i, p := range pods {
			nodes[i] = fmt.Sprintf("%s:%d", p.host, port)
		}
		clusterCmds = fmt.Sprintf("valkey-cli%s%s --cluster create %s --cluster-replicas %d --cluster-yes\n",
			pwArg, tlsArgs, strings.Join(nodes, " "), replicasPerShardOf(vc))
	}

	script := "#!/bin/sh\nset -eu\n" + waitCmd.String() + clusterCmds

	env := []corev1.EnvVar{}
	if password != "" {
		env = append(env, corev1.EnvVar{
			Name: envValkeyPassword,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: passwordSecretName(vc)},
					Key:                  secretKeyPassword,
				},
			},
		})
	}

	volumes := []corev1.Volume{}
	mounts := []corev1.VolumeMount{}
	if tlsEnabled(vc) {
		volumes = append(volumes, corev1.Volume{
			Name:         tlsVolumeName,
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: tlsSecretName(vc)}},
		})
		mounts = append(mounts, corev1.VolumeMount{Name: tlsVolumeName, MountPath: tlsMountPath, ReadOnly: true})
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: vc.Namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            ptr.To[int32](3),
			TTLSecondsAfterFinished: ptr.To[int32](600),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyOnFailure,
					Containers: []corev1.Container{{
						Name:         "bootstrap",
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

// runClusterScaleUp runs a one-shot Job that calls `valkey-cli --cluster
// add-node` for every pod ordinal in [LastAppliedReplicas, totalReplicas).
// The Job is idempotent across re-execution because add-node is a no-op
// against an already-known node, but we still gate it with LastAppliedReplicas
// to avoid running it on every reconcile.
//
// Slot rebalance after add-node is gated by spec.autoReshard (default true):
// with it on, new masters get their share of slots automatically; with it off,
// the operator only adds empty masters and distributing slots is left as a
// deliberate `valkey-cli --cluster reshard` step.
func (r *ValkeyClusterReconciler) runClusterScaleUp(ctx context.Context, vc *cachev1beta1.ValkeyCluster, password string) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	name := vc.Name + "-scaleup"
	key := client.ObjectKey{Namespace: vc.Namespace, Name: name}

	var existing batchv1.Job
	err := r.Get(ctx, key, &existing)
	if apierrors.IsNotFound(err) {
		job := buildScaleUpJob(vc, password, name)
		if err := controllerutil.SetControllerReference(vc, job, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		log.Info("creating cluster scale-up job", "name", name,
			"from", vc.Status.LastAppliedReplicas, "to", totalReplicas(vc))
		return ctrl.Result{}, r.Create(ctx, job)
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	switch {
	case existing.Status.Succeeded > 0:
		log.Info("cluster scale-up job succeeded", "name", name)
		scaleEventsTotal.WithLabelValues(vc.Namespace, vc.Name, "up", "success").Inc()
		patch := client.MergeFrom(vc.DeepCopy())
		vc.Status.LastAppliedReplicas = totalReplicas(vc)
		if err := r.Status().Patch(ctx, vc, patch); err != nil {
			return ctrl.Result{}, err
		}
		// Delete the Job so the next scale-up gets a clean slate. TTL would
		// also work, but explicit delete makes the next reconcile predictable.
		return ctrl.Result{}, r.Delete(ctx, &existing)
	case existing.Status.Failed > *existing.Spec.BackoffLimit:
		scaleEventsTotal.WithLabelValues(vc.Namespace, vc.Name, "up", "failure").Inc()
		return r.setPhase(ctx, vc,
			"ScaleUpFailed", fmt.Sprintf("Job %s failed; inspect kubectl logs job/%s", name, name))
	default:
		return ctrl.Result{}, nil
	}
}

// buildScaleUpJob renders a Job that calls add-node for every pod in
// [LastAppliedReplicas, totalReplicas).
// asmDetectSnippet emits a shell prelude that sets ASM_FLAG to
// " --cluster-use-atomic-slot-migration" when the running Valkey is >= 9.1, else
// empty (ADR 0001 / C3). Appending $ASM_FLAG to a `valkey-cli --cluster
// rebalance/reshard` call makes 9.1+ move whole slot ranges via Atomic Slot
// Migration (CLUSTER MIGRATESLOTS — snapshot+stream+atomic cutover) instead of
// the classic interruptible key-by-key MIGRATE that can leave a slot
// half-migrated on interruption (root cause of the C3/C4 scale-down fragility).
// The version is detected at runtime from INFO server (robust to the image-tag
// form); < 9.1 keeps the classic, proven path. host may be a shell variable
// (e.g. "$HOST") since it is interpolated verbatim into -h.
func asmDetectSnippet(pwArg, tlsArgs, host string, port int32) string {
	return fmt.Sprintf("ASM_FLAG=\"\"\n"+
		"__ver=$(valkey-cli%[1]s%[2]s -h %[3]s -p %[4]d INFO server 2>/dev/null | sed -n 's/^valkey_version:\\([0-9]*\\.[0-9]*\\).*/\\1/p' | tr -d '\\r')\n"+
		"__maj=${__ver%%%%.*}; __min=${__ver#*.}\n"+
		"if [ \"${__maj:-0}\" -gt 9 ] 2>/dev/null || { [ \"${__maj:-0}\" -eq 9 ] && [ \"${__min:-0}\" -ge 1 ]; } 2>/dev/null; then\n"+
		"  ASM_FLAG=\" --cluster-use-atomic-slot-migration\"\n"+
		"  echo \"valkey ${__ver} >= 9.1: resharding via atomic slot migration (ASM)\"\n"+
		"fi\n",
		pwArg, tlsArgs, host, port)
}

func buildScaleUpJob(vc *cachev1beta1.ValkeyCluster, password, name string) *batchv1.Job {
	labels := labelsFor(vc)
	port := valkeyPort
	tlsArgs := ""
	if tlsEnabled(vc) {
		port = valkeyTLSPort
		tlsArgs = fmt.Sprintf(" --tls --cert %s/tls.crt --key %s/tls.key --cacert %s/ca.crt --insecure", tlsMountPath, tlsMountPath, tlsMountPath)
	}

	pwArg := ""
	if password != "" {
		pwArg = noAuthWarningArgs
	}

	var sb strings.Builder
	if perShardEnabled(vc) {
		sb.WriteString(buildShardScaleUpScript(vc, port, tlsArgs, pwArg))
		return buildClusterOpJob(vc, name, "scaleup", password, sb.String())
	}

	headless := headlessServiceName(vc)
	stsName := statefulSetName(vc)
	existingHost := fmt.Sprintf("%s-0.%s.%s.svc.cluster.local", stsName, headless, vc.Namespace)
	existing := fmt.Sprintf("%s:%d", existingHost, port)

	sb.WriteString("#!/bin/sh\nset -eu\n")
	for i := vc.Status.LastAppliedReplicas; i < totalReplicas(vc); i++ {
		newNode := fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local:%d", stsName, i, headless, vc.Namespace, port)
		fmt.Fprintf(&sb,
			"echo 'adding %s'; valkey-cli%s%s --cluster add-node %s %s\n",
			newNode, pwArg, tlsArgs, newNode, existing)
	}
	// AutoReshard: rebalance slots equally across all (including newly-added)
	// masters. --cluster-use-empty-masters lets the rebalance assign slots to
	// brand-new masters that currently hold zero slots.
	//
	// The rebalance must not run until the freshly added empty masters are
	// recognised cluster-wide. add-node sends CLUSTER MEET but gossip
	// propagation is asynchronous; if rebalance runs while the entry node
	// still sees only the original masters, --cluster-use-empty-masters finds
	// no empty masters to fill and reports "No rebalancing needed", silently
	// leaving the new nodes without slots (autoReshard becomes a no-op — the
	// XR converges to Ready but the cluster never actually reshards). So loop:
	// rebalance, then check cluster_size (== masters that own slots); once it
	// reaches the desired shard count every new master has slots and we stop.
	if vc.Spec.AutoReshard {
		wantMasters := *vc.Spec.Shards
		sb.WriteString(asmDetectSnippet(pwArg, tlsArgs, existingHost, port))
		fmt.Fprintf(&sb, "want_masters=%d\n", wantMasters)
		fmt.Fprintf(&sb,
			"for attempt in $(seq 1 12); do\n"+
				"  echo \"rebalance attempt $attempt\"\n"+
				"  valkey-cli%[1]s%[2]s --cluster rebalance %[3]s --cluster-use-empty-masters$ASM_FLAG --cluster-yes || true\n"+
				"  size=$(valkey-cli%[1]s%[2]s -h %[4]s -p %[5]d cluster info | tr -d '\\r' | awk -F: '/^cluster_size:/{print $2}')\n"+
				"  size=${size:-0}\n"+
				"  echo \"cluster_size=$size want=$want_masters\"\n"+
				"  [ \"$size\" -ge \"$want_masters\" ] && { echo 'rebalance complete'; break; }\n"+
				"  sleep 5\n"+
				"done\n",
			pwArg, tlsArgs, existing, existingHost, port)
	}

	env := []corev1.EnvVar{}
	if password != "" {
		env = append(env, corev1.EnvVar{
			Name: envValkeyPassword,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: passwordSecretName(vc)},
					Key:                  secretKeyPassword,
				},
			},
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
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: vc.Namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            ptr.To[int32](3),
			TTLSecondsAfterFinished: ptr.To[int32](600),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyOnFailure,
					Containers: []corev1.Container{{
						Name:         "scaleup",
						Image:        vc.Spec.Image,
						Command:      []string{shellCmd, "-c", sb.String()},
						Env:          env,
						VolumeMounts: mounts,
					}},
					Volumes: volumes,
				},
			},
		},
	}
}

// runClusterScaleDown runs a Job that, for every leaving pod ordinal,
// reshards its slots away to a remaining master (if any) and del-nodes the
// pod from the cluster. On success Status.LastAppliedReplicas is decremented
// to totalReplicas(spec); the next reconcile then lets statefulSetReplicas()
// return the smaller value and the StatefulSet controller actually shrinks
// the pods.
//
// Reshard-then-delete is deliberate: deleting a master first while it still
// owns slots leaves the cluster missing those slots — Cache profile would
// keep operating in degraded mode (cluster-require-full-coverage no), but
// Durable would refuse writes.
func (r *ValkeyClusterReconciler) runClusterScaleDown(ctx context.Context, vc *cachev1beta1.ValkeyCluster, password string) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	name := vc.Name + "-scaledown"
	key := client.ObjectKey{Namespace: vc.Namespace, Name: name}

	var existing batchv1.Job
	err := r.Get(ctx, key, &existing)
	if apierrors.IsNotFound(err) {
		job := buildScaleDownJob(vc, password, name)
		if err := controllerutil.SetControllerReference(vc, job, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		log.Info("creating cluster scale-down job", "name", name,
			"from", vc.Status.LastAppliedReplicas, "to", totalReplicas(vc))
		return ctrl.Result{}, r.Create(ctx, job)
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	switch {
	case existing.Status.Succeeded > 0:
		log.Info("cluster scale-down job succeeded", "name", name)
		scaleEventsTotal.WithLabelValues(vc.Namespace, vc.Name, "down", "success").Inc()
		// Per-shard: the leaving shards' slots are resharded away and their pods
		// del-node'd by the Job — now delete their StatefulSets/Services. MUST run
		// before LastAppliedReplicas is lowered (deleteLeavingShards derives the
		// leaving range from it).
		if perShardEnabled(vc) {
			if err := r.deleteLeavingShards(ctx, vc); err != nil {
				return ctrl.Result{}, err
			}
		}
		patch := client.MergeFrom(vc.DeepCopy())
		vc.Status.LastAppliedReplicas = totalReplicas(vc)
		if err := r.Status().Patch(ctx, vc, patch); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.Delete(ctx, &existing)
	case existing.Status.Failed > *existing.Spec.BackoffLimit:
		scaleEventsTotal.WithLabelValues(vc.Namespace, vc.Name, "down", "failure").Inc()
		return r.setPhase(ctx, vc,
			"ScaleDownFailed", fmt.Sprintf("Job %s failed; inspect kubectl logs job/%s", name, name))
	default:
		return ctrl.Result{}, nil
	}
}

// buildScaleDownJob renders the reshard-away + del-node script for every
// ordinal in [totalReplicas, LastAppliedReplicas). The reshard step uses
// --cluster-slots 16384 as an upper bound — valkey-cli stops when the
// source runs out, so this safely covers both small and full masters.
func buildScaleDownJob(vc *cachev1beta1.ValkeyCluster, password, name string) *batchv1.Job {
	labels := labelsFor(vc)
	port := valkeyPort
	tlsArgs := ""
	if tlsEnabled(vc) {
		port = valkeyTLSPort
		tlsArgs = fmt.Sprintf(" --tls --cert %s/tls.crt --key %s/tls.key --cacert %s/ca.crt --insecure", tlsMountPath, tlsMountPath, tlsMountPath)
	}

	pwArg := ""
	if password != "" {
		pwArg = noAuthWarningArgs
	}
	if perShardEnabled(vc) {
		return buildClusterOpJob(vc, name, "scaledown", password,
			buildShardScaleDownScript(vc, port, tlsArgs, pwArg))
	}

	headless := headlessServiceName(vc)
	stsName := statefulSetName(vc)
	hostBase := fmt.Sprintf("%s-0.%s.%s.svc.cluster.local", stsName, headless, vc.Namespace)
	existing := fmt.Sprintf("%s:%d", hostBase, port)

	// Build script: collect leaving hostnames, find a remaining master ID,
	// for each leaving node look up its ID and reshard+del-node it.
	//
	// NB: the leaving entries are bare FQDNs with NO :port suffix. They are
	// matched against the address field of `CLUSTER NODES` ($2), which is
	// "<ip>:<port>@<bus>,<fqdn>" — i.e. the FQDN appears there WITHOUT a port.
	// A ":port" suffix here would make the `$2 ~ p` regex never match, so the
	// leaving node would be silently "skipped", never resharded or del-node'd,
	// yet the Job would still exit 0 — and the operator would then shrink the
	// StatefulSet, deleting pods that still own slots (data loss). This was the
	// scale-down data-loss surfaced by the cha-02 chaos test.
	var leaving strings.Builder
	for i := totalReplicas(vc); i < vc.Status.LastAppliedReplicas; i++ {
		fmt.Fprintf(&leaving, " %s-%d.%s.%s.svc.cluster.local", stsName, i, headless, vc.Namespace)
	}

	// REMAINING_MASTER_ID is pod-0's OWN id (the `myself` line). pod-0 always
	// survives a scale-down (want >= 3 > 0), so the reshard target can never be
	// one of the leaving masters — selecting "the first master line" could
	// otherwise pick a leaving node and move slots from one leaving node onto
	// another, both of which are then removed (data loss).
	script := fmt.Sprintf(`#!/bin/sh
set -eu
HOST=%[1]s
%[7]sREMAINING_MASTER_ID=$(valkey-cli%[2]s%[3]s -h $HOST -p %[4]d cluster nodes | awk '/myself/ {print $1; exit}')
if [ -z "$REMAINING_MASTER_ID" ]; then
  echo "no surviving master found"; exit 1
fi
echo "remaining master (pod-0): $REMAINING_MASTER_ID"
for L in%[5]s; do
  echo "--- leaving $L ---"
  L_ID=$(valkey-cli%[2]s%[3]s -h $HOST -p %[4]d cluster nodes | awk -v p="$L" '$2 ~ p {print $1; exit}')
  if [ -z "$L_ID" ]; then
    echo "node $L not in cluster, skipping"; continue
  fi
  # Reshard everything it owns away (best-effort: a no-op on an already-empty
  # master can exit non-zero, which is fine — the ownership gate below is the
  # authoritative safety check).
  valkey-cli%[2]s%[3]s --cluster reshard %[6]s \
    --cluster-from "$L_ID" --cluster-to "$REMAINING_MASTER_ID" \
    --cluster-slots 16384$ASM_FLAG --cluster-yes || true
  # SAFETY GATE: refuse to remove a master that still owns slots. Deleting it
  # (and, once this Job succeeds, the StatefulSet pod behind it) would orphan
  # those slots and lose their data. Fail the Job loudly so the operator leaves
  # LastAppliedReplicas — and therefore the StatefulSet size — untouched.
  OWNED=$(valkey-cli%[2]s%[3]s -h $HOST -p %[4]d cluster nodes | awk -v id="$L_ID" '$1==id' | sed -e 's/.* connected//' -e 's/[[:space:]]//g')
  if [ -n "$OWNED" ]; then
    echo "FATAL: $L ($L_ID) still owns slots [$OWNED] after reshard; refusing del-node to avoid data loss"
    exit 1
  fi
  valkey-cli%[2]s%[3]s --cluster del-node %[6]s "$L_ID"
done
`, hostBase, pwArg, tlsArgs, port, leaving.String(), existing,
		asmDetectSnippet(pwArg, tlsArgs, "$HOST", port))

	// AutoReshard: the reshard-away step above piles every leaving master's
	// slots onto the single REMAINING_MASTER_ID, leaving the cluster correct
	// but lopsided. Rebalance spreads those slots back out evenly across the
	// surviving masters so the post-scale-down state is balanced — i.e. the
	// cluster converges without a manual follow-up reshard.
	if vc.Spec.AutoReshard {
		script += fmt.Sprintf("echo 'rebalancing surviving masters'\nvalkey-cli%s%s --cluster rebalance %s$ASM_FLAG --cluster-yes\n",
			pwArg, tlsArgs, existing)
	}

	env := []corev1.EnvVar{}
	if password != "" {
		env = append(env, corev1.EnvVar{
			Name: envValkeyPassword,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: passwordSecretName(vc)},
					Key:                  secretKeyPassword,
				},
			},
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
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: vc.Namespace, Labels: labels},
		Spec: batchv1.JobSpec{
			BackoffLimit:            ptr.To[int32](3),
			TTLSecondsAfterFinished: ptr.To[int32](600),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyOnFailure,
					Containers: []corev1.Container{{
						Name:         "scaledown",
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

// runClusterReshard handles a manual reshard request (valkey.wellcake.io/reshard).
// It runs a one-off `valkey-cli --cluster rebalance` Job and, on success,
// records the request token in status so it runs exactly once per request.
func (r *ValkeyClusterReconciler) runClusterReshard(ctx context.Context, vc *cachev1beta1.ValkeyCluster, password, token string) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	name := vc.Name + "-reshard"
	key := client.ObjectKey{Namespace: vc.Namespace, Name: name}

	var existing batchv1.Job
	err := r.Get(ctx, key, &existing)
	if apierrors.IsNotFound(err) {
		job := buildReshardJob(vc, password, name)
		if err := controllerutil.SetControllerReference(vc, job, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		log.Info("creating manual reshard job", "name", name, "token", token)
		// Requeue (non-zero result) so the caller returns before surveying the
		// cluster — no point reading membership while a rebalance is in flight.
		return ctrl.Result{RequeueAfter: 5 * time.Second}, r.Create(ctx, job)
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	switch {
	case existing.Status.Succeeded > 0:
		log.Info("manual reshard job succeeded", "name", name)
		scaleEventsTotal.WithLabelValues(vc.Namespace, vc.Name, "reshard", "success").Inc()
		patch := client.MergeFrom(vc.DeepCopy())
		vc.Status.LastReshardToken = token
		if err := r.Status().Patch(ctx, vc, patch); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.Delete(ctx, &existing)
	case existing.Status.Failed > *existing.Spec.BackoffLimit:
		scaleEventsTotal.WithLabelValues(vc.Namespace, vc.Name, "reshard", "failure").Inc()
		return r.setPhase(ctx, vc,
			"ReshardFailed", fmt.Sprintf("Job %s failed; inspect kubectl logs job/%s", name, name))
	default:
		// Still running — requeue and skip the survey until it finishes.
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
}

// buildReshardJob renders a one-off rebalance Job that spreads slots evenly
// across all masters (including empty ones).
func buildReshardJob(vc *cachev1beta1.ValkeyCluster, password, name string) *batchv1.Job {
	labels := labelsFor(vc)
	port := valkeyPort
	tlsArgs := ""
	if tlsEnabled(vc) {
		port = valkeyTLSPort
		tlsArgs = fmt.Sprintf(" --tls --cert %s/tls.crt --key %s/tls.key --cacert %s/ca.crt --insecure", tlsMountPath, tlsMountPath, tlsMountPath)
	}
	existingHost := fmt.Sprintf("%s-0.%s.%s.svc.cluster.local",
		statefulSetName(vc), headlessServiceName(vc), vc.Namespace)
	existing := fmt.Sprintf("%s:%d", existingHost, port)

	pwArg := ""
	if password != "" {
		pwArg = noAuthWarningArgs
	}
	script := fmt.Sprintf("#!/bin/sh\nset -eu\n%secho 'rebalancing'\nvalkey-cli%s%s --cluster rebalance %s --cluster-use-empty-masters$ASM_FLAG --cluster-yes\n",
		asmDetectSnippet(pwArg, tlsArgs, existingHost, port), pwArg, tlsArgs, existing)

	env := []corev1.EnvVar{}
	if password != "" {
		env = append(env, corev1.EnvVar{
			Name: envValkeyPassword,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: passwordSecretName(vc)},
					Key:                  secretKeyPassword,
				},
			},
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
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: vc.Namespace, Labels: labels},
		Spec: batchv1.JobSpec{
			BackoffLimit:            ptr.To[int32](3),
			TTLSecondsAfterFinished: ptr.To[int32](600),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyOnFailure,
					Containers: []corev1.Container{{
						Name:         "reshard",
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

// clusterNode is the parsed form of one line of `CLUSTER NODES`.
type clusterNode struct {
	id        string
	podName   string // extracted from the FQDN if it matches our stable naming.
	flags     []string
	masterID  string // empty for masters.
	slots     []string
	linkState string // "connected" | "disconnected"
}

func (n *clusterNode) isMaster() bool {
	return slices.Contains(n.flags, roleMaster)
}

func (n *clusterNode) isHealthy() bool {
	if n.linkState != "connected" {
		return false
	}
	for _, f := range n.flags {
		if f == "fail" || f == "fail?" || f == "noaddr" {
			return false
		}
	}
	return true
}

// parseClusterNodes turns the raw `CLUSTER NODES` output into one entry per
// node. The expected line format is:
//
//	<id> <ip:port@cport[,hostname]> <flags> <master|-> <ping> <pong> <epoch>
//	<link> <slot> <slot> ...
//
// We tolerate the optional ,hostname tail and migrating/importing slot
// markers ("[N->-targetID]", "[N-<-sourceID]") which we don't add to
// SlotRanges (they're transitional).
func parseClusterNodes(raw string) []clusterNode {
	var out []clusterNode
	for line := range strings.SplitSeq(strings.TrimSpace(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 8 {
			continue
		}
		n := clusterNode{
			id:        fields[0],
			flags:     strings.Split(fields[2], ","),
			linkState: fields[7],
		}
		if fields[3] != "-" {
			n.masterID = fields[3]
		}
		// Address field: "host:port@bus[,hostname]". Use the hostname when
		// present (we set cluster-announce-hostname in the runtime config),
		// otherwise the host:port part.
		addr := fields[1]
		if _, after, ok := strings.Cut(addr, ","); ok {
			n.podName = podNameFromFQDN(after)
		} else if before, _, ok := strings.Cut(addr, "@"); ok {
			n.podName = podNameFromFQDN(before)
		}
		// Collect plain "N" or "N-M" slot tokens. Transitional ones
		// ("[N->-...]", "[N-<-...]") are skipped.
		for _, tok := range fields[8:] {
			if strings.HasPrefix(tok, "[") {
				continue
			}
			n.slots = append(n.slots, tok)
		}
		out = append(out, n)
	}
	return out
}

// podNameFromFQDN takes "demo-3.demo-headless.demo.svc.cluster.local"
// (or just "demo-3:6379") and returns "demo-3". Bestow nothing if the
// input doesn't look like one of our pods — the caller will fall back to
// the raw address.
func podNameFromFQDN(s string) string {
	if i := strings.Index(s, ":"); i >= 0 {
		s = s[:i]
	}
	if i := strings.Index(s, "."); i >= 0 {
		s = s[:i]
	}
	return s
}

// surveyCluster runs CLUSTER NODES from pod-0 and turns it into the
// shard-keyed view the operator exposes via status.ShardDetails. The order
// of returned shards is the order in which masters appear in the output —
// stable across reconciles because Valkey itself returns nodes sorted by
// `myself` then by ID, and pod-0 is always the same.
func (r *ValkeyClusterReconciler) surveyCluster(ctx context.Context, vc *cachev1beta1.ValkeyCluster, password string) []cachev1beta1.ShardStatus {
	log := logf.FromContext(ctx)
	port := valkeyPort
	if tlsEnabled(vc) {
		port = valkeyTLSPort
	}
	c := dialReplClient(ctx, clusterAnyPodHost(vc), port, password, tlsEnabled(vc), loadMTLSClientCert(ctx, r, vc), 3*time.Second)
	if c == nil {
		log.Info("surveyCluster: pod-0 unreachable, skipping")
		return nil
	}
	defer c.close()

	raw, err := c.clusterNodes(ctx)
	if err != nil {
		log.Error(err, "CLUSTER NODES failed")
		return nil
	}
	nodes := parseClusterNodes(raw)

	// Build per-master shard records, then attach replicas by masterID.
	idx := int32(0)
	byID := map[string]int{} // master ID -> shard index in `shards`
	var shards []cachev1beta1.ShardStatus
	for _, n := range nodes {
		if !n.isMaster() {
			continue
		}
		s := cachev1beta1.ShardStatus{
			Index:         idx,
			Primary:       n.podName,
			PrimaryNodeID: n.id,
			SlotRanges:    n.slots,
			SlotCount:     slotsCount(n.slots),
			Health:        cachev1beta1.ShardHealthReady,
		}
		if !n.isHealthy() {
			s.Health = cachev1beta1.ShardHealthDown
		}
		byID[n.id] = len(shards)
		shards = append(shards, s)
		idx++
	}
	for _, n := range nodes {
		if n.isMaster() || n.masterID == "" {
			continue
		}
		sIdx, ok := byID[n.masterID]
		if !ok {
			continue
		}
		shards[sIdx].Replicas = append(shards[sIdx].Replicas, n.podName)
		// Replica down with primary up → shard is Degraded but not Down.
		if !n.isHealthy() && shards[sIdx].Health == cachev1beta1.ShardHealthReady {
			shards[sIdx].Health = cachev1beta1.ShardHealthDegraded
		}
	}

	// Second pass: collect repl offsets per shard via INFO replication.
	// Cheap (one TCP open per pod) and bounded by totalReplicas. Skipped
	// for shards already marked Down — no primary to ask.
	r.fillReplicaLag(ctx, vc, password, shards)
	return shards
}

// fillReplicaLag dials the primary of each healthy shard (and every replica
// listed by CLUSTER NODES) for INFO replication, then computes MaxLagBytes.
// Pods that don't answer are silently skipped — failover/availability
// signals are already covered by ShardStatus.Health.
func (r *ValkeyClusterReconciler) fillReplicaLag(ctx context.Context, vc *cachev1beta1.ValkeyCluster, password string, shards []cachev1beta1.ShardStatus) {
	port := valkeyPort
	if tlsEnabled(vc) {
		port = valkeyTLSPort
	}
	dial := func(pod string) *replClient {
		return dialReplClient(ctx, podFQDN(vc, pod), port, password, tlsEnabled(vc), loadMTLSClientCert(ctx, r, vc), 2*time.Second)
	}

	for i := range shards {
		s := &shards[i]
		if s.Health == cachev1beta1.ShardHealthDown || s.Primary == "" {
			continue
		}
		pc := dial(s.Primary)
		if pc == nil {
			continue
		}
		info, err := pc.info(ctx)
		pc.close()
		if err != nil {
			continue
		}
		if v, ok := info["master_repl_offset"]; ok {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				s.PrimaryOffset = n
			}
		}

		var maxLag int64
		for _, replicaPod := range s.Replicas {
			rc := dial(replicaPod)
			if rc == nil {
				continue
			}
			rinfo, err := rc.info(ctx)
			rc.close()
			if err != nil {
				continue
			}
			off := int64(0)
			if v, ok := rinfo["slave_repl_offset"]; ok {
				if n, err := strconv.ParseInt(v, 10, 64); err == nil {
					off = n
				}
			}
			s.ReplicaOffsets = append(s.ReplicaOffsets, cachev1beta1.ReplicaOffset{
				Pod:    replicaPod,
				Offset: off,
			})
			if lag := s.PrimaryOffset - off; lag > maxLag {
				maxLag = lag
			}
		}
		s.MaxLagBytes = maxLag
	}
}

// slotsCount sums slot tokens like "5461", "12000-12100" into a total count.
func slotsCount(tokens []string) int32 {
	var total int32
	for _, t := range tokens {
		if i := strings.Index(t, "-"); i > 0 {
			lo, err1 := strconv.Atoi(t[:i])
			hi, err2 := strconv.Atoi(t[i+1:])
			if err1 == nil && err2 == nil && hi >= lo {
				total += int32(hi - lo + 1)
			}
			continue
		}
		if _, err := strconv.Atoi(t); err == nil {
			total++
		}
	}
	return total
}

func (r *ValkeyClusterReconciler) updateClusterStatus(ctx context.Context, vc *cachev1beta1.ValkeyCluster, readyReplicas int32, allReady bool, shardDetails []cachev1beta1.ShardStatus) (ctrl.Result, error) { //nolint:unparam // Result is always zero; kept for the reconcile-helper return shape
	patch := client.MergeFrom(vc.DeepCopy())
	want := totalReplicas(vc)
	vc.Status.Shards = *vc.Spec.Shards
	vc.Status.ReadyReplicas = readyReplicas
	vc.Status.InternalEndpoint = internalEndpoint(vc)
	vc.Status.ObservedGeneration = vc.Generation
	vc.Status.ShardDetails = shardDetails

	readyShards, degraded := summarizeShards(shardDetails)
	vc.Status.ReadyShards = readyShards
	recordShards(vc.Namespace, vc.Name, shardDetails)

	var availReason, availMsg string
	switch {
	case !allReady:
		vc.Status.Phase = cachev1beta1.PhaseCreating
		availReason = "PodsNotReady"
		availMsg = fmt.Sprintf("%d/%d pods ready", readyReplicas, want)
	case !vc.Status.ClusterInitialized:
		vc.Status.Phase = cachev1beta1.PhaseCreating
		availReason = "Bootstrapping"
		availMsg = "Running valkey-cli --cluster create"
	default:
		vc.Status.Phase = cachev1beta1.PhaseReady
		availReason = reasonReady
		availMsg = fmt.Sprintf("Cluster of %d shards is ready (%d healthy)", *vc.Spec.Shards, readyShards)
	}
	ready := vc.Status.Phase == cachev1beta1.PhaseReady
	setCondition(&vc.Status.Conditions, metav1.Condition{
		Type:               cachev1beta1.ConditionAvailable,
		Status:             condStatus(ready),
		Reason:             availReason,
		Message:            availMsg,
		ObservedGeneration: vc.Generation,
		LastTransitionTime: metav1.Now(),
	})
	setReadyCondition(&vc.Status, ready, availReason, availMsg)

	// ShardsHealthy: True iff every observed shard is Ready. Degraded or
	// Down → False with a reason that names the impacted shard indices.
	if len(shardDetails) > 0 {
		shardCond := metav1.Condition{
			Type:               cachev1beta1.ConditionShardsHealthy,
			ObservedGeneration: vc.Generation,
			LastTransitionTime: metav1.Now(),
		}
		if len(degraded) == 0 {
			shardCond.Status = metav1.ConditionTrue
			shardCond.Reason = "AllHealthy"
			shardCond.Message = fmt.Sprintf("%d shards healthy", readyShards)
		} else {
			shardCond.Status = metav1.ConditionFalse
			shardCond.Reason = "ShardsNotHealthy"
			shardCond.Message = fmt.Sprintf("unhealthy shards: %s", strings.Join(degraded, ", "))
		}
		setCondition(&vc.Status.Conditions, shardCond)
	}

	return ctrl.Result{}, r.Status().Patch(ctx, vc, patch)
}

// summarizeShards returns the count of fully-Ready shards and a list of
// "<index>:<health>" descriptors for any shard that isn't Ready, ready to
// drop into a condition message.
func summarizeShards(shards []cachev1beta1.ShardStatus) (int32, []string) {
	var ready int32
	var bad []string
	for _, s := range shards {
		if s.Health == cachev1beta1.ShardHealthReady {
			ready++
			continue
		}
		bad = append(bad, fmt.Sprintf("%d:%s", s.Index, s.Health))
	}
	return ready, bad
}
