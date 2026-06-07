/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"context"
	"fmt"
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

// Automated Cluster re-assembly on restore (ROADMAP C2). When spec.restoreFrom
// is set on a Cluster topology, the per-shard restore-init containers load each
// master's RDB onto its PVC, but the nodes are isolated single-node clusters
// with NO slots assigned — `valkey-cli --cluster create` can't be used (it
// aborts on non-empty nodes). This Job reconstructs the cluster deterministically
// from the backup's slot manifest, so the SAME slot→shard map is restored and
// every key lands on the node that owns its slot.
//
// Procedure (the manual runbook, automated):
//  1. ADDSLOTSRANGE each master its manifest slot ranges, SET-CONFIG-EPOCH unique
//     (while the nodes are still isolated);
//  2. CLUSTER MEET to form the gossip mesh;
//  3. CLUSTER REPLICATE to attach replica ordinals to their masters;
//  4. wait for cluster_state:ok with all 16384 slots.

// clusterRestoreManifestKey derives the manifest S3 key from the per-shard RDB
// SourceKey: the backup writes `<base>-<stamp>-shard-<N>.rdb` alongside
// `<base>-<stamp>-manifest.txt`, and the cluster SourceKey is
// `<base>-<stamp>-shard-{shard}.rdb`, so swapping the shard-RDB tail for the
// manifest tail yields the manifest key.
func clusterRestoreManifestKey(sourceKey string) string {
	if strings.Contains(sourceKey, "shard-{shard}.rdb") {
		return strings.Replace(sourceKey, "shard-{shard}.rdb", "manifest.txt", 1)
	}
	return strings.Replace(sourceKey, "{shard}.rdb", "manifest.txt", 1)
}

// runClusterRestoreAssembly drives the assembly Job, mirroring runClusterBootstrap:
// if the cluster is already formed it adopts it; otherwise it creates the Job and
// flips ClusterInitialized once the Job succeeds.
func (r *ValkeyClusterReconciler) runClusterRestoreAssembly(ctx context.Context, vc *cachev1beta1.ValkeyCluster, password string) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Idempotency on the data plane: an already-assembled cluster (e.g. the Job
	// ran, status was lost, or a manual assembly happened) is adopted, never
	// re-assembled — ADDSLOTSRANGE on a node that already owns slots would error.
	if r.clusterFormed(ctx, vc, password) {
		log.Info("cluster restore: assembled cluster detected, adopting", "name", vc.Name)
		return r.adoptFormedCluster(ctx, vc, "RestoreAssembled",
			"cluster assembled from restoreFrom adopted")
	}

	name := vc.Name + "-restore-assembly"
	key := client.ObjectKey{Namespace: vc.Namespace, Name: name}

	var existing batchv1.Job
	err := r.Get(ctx, key, &existing)
	if apierrors.IsNotFound(err) {
		job := buildRestoreAssemblyJob(vc, password, name)
		if err := controllerutil.SetControllerReference(vc, job, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		log.Info("creating cluster restore-assembly job", "name", name)
		// Requeue to keep re-checking clusterFormed (top) — the cluster may finish
		// assembling (gossip converge) a little after the Job's own work, and we
		// must not rely solely on Job watch events to notice + adopt it.
		return ctrl.Result{RequeueAfter: 10 * time.Second}, r.Create(ctx, job)
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	switch {
	case existing.Status.Succeeded > 0:
		log.Info("cluster restore-assembly job succeeded", "name", name)
		patch := client.MergeFrom(vc.DeepCopy())
		vc.Status.ClusterInitialized = true
		vc.Status.LastAppliedReplicas = totalReplicas(vc)
		setCondition(&vc.Status.Conditions, metav1.Condition{
			Type:               cachev1beta1.ConditionClusterInitialized,
			Status:             metav1.ConditionTrue,
			Reason:             "RestoreAssembled",
			Message:            fmt.Sprintf("Job %s assembled the restored cluster", name),
			ObservedGeneration: vc.Generation,
			LastTransitionTime: metav1.Now(),
		})
		return ctrl.Result{}, r.Status().Patch(ctx, vc, patch)
	case existing.Status.Failed > *existing.Spec.BackoffLimit:
		// The Job exhausted its retries, but the cluster may actually have formed
		// (the Job's internal wait-for-ok can expire before gossip converges on a
		// slow node). Keep requeuing so the clusterFormed check at the top adopts a
		// late-converging cluster instead of getting stuck. adoptFormedCluster wins
		// the moment the data plane reports cluster_state:ok.
		log.Info("restore-assembly job failed; re-checking whether the cluster formed", "name", name)
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	default:
		// Job still running — poll clusterFormed so we adopt promptly once ok.
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
}

// buildRestoreAssemblyJob renders the assembly Job: an aws-cli init container
// fetches the slot manifest into a shared emptyDir, then the valkey container
// reconstructs the cluster from it.
func buildRestoreAssemblyJob(vc *cachev1beta1.ValkeyCluster, password, name string) *batchv1.Job {
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
	shards := int32(0)
	if vc.Spec.Shards != nil {
		shards = *vc.Spec.Shards
	}
	total := totalReplicas(vc)
	sts := statefulSetName(vc)
	headless := headlessServiceName(vc)

	header := fmt.Sprintf(`#!/bin/sh
set -eu
PORT=%d
SHARDS=%d
TOTAL=%d
node() { echo "%s-$1.%s.%s.svc.cluster.local"; }
VC() { valkey-cli%s%s "$@"; }
`, port, shards, total, sts, headless, vc.Namespace, pwArg, tlsArgs)

	// Body uses no fmt verbs (raw string) — all parameters are in the header.
	body := `
# 1) wait until every node answers PING (the restore pods may still be settling)
i=0
while [ "$i" -lt "$TOTAL" ]; do
  until VC -h "$(node "$i")" -p "$PORT" ping >/dev/null 2>&1; do echo "waiting $(node "$i")"; sleep 2; done
  i=$((i + 1))
done
# 2) per master (from manifest): give it the slots it must own that the restored
#    RDB didn't already claim. A cluster-master RDB carries slot ownership ONLY
#    for data-bearing slots (Valkey AUX slot-info), so on load the node owns a
#    SPARSE subset of its range — ADDSLOTSRANGE over the whole range would fail
#    ("Slot N is already busy"). We compute the gap (manifest slots MINUS the
#    already-owned ones) and add only those, as collapsed contiguous ranges. The
#    config epoch is left as-is: it's carried distinct per shard from the source
#    RDBs, and SET-CONFIG-EPOCH rejects an already-non-zero epoch (collisions, if
#    any, are auto-resolved by gossip after MEET).
expand() { tr ',' '\n' | awk -F- 'NF==2{for(k=$1;k<=$2;k++)print k; next}{print $1}'; }
while read -r LINE; do
  case "$LINE" in \#*|"") continue ;; esac
  IDX=$(printf '%s\n' "$LINE" | awk '{print $1}' | sed 's/^shard-//')
  SLOTS=$(printf '%s\n' "$LINE" | awk '{print $3}')
  [ -n "$SLOTS" ] || { echo "FATAL: shard $IDX has no slots in manifest"; exit 1; }
  MH="$(node "$IDX")"
  printf '%s' "$SLOTS" | expand | sort -n > /tmp/want.$IDX
  VC -h "$MH" -p "$PORT" cluster nodes | tr -d '\r' \
    | awk '/myself/{for(j=9;j<=NF;j++){if($j ~ /^[0-9]+-[0-9]+$/){split($j,r,"-");for(k=r[1];k<=r[2];k++)print k}else if($j ~ /^[0-9]+$/)print $j}}' \
    | sort -n > /tmp/have.$IDX
  awk 'NR==FNR{h[$1]=1;next} !($1 in h){print $1}' /tmp/have.$IDX /tmp/want.$IDX \
    | awk 'NR==1{lo=$1;p=$1;next} $1==p+1{p=$1;next} {print lo,p;lo=$1;p=$1} END{if(NR>0)print lo,p}' \
    | while read -r a b; do echo "  shard $IDX: addslotsrange $a $b"; VC -h "$MH" -p "$PORT" cluster addslotsrange "$a" "$b"; done
done < /work/manifest.txt
# 3) MEET mesh from pod-0. The node's gossip address has no IP (announce-ip is
#    unset → ":port@cport,hostname"), and CLUSTER MEET needs an IP, so resolve
#    each pod's FQDN with getent (present in the valkey image) and meet by IP.
M0="$(node 0)"
i=1
while [ "$i" -lt "$TOTAL" ]; do
  FQ="$(node "$i")"
  IP=$(getent hosts "$FQ" | awk '{print $1; exit}')
  [ -n "$IP" ] || { echo "FATAL: cannot resolve $FQ"; exit 1; }
  VC -h "$M0" -p "$PORT" cluster meet "$IP" "$PORT"
  i=$((i + 1))
done
# 4) wait for gossip to converge (pod-0 knows every node)
ok=0; n=0
while [ "$n" -lt 150 ]; do
  KN=$(VC -h "$M0" -p "$PORT" cluster info | tr -d '\r' | awk -F: '/cluster_known_nodes:/{print $2+0}')
  [ "$KN" = "$TOTAL" ] && { ok=1; break; }
  sleep 2; n=$((n + 1))
done
[ "$ok" = 1 ] || { echo "FATAL: gossip did not converge (${KN:-0}/$TOTAL known)"; exit 1; }
# 5) REPLICATE: replica ordinals (>= SHARDS) attach round-robin to masters.
o="$SHARDS"
while [ "$o" -lt "$TOTAL" ]; do
  SIDX=$(( (o - SHARDS) % SHARDS ))
  MID=$(VC -h "$(node "$SIDX")" -p "$PORT" cluster myid | tr -d '\r')
  [ -n "$MID" ] || { echo "FATAL: empty master id for shard $SIDX"; exit 1; }
  echo "replica $(node "$o") -> master shard $SIDX ($MID)"
  # A replica can only replicate a master it has already learned about via gossip.
  # NB: valkey-cli exits 0 even on an "ERR Unknown node ..." reply, so we must
  # check the reply text (== OK) — not the exit code — and retry (bounded) until
  # gossip converges and the command actually takes.
  rn=0
  until [ "$(VC -h "$(node "$o")" -p "$PORT" cluster replicate "$MID" 2>&1 | tr -d '\r')" = "OK" ]; do
    rn=$((rn + 1)); [ "$rn" -ge 90 ] && { echo "FATAL: replica $(node "$o") could not replicate $MID"; exit 1; }
    echo "retry replicate $(node "$o")"; sleep 2
  done
  o=$((o + 1))
done
# 6) wait for cluster_state:ok with all slots assigned
n=0
while [ "$n" -lt 150 ]; do
  INFO=$(VC -h "$M0" -p "$PORT" cluster info | tr -d '\r')
  ST=$(printf '%s\n' "$INFO" | awk -F: '/cluster_state:/{print $2}')
  SA=$(printf '%s\n' "$INFO" | awk -F: '/cluster_slots_assigned:/{print $2+0}')
  if [ "$ST" = ok ] && [ "$SA" = 16384 ]; then echo "cluster assembled: state ok, 16384 slots"; exit 0; fi
  sleep 2; n=$((n + 1))
done
echo "FATAL: cluster did not reach state ok with all slots"
VC -h "$M0" -p "$PORT" cluster info || true
exit 1
`
	script := header + body

	// aws-cli init: fetch the manifest into the shared work volume.
	rs := vc.Spec.RestoreFrom
	awsImage := rs.AWSCLIImage
	if awsImage == "" {
		awsImage = defaultAWSCLIImage
	}
	endpointFlag := ""
	if rs.S3.Endpoint != "" {
		endpointFlag = s3EndpointURLFlag
	}
	manifestKey := clusterRestoreManifestKey(rs.SourceKey)
	fetchScript := fmt.Sprintf(`set -eu
echo "fetching restore manifest s3://%[1]s/%[2]s"
aws s3 cp "s3://%[1]s/%[2]s" /work/manifest.txt%[3]s
echo "manifest:"; cat /work/manifest.txt
`, rs.S3.Bucket, manifestKey, endpointFlag)

	awsEnv := []corev1.EnvVar{
		{Name: envAWSAccessKey, ValueFrom: secretRef(rs.S3.CredentialsSecret, envAWSAccessKey)},
		{Name: envAWSSecretKey, ValueFrom: secretRef(rs.S3.CredentialsSecret, envAWSSecretKey)},
		{Name: envAWSDefaultRegion, Value: rs.S3.Region},
	}
	if rs.S3.Endpoint != "" {
		awsEnv = append(awsEnv, corev1.EnvVar{Name: envS3EndpointURL, Value: rs.S3.Endpoint})
	}

	valkeyEnv := []corev1.EnvVar{}
	mounts := []corev1.VolumeMount{{Name: workVolumeName, MountPath: "/work"}}
	volumes := []corev1.Volume{{Name: workVolumeName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}}
	if password != "" {
		valkeyEnv = append(valkeyEnv, corev1.EnvVar{Name: envValkeyPassword, ValueFrom: secretRef(passwordSecretName(vc), secretKeyPassword)})
	}
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
					InitContainers: []corev1.Container{{
						Name:         "fetch-manifest",
						Image:        awsImage,
						Command:      []string{shellCmd, "-c", fetchScript},
						Env:          awsEnv,
						VolumeMounts: []corev1.VolumeMount{{Name: workVolumeName, MountPath: "/work"}},
					}},
					Containers: []corev1.Container{{
						Name:         "assemble",
						Image:        vc.Spec.Image,
						Command:      []string{shellCmd, "-c", script},
						Env:          valkeyEnv,
						VolumeMounts: mounts,
					}},
					Volumes: volumes,
				},
			},
		},
	}
}

// secretRef is a small helper for an env var sourced from a Secret key.
func secretRef(secretName, key string) *corev1.EnvVarSource {
	return &corev1.EnvVarSource{
		SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
			Key:                  key,
		},
	}
}
