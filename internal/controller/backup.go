/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"context"
	"fmt"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// ensureBackupCronJob materializes a CronJob that periodically takes an RDB
// snapshot via `valkey-cli --rdb` and uploads it to S3. The CronJob is
// destroyed when spec.backup is unset/disabled.
//
// Implementation note: a two-container pod design keeps the image surface
// minimal. The init container runs valkey-cli (already in the Valkey image)
// to stream a fresh RDB into a shared emptyDir; the main container uses
// the AWS CLI image to upload it. The emptyDir is short-lived (lives only
// for one Job pod) so the no-emptyDir rule for data volumes does not apply.
func (r *ValkeyClusterReconciler) ensureBackupCronJob(ctx context.Context, vc *cachev1beta1.ValkeyCluster) error {
	name := vc.Name + "-backup"
	key := client.ObjectKey{Namespace: vc.Namespace, Name: name}

	want := vc.Spec.Backup != nil && vc.Spec.Backup.Enabled
	if !want {
		var existing batchv1.CronJob
		if err := r.Get(ctx, key, &existing); err == nil {
			return r.Delete(ctx, &existing)
		}
		return nil
	}
	if vc.Spec.Backup.S3 == nil {
		return fmt.Errorf("backup.s3 is required when backup.enabled=true")
	}

	desired := buildBackupCronJob(vc, name)
	if err := controllerutil.SetControllerReference(vc, desired, r.Scheme); err != nil {
		return err
	}

	var existing batchv1.CronJob
	err := r.Get(ctx, key, &existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	existing.Spec = desired.Spec
	existing.Labels = desired.Labels
	return r.Update(ctx, &existing)
}

func buildBackupCronJob(vc *cachev1beta1.ValkeyCluster, name string) *batchv1.CronJob {
	labels := labelsFor(vc)
	port := valkeyPort
	tlsArgs := ""
	if tlsEnabled(vc) {
		port = valkeyTLSPort
		tlsArgs = fmt.Sprintf(" --tls --cert %s/tls.crt --key %s/tls.key --cacert %s/ca.crt --insecure", tlsMountPath, tlsMountPath, tlsMountPath)
	}
	pwArg := ""
	if vc.Spec.Auth != nil && vc.Spec.Auth.Enabled {
		pwArg = noAuthWarningArgs
	}

	// Single-shard topologies dump the client Service (k8s routes to a ready
	// pod, which is the primary). Cluster topology fans out: discover masters
	// via `cluster nodes` and dump each one to /backup/shard-<i>.rdb.
	host := fmt.Sprintf("%s.%s.svc.cluster.local", clientServiceName(vc), vc.Namespace)

	dumpImage := vc.Spec.Backup.Image
	if dumpImage == "" {
		dumpImage = vc.Spec.Image
	}
	awsImage := vc.Spec.Backup.AWSCLIImage
	if awsImage == "" {
		awsImage = defaultAWSCLIImage
	}

	var dumpScript string
	if vc.Spec.Topology == cachev1beta1.TopologyCluster {
		// Iterate pods by their STABLE FQDN (not the gossip IPs from a single
		// CLUSTER NODES view — those go stale when a pod is recreated, and the
		// dump then times out connecting to a dead IP). For each pod that reports
		// itself a master, dump its RDB AND record its owned slot ranges into
		// manifest.txt (slots live in cluster state, not the RDB — this manifest
		// is the prerequisite for automated re-assembly on restore, ROADMAP C2).
		// Dumping + recording slots in the same iteration keeps shard-<i>.rdb and
		// the manifest's shard-<i> entry consistent. Migration markers ([slot->-id])
		// are skipped: only stable owned ranges are recorded.
		var hostList strings.Builder
		for _, p := range clusterDataPods(vc) {
			hostList.WriteString(p.host)
			hostList.WriteByte(' ')
		}
		header := fmt.Sprintf(`set -eu
PORT=%d
HOSTS="%s"
VC() { valkey-cli%s%s "$@"; }
`, port, strings.TrimSpace(hostList.String()), pwArg, tlsArgs)
		dumpScript = header + `MANIFEST=/backup/manifest.txt
{ echo "# valkey-operator cluster restore manifest v1"; echo "# shard-index node-fqdn slot-ranges(csv)"; } > "$MANIFEST"
i=0
for FQDN in $HOSTS; do
  NODES=""; n=0
  until NODES=$(VC -t 20 -h "$FQDN" -p "$PORT" cluster nodes 2>/dev/null | tr -d '\r') && [ -n "$NODES" ]; do
    n=$((n + 1)); [ "$n" -ge 8 ] && { echo "FATAL: cannot reach $FQDN"; exit 1; }
    echo "  waiting for $FQDN"; sleep 3
  done
  SELF=$(printf '%s\n' "$NODES" | awk '/myself/{print; exit}')
  FLAGS=$(printf '%s\n' "$SELF" | awk '{print $3}')
  case "$FLAGS" in
    *master*)
      echo "  dumping shard $i (master $FQDN)"
      m=0
      until VC -t 20 -h "$FQDN" -p "$PORT" --rdb /backup/shard-${i}.rdb; do
        m=$((m + 1)); [ "$m" -ge 5 ] && { echo "FATAL: shard $i dump failed after $m attempts"; exit 1; }
        echo "  shard $i dump attempt $m failed, retrying"; sleep 3
      done
      echo "  verifying shard $i"
      valkey-check-rdb /backup/shard-${i}.rdb
      SLOTS=$(printf '%s\n' "$SELF" | awk '{s="";for(j=9;j<=NF;j++) if($j ~ /^[0-9]/) s=s (s==""?"":",") $j; print s}')
      echo "shard-${i} ${FQDN} ${SLOTS}" >> "$MANIFEST"
      i=$((i + 1))
      ;;
  esac
done
echo "total shards dumped: $i"
echo "manifest:"; cat "$MANIFEST"
`
	} else {
		dumpScript = fmt.Sprintf(`set -eu
echo "dumping RDB from %[1]s:%[2]d"
valkey-cli%[3]s%[4]s -h %[1]s -p %[2]d --rdb /backup/dump.rdb
echo "dump size: $(stat -c %%s /backup/dump.rdb 2>/dev/null || wc -c </backup/dump.rdb) bytes"
echo "verifying dump.rdb"
valkey-check-rdb /backup/dump.rdb
`, host, port, pwArg, tlsArgs)
	}

	s3 := vc.Spec.Backup.S3
	endpointFlag := ""
	if s3.Endpoint != "" {
		endpointFlag = s3EndpointURLFlag
	}
	prefix := s3.Prefix
	if prefix != "" && prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}
	// SSE flags appended to every `aws s3 cp` upload. Empty when SSE is off
	// and for the post-upload retention `aws s3 rm` calls (rm doesn't take
	// SSE flags). The "KMS" CRD value maps to AWS CLI's "aws:kms".
	sseFlag := ""
	switch s3.Encryption {
	case "AES256":
		sseFlag = " --sse AES256"
	case "KMS":
		sseFlag = " --sse aws:kms"
		if s3.KMSKeyID != "" {
			sseFlag += fmt.Sprintf(" --sse-kms-key-id %q", s3.KMSKeyID)
		}
	}

	// Retention block: after a successful upload we list objects under the
	// cluster's prefix, sort them by key (timestamp suffix is lexicographic),
	// and delete everything except the last N. N=0 disables the cleanup.
	// For Cluster topology each snapshot produces `shards` files, so we
	// multiply N by shards to keep the last N snapshots, not the last N files.
	retainFiles := vc.Spec.Backup.Retention
	if retainFiles > 0 && vc.Spec.Topology == cachev1beta1.TopologyCluster && vc.Spec.Shards != nil {
		// Each Cluster snapshot is `shards` RDBs + 1 manifest.txt, so keep
		// retention*(shards+1) files to retain whole snapshots together.
		retainFiles = vc.Spec.Backup.Retention * (*vc.Spec.Shards + 1)
	}
	retentionBlock := ""
	if retainFiles > 0 {
		retentionBlock = fmt.Sprintf(`echo "retention: keeping %[1]d newest files"
aws s3api list-objects-v2 --bucket "%[2]s" --prefix "%[3]s%[4]s-"%[5]s \
  --query 'Contents[].Key' --output text 2>/dev/null \
  | tr '\t' '\n' | sort | head -n -%[1]d \
  | while read -r KEY; do
      [ -z "$KEY" ] && continue
      echo "  deleting $KEY"
      aws s3 rm "s3://%[2]s/$KEY"%[5]s
    done
`, retainFiles, s3.Bucket, prefix, vc.Name, endpointFlag)
	}

	var uploadScript string
	if vc.Spec.Topology == cachev1beta1.TopologyCluster {
		// Each shard goes to a key like <prefix><cluster>-<stamp>-shardNN.rdb
		// so a single retention pass over <prefix><cluster>-* sees a full
		// "set" of files per timestamp and prunes whole sets together.
		uploadScript = fmt.Sprintf(`set -eu
STAMP=$(date -u +%%Y%%m%%d-%%H%%M%%S)
for f in /backup/shard-*.rdb; do
  IDX=$(basename "$f" .rdb)
  KEY=%[1]s%[2]s-${STAMP}-${IDX}.rdb
  echo "uploading to s3://%[3]s/${KEY}"
  aws s3 cp "$f" s3://%[3]s/${KEY}%[4]s%[6]s
done
MKEY=%[1]s%[2]s-${STAMP}-manifest.txt
echo "uploading manifest to s3://%[3]s/${MKEY}"
aws s3 cp /backup/manifest.txt s3://%[3]s/${MKEY}%[4]s%[6]s
echo "ok"
%[5]s`, prefix, vc.Name, s3.Bucket, endpointFlag, retentionBlock, sseFlag)
	} else {
		uploadScript = fmt.Sprintf(`set -eu
STAMP=$(date -u +%%Y%%m%%d-%%H%%M%%S)
KEY=%[1]s%[2]s-${STAMP}.rdb
echo "uploading to s3://%[3]s/${KEY}"
aws s3 cp /backup/dump.rdb s3://%[3]s/${KEY}%[4]s%[6]s
echo "ok"
%[5]s`, prefix, vc.Name, s3.Bucket, endpointFlag, retentionBlock, sseFlag)
	}

	env := []corev1.EnvVar{}
	if vc.Spec.Auth != nil && vc.Spec.Auth.Enabled {
		secretName := passwordSecretName(vc)
		if vc.Spec.Auth.ExistingSecret != "" {
			secretName = vc.Spec.Auth.ExistingSecret
		}
		env = append(env, corev1.EnvVar{
			Name: envValkeyPassword,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
					Key:                  secretKeyPassword,
				},
			},
		})
	}

	awsEnv := []corev1.EnvVar{
		{
			Name: envAWSAccessKey,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: s3.CredentialsSecret},
					Key:                  envAWSAccessKey,
				},
			},
		},
		{
			Name: envAWSSecretKey,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: s3.CredentialsSecret},
					Key:                  envAWSSecretKey,
				},
			},
		},
		{Name: envAWSDefaultRegion, Value: s3.Region},
		// aws-cli writes its CLI cache under $HOME; running non-root under the
		// restricted PSA, the default HOME may be unwritable, so point it at tmpfs.
		{Name: "HOME", Value: "/tmp"},
	}
	if s3.Endpoint != "" {
		awsEnv = append(awsEnv, corev1.EnvVar{Name: envS3EndpointURL, Value: s3.Endpoint})
	}

	mounts := []corev1.VolumeMount{{Name: backupVolumeName, MountPath: "/backup"}}
	if tlsEnabled(vc) {
		mounts = append(mounts, corev1.VolumeMount{Name: tlsVolumeName, MountPath: tlsMountPath, ReadOnly: true})
	}

	volumes := []corev1.Volume{
		{Name: backupVolumeName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
	}
	if tlsEnabled(vc) {
		volumes = append(volumes, corev1.Volume{
			Name:         tlsVolumeName,
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: tlsSecretName(vc)}},
		})
	}

	return &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: vc.Namespace, Labels: labels},
		Spec: batchv1.CronJobSpec{
			Schedule:          vc.Spec.Backup.Schedule,
			ConcurrencyPolicy: batchv1.ForbidConcurrent,
			JobTemplate: batchv1.JobTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: labels},
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyOnFailure,
							InitContainers: []corev1.Container{{
								Name:            "dump",
								Image:           dumpImage,
								Command:         []string{shellCmd, "-c", dumpScript},
								Env:             env,
								VolumeMounts:    mounts,
								SecurityContext: containerSecurityContext(vc),
							}},
							Containers: []corev1.Container{{
								Name:    "upload",
								Image:   awsImage,
								Command: []string{shellCmd, "-c", uploadScript},
								Env:     awsEnv,
								VolumeMounts: []corev1.VolumeMount{
									{Name: backupVolumeName, MountPath: "/backup"},
								},
								SecurityContext: containerSecurityContext(vc),
							}},
							Volumes:         volumes,
							SecurityContext: podSecurityContext(vc),
						},
					},
				},
			},
		},
	}
}
