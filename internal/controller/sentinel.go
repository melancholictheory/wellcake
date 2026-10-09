/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

const (
	sentinelPort    int32 = 26379
	sentinelTLSPort int32 = 26380
	// sentinelPlaneLabel marks the Sentinel pods (value "sentinel"); see sentinelPodLabels.
	sentinelPlaneLabel = "valkey.wellcake.io/plane"
	sentinelMasterName = "mymaster"
	sentinelConfigName = "sentinel.conf"
	// sentinelACLUser is the dedicated ACL user (seeded on the data nodes by
	// renderInitScript) that Sentinel authenticates as when reaching the
	// monitored master — least data exposure vs the default user.
	sentinelACLUser = "sentinel-user"
	// sentinelACLCommands is the minimal command set this user needs to monitor
	// and fail over the data nodes — the canonical Redis/Valkey Sentinel ACL
	// recommendation: health/role checks (ping/info/role), the
	// __sentinel__:hello pub/sub (subscribe/publish, paired with the &* channel
	// glob), the failover transaction (multi/exec + slaveof a.k.a. REPLICAOF),
	// config|rewrite to persist the new topology, and client|kill / script|kill
	// to interrupt clients and a running script mid-failover. No key glob — the
	// user can never read or write your data.
	sentinelACLCommands = "+multi +slaveof +ping +exec +subscribe " +
		"+config|rewrite +role +publish +info +client|setname +client|kill +script|kill"
	// replicationACLUser is the dedicated ACL user a REPLICA authenticates as when
	// it connects to its primary (masteruser/masterauth), instead of the
	// full-access default user. If the replication credential ever leaks it can do
	// nothing but replicate: no key access, no arbitrary commands.
	replicationACLUser = "replicator"
	// replicationACLCommands is the minimal set a replica needs on its primary:
	// PSYNC to start/continue the replication stream, REPLCONF for the handshake
	// and ACKs, and PING for keepalive. The stream itself is not ACL-checked per
	// key, so no key glob is granted.
	replicationACLCommands = "+psync +replconf +ping"
)

// reconcileSentinel brings up Replication primitives plus a separate
// StatefulSet of Sentinel pods that monitor the primary and elect a new one
// on its failure. Once Sentinel is up the operator's own failover loop is
// disabled — Sentinel quorum is authoritative.
func (r *ValkeyClusterReconciler) reconcileSentinel(ctx context.Context, vc *cachev1beta1.ValkeyCluster) (ctrl.Result, error) {
	if vc.Spec.Sentinel == nil || vc.Spec.Sentinel.Replicas < 3 {
		return r.setPhase(ctx, vc,
			"InvalidSpec", "sentinel.replicas must be >= 3 (recommended odd for quorum)")
	}

	password, err := r.ensurePasswordSecret(ctx, vc)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("password secret: %w", err)
	}
	if err := r.ensureHeadlessService(ctx, vc); err != nil {
		return ctrl.Result{}, fmt.Errorf("headless service: %w", err)
	}
	if err := r.ensureClientService(ctx, vc); err != nil {
		return ctrl.Result{}, fmt.Errorf("client service: %w", err)
	}
	configHash, err := r.ensureConfigMap(ctx, vc, password)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("configmap: %w", err)
	}
	sts, err := r.ensureStatefulSet(ctx, vc, configHash)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("statefulset: %w", err)
	}
	// Data budget first: it stops counting the Sentinel pods before their own
	// budget starts, so no pod is ever covered by both (eviction rejects that).
	if err := r.ensurePDB(ctx, vc); err != nil {
		return ctrl.Result{}, fmt.Errorf("pdb: %w", err)
	}
	if err := r.applyPDB(ctx, vc, buildSentinelPDB(vc)); err != nil {
		return ctrl.Result{}, fmt.Errorf("sentinel pdb: %w", err)
	}
	if err := r.ensureNetworkPolicy(ctx, vc); err != nil {
		return ctrl.Result{}, fmt.Errorf("networkpolicy: %w", err)
	}
	if err := r.ensureMetricsServiceMonitor(ctx, vc); err != nil {
		logf.FromContext(ctx).Error(err, "metrics ServiceMonitor")
	}

	// Sentinel-specific objects.
	if err := r.ensureSentinelConfigMap(ctx, vc, password); err != nil {
		return ctrl.Result{}, fmt.Errorf("sentinel configmap: %w", err)
	}
	if err := r.ensureSentinelService(ctx, vc); err != nil {
		return ctrl.Result{}, fmt.Errorf("sentinel service: %w", err)
	}
	sentinelSTS, err := r.ensureSentinelStatefulSet(ctx, vc)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("sentinel statefulset: %w", err)
	}

	if err := r.ensureBackupCronJob(ctx, vc); err != nil {
		return ctrl.Result{}, fmt.Errorf("backup cronjob: %w", err)
	}

	// Proactive rolling restart (ADR 0004), Sentinel topology: when opted in and a
	// rollout is pending (some data or Sentinel pod is on a stale STS revision),
	// the operator owns the rollout under OnDelete — roll data replicas, hand the
	// master over via SENTINEL FAILOVER, then roll the Sentinel pods. While in
	// flight we report Updating and requeue quickly to drive the next step; with
	// nothing pending this is a no-op and the normal readiness status applies.
	if proactiveRolloutEnabled(vc) && sts.Status.ReadyReplicas > 0 {
		inProgress, rerr := r.driveSentinelRollout(ctx, vc, password)
		if rerr != nil {
			return ctrl.Result{}, rerr
		}
		if inProgress {
			patch := client.MergeFrom(vc.DeepCopy())
			vc.Status.Phase = cachev1beta1.PhaseUpdating
			vc.Status.ObservedGeneration = vc.Generation
			vc.Status.ReadyReplicas = sts.Status.ReadyReplicas
			setCondition(&vc.Status.Conditions, metav1.Condition{
				Type:               cachev1beta1.ConditionAvailable,
				Status:             condStatus(false),
				Reason:             string(cachev1beta1.PhaseUpdating),
				Message:            "proactive rolling restart in progress",
				ObservedGeneration: vc.Generation,
				LastTransitionTime: metav1.Now(),
			})
			if err := r.Status().Patch(ctx, vc, patch); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}

	// Status: ReadyReplicas counts only Valkey data pods. Sentinel readiness
	// is reflected via a separate condition.
	patch := client.MergeFrom(vc.DeepCopy())
	vc.Status.ReadyReplicas = sts.Status.ReadyReplicas
	vc.Status.ObservedGeneration = vc.Generation
	phase := cachev1beta1.PhaseCreating
	if sts.Status.ReadyReplicas == *sts.Spec.Replicas && sentinelSTS.Status.ReadyReplicas == *sentinelSTS.Spec.Replicas {
		phase = cachev1beta1.PhaseReady
	}
	vc.Status.Phase = phase
	setCondition(&vc.Status.Conditions, metav1.Condition{
		Type:               cachev1beta1.ConditionAvailable,
		Status:             condStatus(phase == cachev1beta1.PhaseReady),
		Reason:             string(phase),
		Message:            fmt.Sprintf("valkey %d/%d, sentinel %d/%d", sts.Status.ReadyReplicas, *sts.Spec.Replicas, sentinelSTS.Status.ReadyReplicas, *sentinelSTS.Spec.Replicas),
		ObservedGeneration: vc.Generation,
		LastTransitionTime: metav1.Now(),
	})
	return ctrl.Result{}, r.Status().Patch(ctx, vc, patch)
}

// renderSentinelConf assembles the sentinel.conf served via ConfigMap. The
// monitored master starts as pod-0; Sentinel will rewrite this file in place
// after a failover, so we mount via a directory copy rather than directly.
func renderSentinelConf(vc *cachev1beta1.ValkeyCluster, password string) string {
	quorum := vc.Spec.Sentinel.Quorum
	if quorum == 0 {
		quorum = vc.Spec.Sentinel.Replicas/2 + 1
	}
	primary := fmt.Sprintf("%s-0.%s.%s.svc.cluster.local", statefulSetName(vc), headlessServiceName(vc), vc.Namespace)
	port := valkeyPort
	if tlsEnabled(vc) {
		port = valkeyTLSPort
	}

	conf := fmt.Sprintf(`port %d
dir %s
sentinel monitor %s %s %d %d
sentinel down-after-milliseconds %s 5000
sentinel failover-timeout %s 60000
sentinel parallel-syncs %s 1
sentinel resolve-hostnames yes
sentinel announce-hostnames yes
`, sentinelPort, dataMountPath, sentinelMasterName, primary, port, quorum, sentinelMasterName, sentinelMasterName, sentinelMasterName)

	if password != "" {
		// Authenticate to the monitored master as the dedicated least-data-exposure
		// ACL user (seeded on the data nodes) rather than the default user, and
		// keep requirepass for client/inter-sentinel auth on the Sentinel port.
		quotedPassword := valkeyConfigArg(password)
		conf += fmt.Sprintf("sentinel auth-user %s %s\nsentinel auth-pass %s %s\nrequirepass %s\n",
			sentinelMasterName, sentinelACLUser, sentinelMasterName, quotedPassword, quotedPassword)
	}
	if tlsEnabled(vc) {
		conf += fmt.Sprintf(`tls-port %d
port 0
tls-cert-file %s/tls.crt
tls-key-file %s/tls.key
tls-ca-cert-file %s/ca.crt
tls-replication yes
tls-auth-clients optional
`, sentinelTLSPort, tlsMountPath, tlsMountPath, tlsMountPath)
	}
	return conf
}

// sentinelListenPort is the port Sentinel actually serves on. With TLS,
// renderSentinelConf moves it to tls-port and sets `port 0`, so the container
// port, the probes, the Service and the operator's own dial must all follow it.
func sentinelListenPort(vc *cachev1beta1.ValkeyCluster) int32 {
	if tlsEnabled(vc) {
		return sentinelTLSPort
	}
	return sentinelPort
}

func (r *ValkeyClusterReconciler) ensureSentinelConfigMap(ctx context.Context, vc *cachev1beta1.ValkeyCluster, password string) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      vc.Name + "-sentinel-config",
			Namespace: vc.Namespace,
			Labels:    sentinelLabels(vc),
		},
		Data: map[string]string{sentinelConfigName: renderSentinelConf(vc, password)},
	}
	if err := controllerutil.SetControllerReference(vc, cm, r.Scheme); err != nil {
		return err
	}
	var existing corev1.ConfigMap
	err := r.Get(ctx, client.ObjectKeyFromObject(cm), &existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, cm)
	}
	if err != nil {
		return err
	}
	existing.Data = cm.Data
	existing.Labels = cm.Labels
	return r.Update(ctx, &existing)
}

func (r *ValkeyClusterReconciler) ensureSentinelService(ctx context.Context, vc *cachev1beta1.ValkeyCluster) error {
	port := sentinelListenPort(vc)
	// Select on sentinelPlaneLabel only once every Sentinel pod carries it.
	// Switching earlier would drop the unlabeled pods from the headless Service
	// mid-rollout, and with them the DNS names the Sentinels use to reach each
	// other.
	selector := sentinelLabels(vc)
	labeled, err := r.sentinelPodsLabeled(ctx, vc)
	if err != nil {
		return err
	}
	if labeled {
		selector = sentinelPodLabels(vc)
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentinelStatefulSetName(vc),
			Namespace: vc.Namespace,
			Labels:    sentinelLabels(vc),
		},
		Spec: corev1.ServiceSpec{
			Type:                     corev1.ServiceTypeClusterIP,
			ClusterIP:                corev1.ClusterIPNone,
			PublishNotReadyAddresses: true,
			Selector:                 selector,
			Ports: []corev1.ServicePort{{
				Name:       componentSentinel,
				Port:       port,
				TargetPort: intstr.FromString(componentSentinel),
				Protocol:   corev1.ProtocolTCP,
			}},
		},
	}
	if err := controllerutil.SetControllerReference(vc, svc, r.Scheme); err != nil {
		return err
	}
	return r.applyService(ctx, svc)
}

// sentinelPodsLabeled reports whether every existing Sentinel pod carries
// sentinelPlaneLabel (true when there are none yet).
func (r *ValkeyClusterReconciler) sentinelPodsLabeled(ctx context.Context, vc *cachev1beta1.ValkeyCluster) (bool, error) {
	var list corev1.PodList
	if err := r.List(ctx, &list, client.InNamespace(vc.Namespace), client.MatchingLabels(sentinelLabels(vc))); err != nil {
		return false, err
	}
	sts := sentinelStatefulSetName(vc)
	for i := range list.Items {
		p := &list.Items[i]
		if _, ok := ordinalFromPodName(p.Name, sts); !ok {
			continue // a data pod: same labels, different StatefulSet
		}
		if p.Labels[sentinelPlaneLabel] != componentSentinel {
			return false, nil
		}
	}
	return true, nil
}

func (r *ValkeyClusterReconciler) ensureSentinelStatefulSet(ctx context.Context, vc *cachev1beta1.ValkeyCluster) (*appsv1.StatefulSet, error) {
	sts := buildSentinelStatefulSet(vc, proactiveRolloutEnabled(vc))
	if err := controllerutil.SetControllerReference(vc, sts, r.Scheme); err != nil {
		return nil, err
	}
	var existing appsv1.StatefulSet
	err := r.Get(ctx, client.ObjectKeyFromObject(sts), &existing)
	if apierrors.IsNotFound(err) {
		if err := r.Create(ctx, sts); err != nil {
			return nil, err
		}
		return sts, nil
	}
	if err != nil {
		return nil, err
	}
	existing.Spec.Replicas = sts.Spec.Replicas
	existing.Spec.Template = sts.Spec.Template
	existing.Labels = sts.Labels
	if err := r.Update(ctx, &existing); err != nil {
		return nil, err
	}
	return &existing, nil
}

func buildSentinelStatefulSet(vc *cachev1beta1.ValkeyCluster, proactive bool) *appsv1.StatefulSet {
	labels := sentinelLabels(vc)
	replicas := vc.Spec.Sentinel.Replicas
	image := vc.Spec.Sentinel.Image
	if image == "" {
		image = vc.Spec.Image
	}
	port := sentinelListenPort(vc)

	volumeMounts := []corev1.VolumeMount{
		{Name: configVolumeName, MountPath: "/etc/sentinel", ReadOnly: true},
		{Name: dataVolumeName, MountPath: dataMountPath},
	}
	if tlsEnabled(vc) {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{Name: tlsVolumeName, MountPath: tlsMountPath, ReadOnly: true})
	}

	volumes := []corev1.Volume{
		{
			Name: "config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: vc.Name + "-sentinel-config"},
				},
			},
		},
	}
	if tlsEnabled(vc) {
		volumes = append(volumes, corev1.Volume{
			Name:         tlsVolumeName,
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: tlsSecretName(vc)}},
		})
	}

	pvc := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: dataVolumeName, Labels: labels},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
		},
	}
	if vc.Spec.Storage != nil && vc.Spec.Storage.StorageClassName != nil {
		pvc.Spec.StorageClassName = vc.Spec.Storage.StorageClassName
	}

	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentinelStatefulSetName(vc),
			Namespace: vc.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			// OnDelete when opted into the proactive rollout (ADR 0004): the
			// operator drives the Sentinel pods one at a time itself; otherwise the
			// StatefulSet controller's RollingUpdate handles them.
			UpdateStrategy: updateStrategyFor(proactive),
			ServiceName:    sentinelStatefulSetName(vc),
			Selector:       &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: sentinelPodLabels(vc)},
				Spec: corev1.PodSpec{
					InitContainers: []corev1.Container{{
						Name:            "sentinel-init",
						Image:           image,
						Command:         []string{shellCmd, "-c", renderSentinelInitScript(vc)},
						Env:             sentinelInitEnv(vc),
						VolumeMounts:    volumeMounts,
						SecurityContext: containerSecurityContext(vc),
					}},
					Containers: []corev1.Container{{
						Name:    componentSentinel,
						Image:   image,
						Command: []string{"valkey-server"},
						Args:    []string{dataMountPath + "/runtime-sentinel.conf", "--sentinel"},
						Ports: []corev1.ContainerPort{{
							Name:          componentSentinel,
							ContainerPort: port,
							Protocol:      corev1.ProtocolTCP,
						}},
						VolumeMounts:    volumeMounts,
						ReadinessProbe:  tcpProbe(port, 5, 5),
						LivenessProbe:   tcpProbe(port, 15, 20),
						SecurityContext: containerSecurityContext(vc),
					}},
					Volumes:                   volumes,
					ImagePullSecrets:          vc.Spec.ImagePullSecrets,
					SecurityContext:           podSecurityContext(vc),
					Affinity:                  defaultAntiAffinity(vc),
					TopologySpreadConstraints: defaultTopologySpread(vc),
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{pvc},
		},
	}
}

func sentinelLabels(vc *cachev1beta1.ValkeyCluster) map[string]string {
	l := labelsFor(vc)
	l[componentLabel] = componentSentinel
	return l
}

// sentinelPodLabels are the Sentinel pod template labels: sentinelLabels plus
// sentinelPlaneLabel. The data pods carry the same sentinelLabels, and the
// StatefulSet selectors (immutable) cannot change, so only this extra label
// tells the two workloads apart for the headless Sentinel Service.
func sentinelPodLabels(vc *cachev1beta1.ValkeyCluster) map[string]string {
	l := sentinelLabels(vc)
	l[sentinelPlaneLabel] = componentSentinel
	return l
}

// sentinelInitEnv gives the init container the password it needs to ask the
// other Sentinels for the current primary.
func sentinelInitEnv(vc *cachev1beta1.ValkeyCluster) []corev1.EnvVar {
	if !vc.Spec.AuthEnabled() {
		return nil
	}
	return []corev1.EnvVar{{Name: envValkeyPassword, ValueFrom: secretRef(authSecretName(vc), secretKeyPassword)}}
}

// renderSentinelInitScript builds runtime-sentinel.conf from the ConfigMap on
// every start, so auth, TLS and timing changes always apply. Two things carry
// over instead:
//   - the Sentinel's identity (myid, current-epoch) from the previous
//     runtime-sentinel.conf on the PVC. A new ID on every restart leaves the old
//     one with the other Sentinels as a dead peer, which counts against the
//     majority a failover needs.
//   - the monitored primary, taken from the other Sentinels (the answer with the
//     highest config-epoch). The ConfigMap only knows pod-0, so without this a
//     re-created Sentinel reports pod-0 as the primary until hello messages
//     correct it. With no answer (first bootstrap) the ConfigMap value stands.
func renderSentinelInitScript(vc *cachev1beta1.ValkeyCluster) string {
	auth := ""
	if vc.Spec.AuthEnabled() {
		auth = noAuthWarningArgs
	}
	tlsArgs := ""
	if tlsEnabled(vc) {
		tlsArgs = fmt.Sprintf(" --tls --cacert %[1]s/%[2]s --cert %[1]s/%[3]s --key %[1]s/%[4]s",
			tlsMountPath, secretKeyTLSCACert, secretKeyTLSCert, secretKeyTLSKey)
	}
	return fmt.Sprintf(`set -eu
RT=%[1]s/runtime-sentinel.conf
NEW="$RT.new"
cp /etc/sentinel/%[2]s "$NEW"
if [ -f "$RT" ]; then
  awk '$1 == "sentinel" && ($2 == "myid" || $2 == "current-epoch")' "$RT" >> "$NEW"
fi
STS=%[3]s
ME="${HOSTNAME##*-}"
BEST_EPOCH=-1
BEST_HOST=""
BEST_PORT=""
i=0
while [ "$i" -lt %[4]d ]; do
  if [ "$i" != "$ME" ]; then
    OUT=$(timeout 3 valkey-cli -h "$STS-$i.$STS" -p %[5]d%[6]s%[7]s SENTINEL MASTER %[8]s 2>/dev/null || true)
    set -- $(printf '%%s
' "$OUT" | awk 'NR %% 2 == 1 { k = $0 } NR %% 2 == 0 { v[k] = $0 } END { if (v["ip"] != "" && v["config-epoch"] ~ /^[0-9]+$/) print v["ip"], v["port"], v["config-epoch"] }')
    if [ "$#" -eq 3 ] && [ "$3" -gt "$BEST_EPOCH" ]; then
      BEST_HOST=$1
      BEST_PORT=$2
      BEST_EPOCH=$3
    fi
  fi
  i=$((i + 1))
done
if [ -n "$BEST_HOST" ]; then
  echo "monitoring $BEST_HOST:$BEST_PORT (config-epoch $BEST_EPOCH) as reported by the other Sentinels"
  awk -v h="$BEST_HOST" -v p="$BEST_PORT" '$1 == "sentinel" && $2 == "monitor" && $3 == "%[8]s" { $4 = h; $5 = p } { print }' "$NEW" > "$NEW.m"
  mv "$NEW.m" "$NEW"
  echo "sentinel config-epoch %[8]s $BEST_EPOCH" >> "$NEW"
fi
mv "$NEW" "$RT"
`, dataMountPath, sentinelConfigName, sentinelStatefulSetName(vc), vc.Spec.Sentinel.Replicas,
		sentinelListenPort(vc), tlsArgs, auth, sentinelMasterName)
}

// sentinelPodNames lists the Sentinel StatefulSet's pod names. Selectors use
// them because the Sentinel pods carry the same labels as the data pods.
func sentinelPodNames(vc *cachev1beta1.ValkeyCluster) []string {
	if vc.Spec.Sentinel == nil {
		return nil
	}
	names := make([]string, 0, vc.Spec.Sentinel.Replicas)
	for i := int32(0); i < vc.Spec.Sentinel.Replicas; i++ {
		names = append(names, fmt.Sprintf("%s-%d", sentinelStatefulSetName(vc), i))
	}
	return names
}

// buildSentinelPDB keeps a drain from evicting more than one Sentinel at a time,
// so the quorum survives. It is separate from the data budget, which excludes
// the Sentinel pods, and does not take spec.podDisruptionBudget values: those
// are sized for the data pods.
func buildSentinelPDB(vc *cachev1beta1.ValkeyCluster) *policyv1.PodDisruptionBudget {
	maxU := intstr.FromInt32(1)
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentinelStatefulSetName(vc) + "-pdb",
			Namespace: vc.Namespace,
			Labels:    sentinelLabels(vc),
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: sentinelLabels(vc),
				MatchExpressions: []metav1.LabelSelectorRequirement{{
					Key: appsv1.StatefulSetPodNameLabel, Operator: metav1.LabelSelectorOpIn, Values: sentinelPodNames(vc),
				}},
			},
			MaxUnavailable: &maxU,
		},
	}
}
