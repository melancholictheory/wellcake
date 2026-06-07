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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

const (
	sentinelPort       int32 = 26379
	sentinelMasterName       = "mymaster"
	sentinelConfigName       = "sentinel.conf"
	// sentinelACLUser is the dedicated ACL user (seeded on the data nodes by
	// renderInitScript) that Sentinel authenticates as when reaching the
	// monitored master — least data exposure vs the default user.
	sentinelACLUser = "sentinel-user"
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
	if err := r.ensurePDB(ctx, vc); err != nil {
		return ctrl.Result{}, fmt.Errorf("pdb: %w", err)
	}
	if err := r.ensureNetworkPolicy(ctx, vc); err != nil {
		return ctrl.Result{}, fmt.Errorf("networkpolicy: %w", err)
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
		conf += fmt.Sprintf("sentinel auth-user %s %s\nsentinel auth-pass %s %s\nrequirepass %s\n",
			sentinelMasterName, sentinelACLUser, sentinelMasterName, password, password)
	}
	if tlsEnabled(vc) {
		conf += fmt.Sprintf(`tls-port %d
port 0
tls-cert-file %s/tls.crt
tls-key-file %s/tls.key
tls-ca-cert-file %s/ca.crt
tls-replication yes
tls-auth-clients optional
`, sentinelPort+1, tlsMountPath, tlsMountPath, tlsMountPath)
	}
	return conf
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
			Selector:                 sentinelLabels(vc),
			Ports: []corev1.ServicePort{{
				Name:       componentSentinel,
				Port:       sentinelPort,
				TargetPort: intstr.FromInt32(sentinelPort),
				Protocol:   corev1.ProtocolTCP,
			}},
		},
	}
	if err := controllerutil.SetControllerReference(vc, svc, r.Scheme); err != nil {
		return err
	}
	return r.applyService(ctx, svc)
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

	// Sentinel rewrites its config file in place on failover, so we copy
	// it from the ConfigMap to /data on each start (the initContainer pattern).
	initScript := fmt.Sprintf("set -eu\ncp /etc/sentinel/%s %s/runtime-sentinel.conf\n", sentinelConfigName, dataMountPath)

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
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					InitContainers: []corev1.Container{{
						Name:    "sentinel-init",
						Image:   image,
						Command: []string{shellCmd, "-c", initScript},
						VolumeMounts: []corev1.VolumeMount{
							{Name: configVolumeName, MountPath: "/etc/sentinel", ReadOnly: true},
							{Name: dataVolumeName, MountPath: dataMountPath},
						},
					}},
					Containers: []corev1.Container{{
						Name:    componentSentinel,
						Image:   image,
						Command: []string{"valkey-server"},
						Args:    []string{dataMountPath + "/runtime-sentinel.conf", "--sentinel"},
						Ports: []corev1.ContainerPort{{
							Name:          componentSentinel,
							ContainerPort: sentinelPort,
							Protocol:      corev1.ProtocolTCP,
						}},
						VolumeMounts:   volumeMounts,
						ReadinessProbe: tcpProbe(sentinelPort, 5, 5),
						LivenessProbe:  tcpProbe(sentinelPort, 15, 20),
					}},
					Volumes: volumes,
					SecurityContext: &corev1.PodSecurityContext{
						FSGroup:   ptr.To[int64](1000),
						RunAsUser: ptr.To[int64](1000),
					},
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
