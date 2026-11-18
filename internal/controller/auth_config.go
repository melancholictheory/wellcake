/*
Copyright 2026 The Wellcake Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

const (
	exporterACLUser             = cachev1beta1.ExporterACLUser
	exporterACLCommands         = "+ping +info +client|setname +cluster|info +slowlog|len +slowlog|get +latency|latest +latency|histogram"
	sentinelExporterACLCommands = "+ping +info +client|setname +sentinel|masters +sentinel|ckquorum +sentinel|sentinels +sentinel|slaves"
)

// Encode bytes as Valkey's double-quoted hex escapes. This preserves leading,
// embedded and trailing newlines as well as quotes and backslashes.
const shellConfigArg = `valkey_config_arg() {
  printf '"'
  od -An -v -tx1 | tr -d '[:space:]' | sed 's/../\\x&/g'
  printf '"'
}
`

// Init containers run once per Pod. Refresh the runtime credentials on every
// server start so a container restart after live rotation uses the current Secret.
func refreshRuntimeAuth(sentinel bool) string {
	if sentinel {
		return "set -eu\numask 077\n" + shellConfigArg + fmt.Sprintf(`
CONF=%[1]s/runtime-sentinel.conf
awk 'tolower($1) != "requirepass" && !(tolower($1) == "sentinel" && (tolower($2) == "auth-pass" || tolower($2) == "sentinel-pass")) && !(tolower($1) == "user" && $2 == "%[3]s")' "$CONF" > "$CONF.auth"
PASSWORD_ARG=$(printf '%%s' "$VALKEY_PASSWORD" | valkey_config_arg)
printf 'sentinel auth-pass %[2]s %%s\nsentinel sentinel-pass %%s\nrequirepass %%s\n' "$PASSWORD_ARG" "$PASSWORD_ARG" "$PASSWORD_ARG" >> "$CONF.auth"
if [ -n "${WELLCAKE_INTERNAL_PASSWORD:-}" ]; then
  EXPORTER_HASH=$(printf '%%s' "$WELLCAKE_INTERNAL_PASSWORD" | sha256sum | awk '{print $1}')
  printf 'user %[3]s on #%%s %[4]s\n' "$EXPORTER_HASH" >> "$CONF.auth"
fi
mv "$CONF.auth" "$CONF"
`, dataMountPath, sentinelMasterName, exporterACLUser, sentinelExporterACLCommands)
	}
	return `set -eu
umask 077
` + shellConfigArg + `
CONF=/data/runtime.conf
awk 'tolower($1) != "requirepass" && tolower($1) != "masterauth"' "$CONF" > "$CONF.auth"
PASSWORD_ARG=$(printf '%s' "$VALKEY_PASSWORD" | valkey_config_arg)
MASTER_PASSWORD_ARG=$(printf '%s' "${SOURCE_PASSWORD:-$VALKEY_PASSWORD}" | valkey_config_arg)
printf 'requirepass %s\nmasterauth %s\n' "$PASSWORD_ARG" "$MASTER_PASSWORD_ARG" >> "$CONF.auth"
mv "$CONF.auth" "$CONF"
`
}

func internalAuthSecretName(vc *cachev1beta1.ValkeyCluster) string {
	// Use the CR UID to avoid collisions with a previous cluster that
	// is still being garbage-collected or a user-managed Secret.
	suffix := configHashFromData(map[string]string{"uid": string(vc.UID)})
	return vc.Name + "-internal-auth-" + suffix
}

func internalAuthPasswordEnv(vc *cachev1beta1.ValkeyCluster) corev1.EnvVar {
	return corev1.EnvVar{Name: "WELLCAKE_INTERNAL_PASSWORD", ValueFrom: secretRef(internalAuthSecretName(vc), secretKeyPassword)}
}

// internalACLInit reserves a stable INFO credential for readiness. The same
// user gets the exporter commands when metrics are enabled.
func internalACLInit(vc *cachev1beta1.ValkeyCluster) string {
	if !vc.Spec.AuthEnabled() {
		return fmt.Sprintf(`if grep -q "^user %[1]s " %[2]s/users.acl; then
  grep -v "^user %[1]s " %[2]s/users.acl > %[2]s/users.acl.new || true
  mv %[2]s/users.acl.new %[2]s/users.acl
fi
`, exporterACLUser, dataMountPath)
	}
	commands := "+info"
	if metricsEnabled(vc) {
		commands = exporterACLCommands
	}
	return fmt.Sprintf(`EXPORTER_HASH=$(printf '%%s' "$WELLCAKE_INTERNAL_PASSWORD" | sha256sum | awk '{print $1}')
grep -v "^user %[1]s " %[2]s/users.acl > %[2]s/users.acl.new || true
printf 'user %[1]s on #%%s %[3]s\n' "$EXPORTER_HASH" >> %[2]s/users.acl.new
mv %[2]s/users.acl.new %[2]s/users.acl
`, exporterACLUser, dataMountPath, commands)
}

func (r *ValkeyClusterReconciler) authConfigHash(ctx context.Context, vc *cachev1beta1.ValkeyCluster, configHash, password string, internalAuth bool) (string, error) {
	if !vc.Spec.AuthEnabled() || (!internalAuth && vc.Spec.Auth.ExistingSecret == "") {
		return configHash, nil
	}
	fingerprint := map[string]string{"config": configHash}
	if vc.Spec.Auth.ExistingSecret != "" {
		fingerprint["existingSecret"] = password
	}
	if internalAuth {
		internalPassword, err := r.ensureInternalAuthSecret(ctx, vc)
		if err != nil {
			return "", err
		}
		fingerprint["internalAuth"] = internalPassword
	}
	return configHashFromData(fingerprint), nil
}

func (r *ValkeyClusterReconciler) ensureInternalAuthSecret(ctx context.Context, vc *cachev1beta1.ValkeyCluster) (string, error) {
	key := client.ObjectKey{Namespace: vc.Namespace, Name: internalAuthSecretName(vc)}
	var existing corev1.Secret
	if err := r.Get(ctx, key, &existing); err == nil {
		if !metav1.IsControlledBy(&existing, vc) {
			return "", fmt.Errorf("internal auth Secret %s has an unexpected owner", key)
		}
		if password := string(existing.Data[secretKeyPassword]); password != "" {
			return password, nil
		}
		password, err := generatePassword()
		if err != nil {
			return "", err
		}
		if existing.Data == nil {
			existing.Data = make(map[string][]byte)
		}
		existing.Data[secretKeyPassword] = []byte(password)
		return password, r.Update(ctx, &existing)
	} else if !apierrors.IsNotFound(err) {
		return "", err
	}

	password, err := generatePassword()
	if err != nil {
		return "", err
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name, Labels: labelsFor(vc)},
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{secretKeyPassword: []byte(password)},
	}
	if err := controllerutil.SetControllerReference(vc, secret, r.Scheme); err != nil {
		return "", err
	}
	if err := r.Create(ctx, secret); err != nil {
		if apierrors.IsAlreadyExists(err) && r.APIReader != nil {
			if readErr := r.APIReader.Get(ctx, key, &existing); readErr != nil {
				return "", readErr
			}
			if value := string(existing.Data[secretKeyPassword]); metav1.IsControlledBy(&existing, vc) && value != "" {
				return value, nil
			}
		}
		return "", err
	}
	return password, nil
}

// The old init containers require their original ConfigMaps on pod recreation.
// Remove those ConfigMaps only after every current template and pod has migrated.
func (r *ValkeyClusterReconciler) cleanLegacyAuthConfig(ctx context.Context, vc *cachev1beta1.ValkeyCluster) error {
	var configMaps corev1.ConfigMapList
	if err := r.List(ctx, &configMaps, client.InNamespace(vc.Namespace), client.MatchingLabels{instanceLabel: vc.Name}); err != nil {
		return err
	}
	for i := range configMaps.Items {
		cm := &configMaps.Items[i]
		if cm.Name != vc.Name+"-config" && cm.Name != vc.Name+"-sentinel-config" {
			continue
		}
		if !metav1.IsControlledBy(cm, vc) {
			continue
		}
		inUse, err := r.legacyConfigInUse(ctx, vc, cm.Name)
		if err != nil {
			return err
		}
		if !inUse {
			if err := r.Delete(ctx, cm, client.Preconditions{UID: &cm.UID}); client.IgnoreNotFound(err) != nil {
				return err
			}
		}
	}
	return nil
}

func (r *ValkeyClusterReconciler) legacyConfigInUse(ctx context.Context, vc *cachev1beta1.ValkeyCluster, name string) (bool, error) {
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	namespace := client.InNamespace(vc.Namespace)
	usesConfig := func(spec corev1.PodSpec) bool {
		for _, volume := range spec.Volumes {
			if volume.ConfigMap != nil && volume.ConfigMap.Name == name {
				return true
			}
		}
		return false
	}
	var sets appsv1.StatefulSetList
	if err := reader.List(ctx, &sets, namespace); err != nil {
		return false, err
	}
	for i := range sets.Items {
		sts := &sets.Items[i]
		if usesConfig(sts.Spec.Template.Spec) || (metav1.IsControlledBy(sts, vc) && sts.Status.ObservedGeneration < sts.Generation) {
			return true, nil
		}
		// RollingUpdate can recreate a pod from CurrentRevision until it
		// completes. OnDelete uses the updated template for new pods.
		if metav1.IsControlledBy(sts, vc) && sts.Spec.UpdateStrategy.Type != appsv1.OnDeleteStatefulSetStrategyType &&
			sts.Status.CurrentRevision != sts.Status.UpdateRevision {
			return true, nil
		}
	}
	var pods corev1.PodList
	if err := reader.List(ctx, &pods, namespace); err != nil {
		return false, err
	}
	for i := range pods.Items {
		if usesConfig(pods.Items[i].Spec) {
			return true, nil
		}
	}
	return false, nil
}
