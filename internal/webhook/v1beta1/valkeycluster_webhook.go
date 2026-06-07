/*
Copyright 2026 The Wellcake Authors.
*/

package v1beta1

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

var valkeyclusterlog = logf.Log.WithName("valkeycluster-resource")

// SetupValkeyClusterWebhookWithManager registers the webhook for ValkeyCluster
// in the manager. It captures mgr.GetClient() because the validator's main job
// is reading referenced Secrets — checks CEL XValidation can't do.
func SetupValkeyClusterWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &cachev1beta1.ValkeyCluster{}).
		WithValidator(&ValkeyClusterCustomValidator{Client: mgr.GetClient()}).
		WithDefaulter(&ValkeyClusterCustomDefaulter{}).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-cache-wellcake-io-v1beta1-valkeycluster,mutating=true,failurePolicy=fail,sideEffects=None,groups=cache.wellcake.io,resources=valkeyclusters,verbs=create;update,versions=v1beta1,name=mvalkeycluster-v1beta1.kb.io,admissionReviewVersions=v1

// ValkeyClusterCustomDefaulter applies spec defaults at admission so the stored
// object already carries them — `kubectl get` shows the effective spec and the
// reconciler never races a half-defaulted object. The defaulting logic itself
// lives on the API type (ValkeyCluster.Default) so it is shared with the
// defensive reconcile-side call.
type ValkeyClusterCustomDefaulter struct{}

func (d *ValkeyClusterCustomDefaulter) Default(_ context.Context, vc *cachev1beta1.ValkeyCluster) error {
	vc.Default()
	return nil
}

// +kubebuilder:webhook:path=/validate-cache-wellcake-io-v1beta1-valkeycluster,mutating=false,failurePolicy=fail,sideEffects=None,groups=cache.wellcake.io,resources=valkeyclusters,verbs=create;update,versions=v1beta1,name=vvalkeycluster-v1beta1.kb.io,admissionReviewVersions=v1

// ValkeyClusterCustomValidator validates Secret references and other
// cross-resource invariants that CEL XValidation cannot express.
type ValkeyClusterCustomValidator struct {
	Client client.Client
}

func (v *ValkeyClusterCustomValidator) ValidateCreate(ctx context.Context, obj *cachev1beta1.ValkeyCluster) (admission.Warnings, error) {
	valkeyclusterlog.V(1).Info("validate create", "name", obj.GetName())
	return v.validate(ctx, obj)
}

func (v *ValkeyClusterCustomValidator) ValidateUpdate(ctx context.Context, _, newObj *cachev1beta1.ValkeyCluster) (admission.Warnings, error) {
	valkeyclusterlog.V(1).Info("validate update", "name", newObj.GetName())
	return v.validate(ctx, newObj)
}

func (v *ValkeyClusterCustomValidator) ValidateDelete(_ context.Context, _ *cachev1beta1.ValkeyCluster) (admission.Warnings, error) {
	return nil, nil
}

// referencedSecrets maps each Secret name the spec references to the field path
// that references it (for error messages). Kept separate from validate() so the
// existence loop stays simple and the validator's cyclomatic complexity in check.
func referencedSecrets(vc *cachev1beta1.ValkeyCluster) map[string]string {
	checks := map[string]string{}
	if vc.Spec.Auth != nil && vc.Spec.Auth.Enabled && vc.Spec.Auth.ExistingSecret != "" {
		checks[vc.Spec.Auth.ExistingSecret] = "spec.auth.existingSecret"
	}
	if vc.Spec.TLS != nil && vc.Spec.TLS.Enabled && vc.Spec.TLS.ExistingSecret != "" {
		checks[vc.Spec.TLS.ExistingSecret] = "spec.tls.existingSecret"
	}
	if vc.Spec.Backup != nil && vc.Spec.Backup.Enabled && vc.Spec.Backup.S3 != nil && vc.Spec.Backup.S3.CredentialsSecret != "" {
		checks[vc.Spec.Backup.S3.CredentialsSecret] = "spec.backup.s3.credentialsSecret"
	}
	if vc.Spec.RestoreFrom != nil && vc.Spec.RestoreFrom.S3 != nil && vc.Spec.RestoreFrom.S3.CredentialsSecret != "" {
		checks[vc.Spec.RestoreFrom.S3.CredentialsSecret] = "spec.restoreFrom.s3.credentialsSecret"
	}
	if r := vc.Spec.ReplicateFrom; r != nil && r.PasswordSecret != nil {
		checks[r.PasswordSecret.Name] = "spec.replicateFrom.passwordSecret.name"
	}
	if r := vc.Spec.ReplicateFrom; r != nil && r.CASecret != nil {
		checks[r.CASecret.Name] = "spec.replicateFrom.caSecret.name"
	}
	return checks
}

// validate runs all referenced-Secret existence checks. Missing Secrets are
// hard errors at admission time — the alternative is a CR that admits but
// never reconciles because the controller can't read its inputs, which is
// confusing for users.
func (v *ValkeyClusterCustomValidator) validate(ctx context.Context, vc *cachev1beta1.ValkeyCluster) (admission.Warnings, error) {
	for name, where := range referencedSecrets(vc) {
		var s corev1.Secret
		if err := v.Client.Get(ctx, types.NamespacedName{Namespace: vc.Namespace, Name: name}, &s); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, fmt.Errorf("%s: Secret %q not found in namespace %q", where, name, vc.Namespace)
			}
			return nil, fmt.Errorf("%s: unable to read Secret %q: %w", where, name, err)
		}
	}

	// Belt-and-suspenders for the conditional-required CEL rules: also reject
	// inconsistent shapes at admission time so the user gets a clearer error
	// than the API server's CEL message.
	switch vc.Spec.Topology {
	case cachev1beta1.TopologyCluster:
		if vc.Spec.Shards == nil {
			return nil, fmt.Errorf("topology=Cluster requires spec.shards")
		}
	case cachev1beta1.TopologySentinel:
		if vc.Spec.Sentinel == nil {
			return nil, fmt.Errorf("topology=Sentinel requires spec.sentinel")
		}
	}

	var warnings admission.Warnings

	// Cluster restore is per-shard and only semi-automated: the per-shard RDBs
	// are placed by the restore init container, but cluster assembly is a guided
	// manual procedure (C2). Warn, and require the {shard} placeholder so the
	// right snapshot reaches each master ordinal.
	if vc.Spec.Topology == cachev1beta1.TopologyCluster && vc.Spec.RestoreFrom != nil {
		if !strings.Contains(vc.Spec.RestoreFrom.SourceKey, "{shard}") {
			return nil, fmt.Errorf("topology=Cluster with restoreFrom requires a {shard} " +
				"placeholder in spec.restoreFrom.sourceKey (per-shard restore)")
		}
		warnings = append(warnings,
			"topology=Cluster restore is a guided manual procedure: per-shard RDBs are "+
				"pre-loaded, but cluster assembly (CLUSTER ADDSLOTSRANGE/MEET/REPLICATE) is "+
				"manual — see docs/runbook.md#cluster-restore. The cluster must have the same "+
				"shard count as the snapshot.")
	}

	// AR1/EC1: the Durable profile on a Replication topology relies on
	// operator-arbitrated failover — promotion happens via the reconcile loop
	// (≈15s cadence) and only while the operator is alive, with a split-brain
	// window on a network partition. For durable data prefer Sentinel (or a
	// Cluster topology), which arbitrates failover in the data plane. An empty
	// topology defaults to Replication, so warn for that case too.
	if vc.Spec.Profile == cachev1beta1.ProfileDurable &&
		(vc.Spec.Topology == "" || vc.Spec.Topology == cachev1beta1.TopologyReplication) {
		warnings = append(warnings,
			"profile=Durable with topology=Replication uses operator-arbitrated failover "+
				"(bounded by the reconcile interval and the operator's own liveness, with a "+
				"split-brain window on network partition). For durable data prefer "+
				"topology=Sentinel or Cluster, which arbitrate failover in the data plane.")
	}

	return warnings, nil
}
