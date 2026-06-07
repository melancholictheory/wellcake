/*
Copyright 2026 The Wellcake Authors.
*/

package v1beta1

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

var valkeyacllog = logf.Log.WithName("valkeyacl-resource")

// SetupValkeyACLWebhookWithManager registers the webhook for ValkeyACL.
func SetupValkeyACLWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &cachev1beta1.ValkeyACL{}).
		WithValidator(&ValkeyACLCustomValidator{Client: mgr.GetClient()}).
		Complete()
}

// +kubebuilder:webhook:path=/validate-cache-wellcake-io-v1beta1-valkeyacl,mutating=false,failurePolicy=fail,sideEffects=None,groups=cache.wellcake.io,resources=valkeyacls,verbs=create;update,versions=v1beta1,name=vvalkeyacl-v1beta1.kb.io,admissionReviewVersions=v1

// ValkeyACLCustomValidator validates that referenced ValkeyCluster and
// per-user Secrets exist in the same namespace. These are the exact checks
// that would otherwise turn into "Available=False/ClusterMissing" loops at
// reconcile time — failing them at admission gives a clearer error message.
type ValkeyACLCustomValidator struct {
	Client client.Client
}

func (v *ValkeyACLCustomValidator) ValidateCreate(ctx context.Context, obj *cachev1beta1.ValkeyACL) (admission.Warnings, error) {
	valkeyacllog.V(1).Info("validate create", "name", obj.GetName())
	return v.validate(ctx, obj)
}

func (v *ValkeyACLCustomValidator) ValidateUpdate(ctx context.Context, _, newObj *cachev1beta1.ValkeyACL) (admission.Warnings, error) {
	valkeyacllog.V(1).Info("validate update", "name", newObj.GetName())
	return v.validate(ctx, newObj)
}

func (v *ValkeyACLCustomValidator) ValidateDelete(_ context.Context, _ *cachev1beta1.ValkeyACL) (admission.Warnings, error) {
	return nil, nil
}

func (v *ValkeyACLCustomValidator) validate(ctx context.Context, acl *cachev1beta1.ValkeyACL) (admission.Warnings, error) {
	// Target cluster must exist (warning, not error, so the order in which
	// CR and target cluster are applied doesn't matter — apply both, fix
	// later. The reconciler also has its own retry loop.)
	var warnings admission.Warnings
	var vc cachev1beta1.ValkeyCluster
	err := v.Client.Get(ctx, types.NamespacedName{Namespace: acl.Namespace, Name: acl.Spec.ClusterRef.Name}, &vc)
	switch {
	case apierrors.IsNotFound(err):
		warnings = append(warnings,
			fmt.Sprintf("ValkeyCluster %q not found yet in namespace %q — reconciler will retry",
				acl.Spec.ClusterRef.Name, acl.Namespace))
	case err != nil:
		return nil, fmt.Errorf("cannot read ValkeyCluster %q: %w", acl.Spec.ClusterRef.Name, err)
	}

	// User name uniqueness within the CR.
	seen := map[string]struct{}{}
	for _, u := range acl.Spec.Users {
		if _, dup := seen[u.Name]; dup {
			return nil, fmt.Errorf("duplicate user %q in spec.users", u.Name)
		}
		seen[u.Name] = struct{}{}
		if u.Name == "default" {
			return nil, fmt.Errorf("user %q is reserved and cannot be managed via ValkeyACL", u.Name)
		}
	}

	// Per-user password Secrets must exist (hard error: a missing Secret
	// would silently drop the user from the reconcile).
	for _, u := range acl.Spec.Users {
		if u.PasswordSecret == nil {
			continue
		}
		var s corev1.Secret
		if err := v.Client.Get(ctx, types.NamespacedName{Namespace: acl.Namespace, Name: u.PasswordSecret.Name}, &s); err != nil {
			if apierrors.IsNotFound(err) {
				return warnings, fmt.Errorf("user %q: Secret %q not found in namespace %q",
					u.Name, u.PasswordSecret.Name, acl.Namespace)
			}
			return warnings, fmt.Errorf("user %q: cannot read Secret %q: %w", u.Name, u.PasswordSecret.Name, err)
		}
	}
	return warnings, nil
}
