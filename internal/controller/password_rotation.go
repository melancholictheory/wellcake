/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// reconcilePasswordRotation performs an in-place password rotation with no pod
// restart. It is triggered by the valkey.wellcake.io/rotate-password annotation
// (a token that the operator acts on exactly once).
//
// The hard constraint is the chicken-and-egg of a shared password: once the
// Secret changes the operator can only reach the pods with the value they
// currently hold. So the rotation is operator-driven and keeps the old password
// (the current Secret) available throughout:
//
//  1. old = current Secret password (the value the live pods still enforce).
//  2. new = freshly generated password.
//  3. applyPasswordToPods rolls every node over to new with no replication blip
//     (see its doc — additive add-new pass, then cutover pass + ACL SAVE).
//  4. Only once every pod enforces new, rewrite the Secret to new.
//  5. Record the token (RetryOnConflict) so the rotation runs exactly once.
//
// This works for operator-managed Secrets. A user-supplied ExistingSecret is
// out of scope: the operator never learns the old value if the user edits the
// Secret directly, so for that case a rotation still needs a restart.
//
// Returns rotated=true when it just performed a rotation (caller requeues).
func (r *ValkeyClusterReconciler) reconcilePasswordRotation(ctx context.Context, vc *cachev1beta1.ValkeyCluster) (bool, error) {
	if vc.Spec.Auth == nil || !vc.Spec.Auth.Enabled {
		return false, nil
	}
	// User-managed Secret: the operator cannot recover the old password, so an
	// in-place rotation is not possible — leave it to a restart.
	if vc.Spec.Auth.ExistingSecret != "" {
		return false, nil
	}
	token := vc.Annotations[rotatePasswordAnnotation]
	if token == "" || token == vc.Status.LastPasswordRotationToken {
		return false, nil
	}

	// Authoritative (uncached) status read: the cached vc above can lag a prior
	// reconcile's status write, which would let this reconcile re-run a rotation
	// that already happened. Confirm against the API server before committing.
	if r.APIReader != nil {
		var fresh cachev1beta1.ValkeyCluster
		if err := r.APIReader.Get(ctx, types.NamespacedName{Namespace: vc.Namespace, Name: vc.Name}, &fresh); err != nil {
			return false, err
		}
		if token == fresh.Status.LastPasswordRotationToken {
			return false, nil
		}
	}

	oldPw, err := r.ensurePasswordSecret(ctx, vc)
	if err != nil {
		return false, err
	}
	if oldPw == "" {
		return false, nil
	}
	newPw, err := generatePassword(32)
	if err != nil {
		return false, err
	}

	log := logf.FromContext(ctx)
	log.Info("password rotation: starting in-place rotation", "token", token, "pods", totalReplicas(vc))
	if err := r.applyPasswordToPods(ctx, vc, oldPw, newPw); err != nil {
		log.Error(err, "password rotation: applying to pods failed")
		return false, err
	}
	log.Info("password rotation: applied to all pods, updating Secret")

	// All pods enforce the new password now — persist it to the Secret so pod
	// restarts (env re-read from the Secret) stay consistent, and future
	// reconciles dial with the new value.
	if err := r.updatePasswordSecret(ctx, vc, newPw); err != nil {
		return false, err
	}

	// Record the token so the rotation runs exactly once. This MUST land: if it
	// is lost to a conflict and the reconcile re-runs, the guard above no longer
	// stops re-entry (the Secret already advanced), so the operator would
	// generate yet another password and churn. RetryOnConflict re-Gets and
	// retries so the token sticks within this reconcile.
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var latest cachev1beta1.ValkeyCluster
		if err := r.Get(ctx, types.NamespacedName{Namespace: vc.Namespace, Name: vc.Name}, &latest); err != nil {
			return err
		}
		latest.Status.LastPasswordRotationToken = token
		return r.Status().Update(ctx, &latest)
	}); err != nil {
		return false, err
	}

	log.Info("password rotation: complete", "token", token)
	return true, nil
}

// applyPasswordToPods rotates the auth password on every data pod with no
// replication blip, using the default ACL user's support for multiple passwords:
//
//	Pass 1 (additive): on each pod, ADD the new password alongside the old
//	  (ACL SETUSER default >new) and point masterauth at the new value. Every
//	  primary now accepts BOTH passwords, so a replica re-authenticating with the
//	  new masterauth stays connected — the link never drops.
//	Pass 2 (cutover): on each pod, drop the old password (ACL SETUSER default
//	  resetpass >new), align requirepass, and ACL SAVE so the change survives a
//	  restart (the on-disk aclfile is authoritative on load).
//
// Both passes dial with the old password (still valid until pass 2 completes on
// a given node), falling back to the new one so a partially-completed run is
// retry-safe.
func (r *ValkeyClusterReconciler) applyPasswordToPods(ctx context.Context, vc *cachev1beta1.ValkeyCluster, oldPw, newPw string) error {
	port := valkeyPort
	if tlsEnabled(vc) {
		port = valkeyTLSPort
	}
	useTLS := tlsEnabled(vc)
	clientCert := loadMTLSClientCert(ctx, r, vc)
	log := logf.FromContext(ctx)

	// onEachPod dials every pod (old password, then new as fallback) and runs fn.
	// clusterDataPods enumerates the right FQDNs for every topology, including the
	// per-shard Cluster layout (ADR 0005).
	onEachPod := func(pass string, fn func(c *replClient) error) error {
		for _, p := range clusterDataPods(vc) {
			host := p.host
			c := dialReplClient(ctx, host, port, oldPw, useTLS, clientCert, 5*time.Second)
			if c == nil {
				c = dialReplClient(ctx, host, port, newPw, useTLS, clientCert, 5*time.Second)
			}
			if c == nil {
				return fmt.Errorf("rotate %s: unreachable with current or new password", host)
			}
			log.Info("password rotation: applying", "pass", pass, "host", host)
			err := fn(c)
			c.close()
			if err != nil {
				return fmt.Errorf("rotate %s (%s): %w", host, pass, err)
			}
		}
		return nil
	}

	// Pass 1: every node accepts old AND new; replicas adopt the new masterauth.
	if err := onEachPod("add-new", func(c *replClient) error {
		if err := c.aclAddDefaultPassword(ctx, newPw); err != nil {
			return err
		}
		return c.configSet(ctx, "masterauth", newPw)
	}); err != nil {
		return err
	}

	// Pass 2: drop the old password, align requirepass, persist to the aclfile.
	return onEachPod("cutover", func(c *replClient) error {
		if err := c.aclSetDefaultPassword(ctx, newPw); err != nil {
			return err
		}
		if err := c.configSet(ctx, "requirepass", newPw); err != nil {
			return err
		}
		return c.aclSave(ctx)
	})
}

// updatePasswordSecret rewrites the operator-managed password Secret in place.
func (r *ValkeyClusterReconciler) updatePasswordSecret(ctx context.Context, vc *cachev1beta1.ValkeyCluster, newPw string) error {
	name := passwordSecretName(vc)
	var sec corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: vc.Namespace, Name: name}, &sec); err != nil {
		return err
	}
	if sec.Data == nil {
		sec.Data = map[string][]byte{}
	}
	sec.Data[secretKeyPassword] = []byte(newPw)
	return r.Update(ctx, &sec)
}
