/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// ValkeyACLReconciler reconciles a ValkeyACL object.
type ValkeyACLReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=cache.wellcake.io,resources=valkeyacls,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cache.wellcake.io,resources=valkeyacls/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cache.wellcake.io,resources=valkeyacls/finalizers,verbs=update

func (r *ValkeyACLReconciler) Reconcile(ctx context.Context, req ctrl.Request) (_ ctrl.Result, retErr error) {
	defer recordReconcile("valkeyacl", req.Namespace, req.Name, &retErr)
	defer func() {
		// Count outcome on success only — markUnavailable paths leave retErr=nil
		// but Status.Conditions tell the real story; pair the result label
		// with the resolved condition.
		result := "success"
		if retErr != nil {
			result = "error"
		}
		aclApplyTotal.WithLabelValues(req.Namespace, req.Name, result).Inc()
	}()

	var acl cachev1beta1.ValkeyACL
	if err := r.Get(ctx, req.NamespacedName, &acl); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Resolve target ValkeyCluster.
	var vc cachev1beta1.ValkeyCluster
	if err := r.Get(ctx, types.NamespacedName{Namespace: acl.Namespace, Name: acl.Spec.ClusterRef.Name}, &vc); err != nil {
		if apierrors.IsNotFound(err) {
			return r.markUnavailable(ctx, &acl, "ClusterMissing",
				fmt.Sprintf("ValkeyCluster %q not found in namespace %q", acl.Spec.ClusterRef.Name, acl.Namespace))
		}
		return ctrl.Result{}, err
	}

	// Wait until the cluster is ready for ACL writes:
	//   - Cluster topology: need bootstrap done (otherwise nodes are not
	//     gossiping each other) and at least one Ready pod;
	//   - Other topologies: need a known primary.
	notReady := false
	switch vc.Spec.Topology {
	case cachev1beta1.TopologyCluster:
		notReady = !vc.Status.ClusterInitialized || vc.Status.ReadyReplicas == 0
	default:
		notReady = vc.Status.Primary == "" || vc.Status.ReadyReplicas == 0
	}
	if notReady {
		res, err := r.markUnavailable(ctx, &acl, "ClusterNotReady",
			"target cluster is not ready for ACL writes yet; retrying in 15s")
		if err == nil {
			res.RequeueAfter = 15 * time.Second
		}
		return res, err
	}

	clusterPassword, err := r.lookupClusterPassword(ctx, &vc)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("cluster password: %w", err)
	}

	// Pick targets:
	//   - Replication/Standalone/Sentinel: write to the current primary;
	//     replicas inherit ACLs through replication.
	//   - Cluster: ACLs are NOT replicated by cluster gossip — apply to
	//     every node individually (all pod ordinals).
	port := valkeyPort
	if tlsEnabled(&vc) {
		port = valkeyTLSPort
	}
	targets := r.aclTargets(&vc, port)
	if len(targets) == 0 {
		res, err := r.markUnavailable(ctx, &acl, "NoTargets",
			"could not determine any ACL target pods")
		if err == nil {
			res.RequeueAfter = 15 * time.Second
		}
		return res, err
	}

	// Resolve user-password secrets once.
	desiredNames, userPasswords := r.resolveUserPasswords(ctx, &acl)

	// Fanout: apply on every target. Any per-target error is logged and
	// reconcile continues; the next pass will retry.
	reached := r.applyACLFanout(ctx, &acl, &vc, targets, port, clusterPassword, desiredNames, userPasswords)

	if reached == 0 {
		res, err := r.markUnavailable(ctx, &acl, "PrimaryUnreachable",
			fmt.Sprintf("could not reach any of %d target nodes", len(targets)))
		if err == nil {
			res.RequeueAfter = 15 * time.Second
		}
		return res, err
	}

	applied := make([]string, 0, len(desiredNames))
	for name := range desiredNames {
		applied = append(applied, name)
	}
	sort.Strings(applied)

	patch := client.MergeFrom(acl.DeepCopy())
	acl.Status.AppliedUsers = applied
	acl.Status.ObservedGeneration = acl.Generation
	setCondition(&acl.Status.Conditions, metav1.Condition{
		Type:               cachev1beta1.ConditionAvailable,
		Status:             metav1.ConditionTrue,
		Reason:             "Applied",
		Message:            fmt.Sprintf("applied %d users", len(applied)),
		ObservedGeneration: acl.Generation,
		LastTransitionTime: metav1.Now(),
	})
	return ctrl.Result{}, r.Status().Patch(ctx, &acl, patch)
}

// resolveUserPasswords reads each user's referenced password Secret once and
// returns the set of desired user names plus a name→password map. A missing
// Secret is logged and skipped (the SETUSER will simply have no password);
// reconcile continues so one bad reference does not block the whole ACL.
func (r *ValkeyACLReconciler) resolveUserPasswords(ctx context.Context, acl *cachev1beta1.ValkeyACL) (map[string]struct{}, map[string]string) {
	log := logf.FromContext(ctx)
	desiredNames := map[string]struct{}{}
	userPasswords := map[string]string{}
	for _, u := range acl.Spec.Users {
		desiredNames[u.Name] = struct{}{}
		if u.PasswordSecret == nil {
			continue
		}
		var sec corev1.Secret
		if err := r.Get(ctx, types.NamespacedName{Namespace: acl.Namespace, Name: u.PasswordSecret.Name}, &sec); err != nil {
			log.Error(err, "user password secret missing", "user", u.Name)
			continue
		}
		key := u.PasswordSecret.Key
		if key == "" {
			key = secretKeyPassword
		}
		userPasswords[u.Name] = string(sec.Data[key])
	}
	return desiredNames, userPasswords
}

// applyACLFanout dials every target node and applies the desired user set:
// SETUSER for each spec'd user, DELUSER for users dropped since the last apply
// (never the reserved `default`), then ACL SAVE. Per-target/per-user errors are
// logged and skipped — the next reconcile retries. Returns the number of nodes
// successfully reached, so the caller can mark the ACL unavailable if it is 0.
func (r *ValkeyACLReconciler) applyACLFanout(
	ctx context.Context,
	acl *cachev1beta1.ValkeyACL,
	vc *cachev1beta1.ValkeyCluster,
	targets []string,
	port int32,
	clusterPassword string,
	desiredNames map[string]struct{},
	userPasswords map[string]string,
) int {
	log := logf.FromContext(ctx)
	reached := 0
	for _, host := range targets {
		c := dialReplClient(ctx, host, port, clusterPassword, tlsEnabled(vc), loadMTLSClientCert(ctx, r, vc), 3*time.Second)
		if c == nil {
			log.Info("ACL target unreachable, skipping", "host", host)
			continue
		}
		reached++
		for _, u := range acl.Spec.Users {
			if err := applyACLUser(ctx, c, u, userPasswords[u.Name]); err != nil {
				log.Error(err, "ACL SETUSER failed", "user", u.Name, "host", host)
			}
		}
		for _, prev := range acl.Status.AppliedUsers {
			if _, keep := desiredNames[prev]; keep || prev == "default" {
				continue
			}
			if err := c.rdb.Do(ctx, "ACL", "DELUSER", prev).Err(); err != nil {
				log.Error(err, "ACL DELUSER failed", "user", prev, "host", host)
			}
		}
		if err := c.rdb.Do(ctx, "ACL", "SAVE").Err(); err != nil {
			log.Info("ACL SAVE skipped", "host", host, "err", err.Error())
		}
		c.close()
	}
	return reached
}

// aclTargets returns the FQDN list to apply ACL changes against, depending on
// the cluster topology. Replication uses the current primary only (replicas
// inherit ACLs over replication). Cluster topology has no ACL gossip —
// every node needs its own SETUSER, so we fan out to every pod ordinal.
func (r *ValkeyACLReconciler) aclTargets(vc *cachev1beta1.ValkeyCluster, _ int32) []string {
	headless := headlessServiceName(vc)
	if vc.Spec.Topology == cachev1beta1.TopologyCluster {
		total := totalReplicas(vc)
		out := make([]string, 0, total)
		for i := range total {
			out = append(out, fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local", statefulSetName(vc), i, headless, vc.Namespace))
		}
		return out
	}
	if vc.Status.Primary == "" {
		return nil
	}
	return []string{fmt.Sprintf("%s.%s.%s.svc.cluster.local", vc.Status.Primary, headless, vc.Namespace)}
}

// applyACLUser builds and runs an ACL SETUSER call. We always include the
// `reset` token so the user state is fully derived from the CR — without it
// ACL SETUSER is additive and would accumulate drift over time.
func applyACLUser(ctx context.Context, c *replClient, u cachev1beta1.ValkeyACLUser, password string) error {
	args := []any{"ACL", "SETUSER", u.Name, "reset"}
	if u.Rules != "" {
		for tok := range strings.FieldsSeq(u.Rules) {
			args = append(args, tok)
		}
	}
	if password != "" {
		args = append(args, ">"+password)
	} else {
		args = append(args, "nopass")
	}
	return c.rdb.Do(ctx, args...).Err()
}

func (r *ValkeyACLReconciler) lookupClusterPassword(ctx context.Context, vc *cachev1beta1.ValkeyCluster) (string, error) {
	if vc.Spec.Auth == nil || !vc.Spec.Auth.Enabled {
		return "", nil
	}
	name := passwordSecretName(vc)
	if vc.Spec.Auth.ExistingSecret != "" {
		name = vc.Spec.Auth.ExistingSecret
	}
	var sec corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: vc.Namespace, Name: name}, &sec); err != nil {
		return "", err
	}
	return string(sec.Data[secretKeyPassword]), nil
}

func (r *ValkeyACLReconciler) markUnavailable(ctx context.Context, acl *cachev1beta1.ValkeyACL, reason, msg string) (ctrl.Result, error) { //nolint:unparam // Result is always zero; kept for the reconcile-helper return shape
	patch := client.MergeFrom(acl.DeepCopy())
	acl.Status.ObservedGeneration = acl.Generation
	setCondition(&acl.Status.Conditions, metav1.Condition{
		Type:               cachev1beta1.ConditionAvailable,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: acl.Generation,
		LastTransitionTime: metav1.Now(),
	})
	return ctrl.Result{}, r.Status().Patch(ctx, acl, patch)
}

// SetupWithManager registers the controller.
func (r *ValkeyACLReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cachev1beta1.ValkeyACL{}).
		Named("valkeyacl").
		Complete(r)
}
