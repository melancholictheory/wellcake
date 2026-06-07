/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/redis/go-redis/v9"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// failoverDownAfter is how long the operator must observe the primary
// unreachable before promoting a replica — a debounce (cf. Sentinel
// down-after-milliseconds) that prevents failing over a primary that is merely
// briefly busy (slow BGSAVE, GC pause). Found by chaos C-5 (premature failover).
const failoverDownAfter = 20 * time.Second

// replClient is a thin wrapper around go-redis with operator-friendly timeouts.
type replClient struct {
	rdb  *redis.Client
	host string
	port int32
}

// dialReplClient opens a connection to a single Valkey pod. Returns nil if it
// can't be reached within `timeout`.
func dialReplClient(ctx context.Context, host string, port int32, password string, useTLS bool, clientCert *tls.Certificate, timeout time.Duration) *replClient {
	opts := &redis.Options{
		Addr:         net.JoinHostPort(host, strconv.Itoa(int(port))),
		Password:     password,
		DialTimeout:  timeout,
		ReadTimeout:  timeout,
		WriteTimeout: timeout,
	}
	if useTLS {
		// We trust the cert-manager-issued certificate chain via the cluster's CA;
		// but for an operator dialing pod IPs short-lived InsecureSkipVerify is OK —
		// pod identity is already enforced by ServiceAccount RBAC and NetworkPolicy.
		opts.TLSConfig = &tls.Config{InsecureSkipVerify: true} // #nosec G402 — see comment above
		// Under mutual TLS (tls-auth-clients yes) the server rejects a handshake
		// with no client cert, so present the cluster cert when one is supplied.
		if clientCert != nil {
			opts.TLSConfig.Certificates = []tls.Certificate{*clientCert}
		}
	}
	rdb := redis.NewClient(opts)
	pingCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		_ = rdb.Close()
		return nil
	}
	return &replClient{rdb: rdb, host: host, port: port}
}

func (c *replClient) close() {
	if c != nil && c.rdb != nil {
		_ = c.rdb.Close()
	}
}

// info parses `INFO replication` into a map.
func (c *replClient) info(ctx context.Context) (map[string]string, error) {
	res, err := c.rdb.Info(ctx, "replication").Result()
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for line := range strings.SplitSeq(res, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if i := strings.Index(line, ":"); i > 0 {
			out[line[:i]] = strings.TrimSpace(line[i+1:])
		}
	}
	return out, nil
}

// dbsize returns the number of keys (DBSIZE). It is the operator's "has data"
// signal: a freshly-restarted, empty primary reports 0 while a replica that
// still holds the dataset reports the live key count. reconcileFailover uses it
// to avoid adopting an empty self-master over data-holding replicas.
func (c *replClient) dbsize(ctx context.Context) (int64, error) {
	return c.rdb.DBSize(ctx).Result()
}

func (c *replClient) replicaOfNoOne(ctx context.Context) error {
	return c.rdb.Do(ctx, "REPLICAOF", "NO", "ONE").Err()
}

// configSet applies a live `CONFIG SET <param> <value>` — used by the
// no-restart password rotation to update requirepass/masterauth in place.
func (c *replClient) configSet(ctx context.Context, param, value string) error {
	return c.rdb.Do(ctx, "CONFIG", "SET", param, value).Err()
}

// aclSetDefaultPassword rotates the `default` ACL user's password in place.
//
// In our config the auth password lives on the default ACL user, and a live
// `CONFIG SET requirepass` does NOT update it (the two are decoupled once the
// default user is declared in the config). `ACL SETUSER default resetpass
// >pw` replaces the password while leaving the user's rules (keys, channels,
// commands, on/off) untouched. Existing authenticated connections stay valid.
func (c *replClient) aclSetDefaultPassword(ctx context.Context, password string) error {
	return c.rdb.Do(ctx, "ACL", "SETUSER", "default", "resetpass", ">"+password).Err()
}

// aclAddDefaultPassword adds a password to the `default` ACL user WITHOUT
// removing the existing one(s): `ACL SETUSER default >pw` is additive. During
// rotation this lets a primary accept both the old and new password at once, so
// replicas can switch their masterauth to the new value without ever losing the
// replication link.
func (c *replClient) aclAddDefaultPassword(ctx context.Context, password string) error {
	return c.rdb.Do(ctx, "ACL", "SETUSER", "default", ">"+password).Err()
}

// aclSave persists the in-memory ACL to the configured aclfile (users.acl on
// the data PVC). Without it the live `ACL SETUSER` is lost on the next pod
// restart, which reloads the old password from the on-disk aclfile.
func (c *replClient) aclSave(ctx context.Context) error {
	return c.rdb.Do(ctx, "ACL", "SAVE").Err()
}

func (c *replClient) replicaOf(ctx context.Context, host string, port int32) error {
	return c.rdb.Do(ctx, "REPLICAOF", host, strconv.Itoa(int(port))).Err()
}

// clusterNodes returns the raw text output of CLUSTER NODES, suitable for the
// parser in cluster.go. We use a raw string command (not the typed redis API)
// because go-redis doesn't yet model the Valkey-specific output cleanly.
func (c *replClient) clusterNodes(ctx context.Context) (string, error) {
	res, err := c.rdb.Do(ctx, "CLUSTER", "NODES").Text()
	if err != nil {
		return "", err
	}
	return res, nil
}

// clusterFailover issues CLUSTER FAILOVER on THIS node (must be a replica): it
// coordinates a graceful, no-data-loss handover of its master's slots to itself
// (the master pauses writes, the replica catches up to the master's offset, then
// takes over). Used by the proactive Cluster rollout to promote a fresh replica
// before the old master pod is restarted.
func (c *replClient) clusterFailover(ctx context.Context) error {
	return c.rdb.Do(ctx, "CLUSTER", "FAILOVER").Err()
}

// sentinelFailover asks THIS Sentinel to fail the monitored master over to a
// replica (SENTINEL FAILOVER <name>). The Sentinel quorum picks the best replica
// (priority/offset), promotes it, and reconfigures the rest. Used by the
// proactive Sentinel rollout to hand the master over before its pod is
// restarted — the operator must NOT promote directly in Sentinel topology or it
// would race the Sentinels' own election. Re-issuing while a failover is already
// in progress returns an error ("-INPROG ..."), which the caller can ignore.
func (c *replClient) sentinelFailover(ctx context.Context, masterName string) error {
	return c.rdb.Do(ctx, "SENTINEL", "FAILOVER", masterName).Err()
}

// podRole is the operator's view of a single pod's replication state.
type podRole struct {
	ordinal    int32
	host       string
	role       string // "master" | "slave" | ""
	masterHost string
	masterPort int32
	masterLink string // "up" | "down"
	offset     int64
	keys       int64 // DBSIZE — "has data" signal for the empty-master guard
	reachable  bool
}

// surveyReplication dials every pod and collects its replication info.
// Pods that don't answer within timeout end up with reachable=false.
func (r *ValkeyClusterReconciler) surveyReplication(ctx context.Context, vc *cachev1beta1.ValkeyCluster, password string) []podRole {
	port := valkeyPort
	if tlsEnabled(vc) {
		port = valkeyTLSPort
	}
	headless := headlessServiceName(vc)
	sts := statefulSetName(vc)
	out := make([]podRole, vc.Spec.Replicas)

	for i := int32(0); i < vc.Spec.Replicas; i++ {
		host := fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local", sts, i, headless, vc.Namespace)
		role := podRole{ordinal: i, host: host}
		c := dialReplClient(ctx, host, port, password, tlsEnabled(vc), loadMTLSClientCert(ctx, r, vc), 2*time.Second)
		if c == nil {
			out[i] = role
			continue
		}
		role.reachable = true
		if info, err := c.info(ctx); err == nil {
			role.role = info["role"]
			role.masterHost = info["master_host"]
			role.masterLink = info["master_link_status"]
			if v, err := strconv.ParseInt(info["master_port"], 10, 32); err == nil {
				role.masterPort = int32(v)
			}
			switch role.role {
			case roleMaster:
				if v, err := strconv.ParseInt(info["master_repl_offset"], 10, 64); err == nil {
					role.offset = v
				}
			case roleSlave:
				if v, err := strconv.ParseInt(info["slave_repl_offset"], 10, 64); err == nil {
					role.offset = v
				}
			}
		}
		if n, err := c.dbsize(ctx); err == nil {
			role.keys = n
		}
		c.close()
		out[i] = role
	}
	return out
}

// reconcileFailover inspects the survey and, if the expected primary is gone,
// promotes the most up-to-date reachable replica and re-points the others.
// Returns the pod that should be considered primary after this call (best-effort).
func (r *ValkeyClusterReconciler) reconcileFailover(ctx context.Context, vc *cachev1beta1.ValkeyCluster, password string, survey []podRole) string {
	log := logf.FromContext(ctx)

	reachable := 0
	var masters []podRole
	var bestReplica *podRole // most data, tiebreak highest offset
	for i := range survey {
		s := survey[i]
		if !s.reachable {
			continue
		}
		reachable++
		switch s.role {
		case roleMaster:
			masters = append(masters, s)
		case roleSlave:
			if bestReplica == nil || s.keys > bestReplica.keys ||
				(s.keys == bestReplica.keys && s.offset > bestReplica.offset) {
				b := s
				bestReplica = &b
			}
		}
	}

	// Safety: do nothing until at least 2 pods are reachable and we have visibility.
	if reachable < 2 {
		if vc.Status.Primary != "" {
			return vc.Status.Primary
		}
		return ""
	}

	port := valkeyPort
	if tlsEnabled(vc) {
		port = valkeyTLSPort
	}

	// Happy path: at least one master is reachable. Pick the authoritative one
	// (most data — see pickAuthoritativeMaster) and force every other reachable
	// pod to replicate from it. Choosing by data converges the split-brain
	// window after a primary pod restarts empty and masters itself.
	if len(masters) > 0 {
		// A master is reachable again — cancel any in-progress failover debounce.
		r.clearPrimaryDown(ctx, vc)
		chosen := pickAuthoritativeMaster(vc, masters)

		// EMPTY-MASTER GUARD: a restarted primary comes back empty and, per its
		// entrypoint, masters itself (replicaof itself). Adopting it would force
		// the data-holding replicas to REPLICAOF an empty node and resync the
		// whole dataset away — silent total data loss. If the chosen master
		// holds no data but a reachable replica does, promote that replica
		// instead and let the empty node rejoin as its replica.
		if chosen.keys == 0 && bestReplica != nil && bestReplica.keys > 0 {
			log.Info("empty-master guard: promoting data-holding replica over empty master",
				"name", vc.Name, "emptyMaster", podName(vc, chosen.ordinal),
				"promote", podName(vc, bestReplica.ordinal), "keys", bestReplica.keys)
			return r.promoteReplica(ctx, vc, password, survey, bestReplica, port, log)
		}

		want := podName(vc, chosen.ordinal)
		// Adoption counts as a failover only if the primary changed.
		if vc.Status.Primary != "" && vc.Status.Primary != want {
			failoverTotal.WithLabelValues(vc.Namespace, vc.Name, "adoption").Inc()
		}
		r.enforceReplication(ctx, vc, password, survey, &chosen, port, log)
		return want
	}

	// No master is reachable. Promote the replica with the most data / highest
	// offset — but DEBOUNCE first: a primary that merely misses a single survey
	// (briefly busy with a slow save / GC pause) is not dead. Promoting on the
	// first miss is a premature failover (found by chaos C-5). Only promote once
	// the primary has been unreachable for >= failoverDownAfter.
	if bestReplica == nil {
		log.Info("no reachable replica to promote", "name", vc.Name)
		return vc.Status.Primary
	}
	if !r.primaryDownLongEnough(ctx, vc, log) {
		return vc.Status.Primary
	}
	r.clearPrimaryDown(ctx, vc)
	return r.promoteReplica(ctx, vc, password, survey, bestReplica, port, log)
}

// downElapsed is the pure debounce predicate (extracted for testing): has the
// primary been unreachable since `since` for at least `threshold`?
func downElapsed(since, now time.Time, threshold time.Duration) bool {
	return now.Sub(since) >= threshold
}

// primaryDownLongEnough records (on first miss) when the primary went
// unreachable and reports whether it has now been down for >= failoverDownAfter.
// The timestamp is persisted in status so the window survives across reconciles.
func (r *ValkeyClusterReconciler) primaryDownLongEnough(ctx context.Context, vc *cachev1beta1.ValkeyCluster, log logr.Logger) bool {
	if vc.Status.PrimaryDownSince == nil {
		now := metav1.Now()
		log.Info("primary unreachable; starting failover debounce",
			"name", vc.Name, "downAfter", failoverDownAfter.String())
		r.setPrimaryDown(ctx, vc, &now)
		return false
	}
	return downElapsed(vc.Status.PrimaryDownSince.Time, time.Now(), failoverDownAfter)
}

func (r *ValkeyClusterReconciler) clearPrimaryDown(ctx context.Context, vc *cachev1beta1.ValkeyCluster) {
	if vc.Status.PrimaryDownSince != nil {
		r.setPrimaryDown(ctx, vc, nil)
	}
}

// setPrimaryDown persists the PrimaryDownSince marker via its OWN status patch
// with a base captured before the mutation, so the change is actually in the
// diff. (The later updateStatus patch captures its base after this and so
// leaves the field untouched.) Best-effort: a failed patch just means the next
// reconcile re-evaluates the window.
func (r *ValkeyClusterReconciler) setPrimaryDown(ctx context.Context, vc *cachev1beta1.ValkeyCluster, t *metav1.Time) {
	base := client.MergeFrom(vc.DeepCopy())
	vc.Status.PrimaryDownSince = t
	if err := r.Status().Patch(ctx, vc, base); err != nil {
		logf.FromContext(ctx).V(1).Info("failed to persist PrimaryDownSince (will re-evaluate)", "err", err.Error())
	}
}

// pickAuthoritativeMaster chooses which reachable master to keep when one or
// more are present. Preference order: most data (DBSIZE), then the pod that is
// already status.Primary, then lowest ordinal. Data-first is what demotes an
// empty restarted self-master in favour of the master that holds the keyspace.
func pickAuthoritativeMaster(vc *cachev1beta1.ValkeyCluster, masters []podRole) podRole {
	best := masters[0]
	for i := 1; i < len(masters); i++ {
		if betterMaster(vc, masters[i], best) {
			best = masters[i]
		}
	}
	return best
}

func betterMaster(vc *cachev1beta1.ValkeyCluster, a, b podRole) bool {
	if a.keys != b.keys {
		return a.keys > b.keys
	}
	aPrimary := podName(vc, a.ordinal) == vc.Status.Primary
	bPrimary := podName(vc, b.ordinal) == vc.Status.Primary
	if aPrimary != bPrimary {
		return aPrimary
	}
	return a.ordinal < b.ordinal
}

// promoteReplica makes `target` a standalone primary (REPLICAOF NO ONE) and
// re-points the rest of the cluster at it. Returns the new primary's pod name,
// or the existing Status.Primary if promotion couldn't be issued.
func (r *ValkeyClusterReconciler) promoteReplica(ctx context.Context, vc *cachev1beta1.ValkeyCluster, password string, survey []podRole, target *podRole, port int32, log logr.Logger) string {
	log.Info("promoting replica", "name", vc.Name, "pod", podName(vc, target.ordinal), "offset", target.offset, "keys", target.keys)
	c := dialReplClient(ctx, target.host, port, password, tlsEnabled(vc), loadMTLSClientCert(ctx, r, vc), 2*time.Second)
	if c == nil {
		return vc.Status.Primary
	}
	defer c.close()
	if err := c.replicaOfNoOne(ctx); err != nil {
		log.Error(err, "REPLICAOF NO ONE failed", "pod", podName(vc, target.ordinal))
		return vc.Status.Primary
	}
	failoverTotal.WithLabelValues(vc.Namespace, vc.Name, "promotion").Inc()
	r.enforceReplication(ctx, vc, password, survey, target, port, log)
	return podName(vc, target.ordinal)
}

// enforceReplication makes every reachable non-primary pod replicate from the
// chosen primary. This handles both fresh failovers and the split-brain window
// after a pod-0 restart (where the new empty pod-0 becomes master of itself).
func (r *ValkeyClusterReconciler) enforceReplication(ctx context.Context, vc *cachev1beta1.ValkeyCluster, password string, survey []podRole, primary *podRole, port int32, log logr.Logger) {
	for i := range survey {
		s := survey[i]
		if !s.reachable || s.ordinal == primary.ordinal {
			continue
		}
		// Skip pods already pointing at the right primary with link up.
		if s.role == roleSlave && s.masterHost == primary.host && s.masterLink == "up" {
			continue
		}
		c := dialReplClient(ctx, s.host, port, password, tlsEnabled(vc), loadMTLSClientCert(ctx, r, vc), 2*time.Second)
		if c == nil {
			continue
		}
		if err := c.replicaOf(ctx, primary.host, port); err != nil {
			log.Error(err, "REPLICAOF retarget failed", "pod", podName(vc, s.ordinal))
		}
		c.close()
	}
}

func podName(vc *cachev1beta1.ValkeyCluster, ord int32) string {
	return fmt.Sprintf("%s-%d", statefulSetName(vc), ord)
}

// pickFailoverTarget chooses which replica to promote for a MANUAL failover.
// If targetPod is set it must name a reachable replica (role slave); otherwise
// the most up-to-date reachable replica wins. Returns (nil, false) when no
// valid target exists — the caller then leaves the request unhandled so it can
// retry once the cluster is healthy. Pure function: unit-testable.
func pickFailoverTarget(vc *cachev1beta1.ValkeyCluster, survey []podRole, targetPod string) (*podRole, bool) {
	if targetPod != "" {
		for i := range survey {
			s := survey[i]
			if podName(vc, s.ordinal) == targetPod {
				if s.reachable && s.role == roleSlave {
					return &survey[i], true
				}
				return nil, false // named pod isn't a promotable replica
			}
		}
		return nil, false // named pod not found
	}
	var best *podRole
	for i := range survey {
		s := survey[i]
		if !s.reachable || s.role != roleSlave {
			continue
		}
		if best == nil || s.offset > best.offset {
			best = &survey[i]
		}
	}
	return best, best != nil
}

// manualFailover promotes a chosen replica on operator request
// (valkey.wellcake.io/failover), even when the current primary is healthy. On a
// successful promotion it records the request token in status so it runs
// exactly once. Returns the new primary pod name, or "" if nothing was done.
func (r *ValkeyClusterReconciler) manualFailover(ctx context.Context, vc *cachev1beta1.ValkeyCluster, password string, survey []podRole, token string) string {
	log := logf.FromContext(ctx)

	target, ok := pickFailoverTarget(vc, survey, vc.Annotations[failoverTargetAnnotation])
	if !ok {
		log.Info("manual failover requested but no valid target replica; will retry",
			"name", vc.Name, "target", vc.Annotations[failoverTargetAnnotation])
		return ""
	}

	port := valkeyPort
	if tlsEnabled(vc) {
		port = valkeyTLSPort
	}

	log.Info("manual failover: promoting replica", "name", vc.Name, "pod", podName(vc, target.ordinal))
	c := dialReplClient(ctx, target.host, port, password, tlsEnabled(vc), loadMTLSClientCert(ctx, r, vc), 2*time.Second)
	if c == nil {
		log.Info("manual failover: target unreachable; will retry", "pod", podName(vc, target.ordinal))
		return ""
	}
	defer c.close()
	if err := c.replicaOfNoOne(ctx); err != nil {
		log.Error(err, "manual failover: REPLICAOF NO ONE failed", "pod", podName(vc, target.ordinal))
		return ""
	}
	failoverTotal.WithLabelValues(vc.Namespace, vc.Name, "manual").Inc()
	r.enforceReplication(ctx, vc, password, survey, target, port, log)

	// Record the handled token so we don't re-promote on the next reconcile.
	patch := client.MergeFrom(vc.DeepCopy())
	vc.Status.LastFailoverToken = token
	if err := r.Status().Patch(ctx, vc, patch); err != nil {
		log.Error(err, "manual failover: recording token failed")
	}
	return podName(vc, target.ordinal)
}
