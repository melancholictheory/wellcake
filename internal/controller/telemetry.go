/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// Data-plane telemetry the operator is uniquely placed to publish: it already
// dials every managed pod and knows the intended topology, so it can surface
// signals no generic exporter has the context for.
//
// Labels stay low-cardinality (namespace + cluster), matching metrics.go: values
// are aggregated across pods rather than emitted per pod/shard/thread.
var (
	tlsCertExpiryGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "valkey_operator_tls_cert_expiry_seconds",
			Help: "Seconds until the earliest-expiring served TLS certificate across a cluster's pods (Valkey 9.1+). Goes negative once expired. Valkey does not refuse to start on an expired cert, so this is the signal that catches a stalled cert-manager renewal.",
		},
		[]string{labelNamespace, labelCluster},
	)

	threadUtilGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "valkey_operator_thread_utilization_ratio",
			Help: "Busiest pod's mean thread utilization (0-1) over the last reconcile interval, derived from Valkey 9.1+ active-time counters. Truthful under I/O threading, where process CPU sits near 100% because threads busy-poll.",
		},
		[]string{labelNamespace, labelCluster},
	)

	zoneColocatedGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "valkey_operator_zone_colocated_groups",
			Help: "Replication groups (shards, or the whole cluster for Replication/Sentinel) whose pods all sit in ONE availability zone, so a single zone failure takes the group down. 0 means the intended spread held.",
		},
		[]string{labelNamespace, labelCluster},
	)
)

func init() {
	metrics.Registry.MustRegister(tlsCertExpiryGauge, threadUtilGauge, zoneColocatedGauge)
}

// threadSample is the previous active-time reading for one pod. Utilization is a
// RATE, so it needs two samples; we keep the last one per pod rather than
// exporting raw cumulative counters, because a cumulative counter cannot be
// aggregated across pods into a single low-cardinality series.
type threadSample struct {
	at         time.Time
	activeUsec float64
	threads    int
}

var (
	threadSamplesMu sync.Mutex
	threadSamples   = map[string]threadSample{}
)

// nodeZoneTTL bounds how long a node's zone label is trusted. Zone labels are
// effectively immutable, so a long TTL keeps the zone check from adding API
// traffic proportional to fleet size x reconcile rate.
const nodeZoneTTL = 10 * time.Minute

type zoneEntry struct {
	zone string
	at   time.Time
}

var (
	nodeZoneMu    sync.Mutex
	nodeZoneCache = map[string]zoneEntry{}
)

// forgetTelemetry drops a deleted cluster's samples and metric series so neither
// leaks for the operator's lifetime.
func forgetTelemetry(vc *cachev1beta1.ValkeyCluster) {
	prefix := vc.Namespace + "/" + vc.Name + "/"
	threadSamplesMu.Lock()
	for k := range threadSamples {
		if strings.HasPrefix(k, prefix) {
			delete(threadSamples, k)
		}
	}
	threadSamplesMu.Unlock()
	tlsCertExpiryGauge.DeleteLabelValues(vc.Namespace, vc.Name)
	threadUtilGauge.DeleteLabelValues(vc.Namespace, vc.Name)
	zoneColocatedGauge.DeleteLabelValues(vc.Namespace, vc.Name)
}

// collectValkeyTelemetry dials the managed pods once and publishes the INFO-derived
// signals. Both fields are Valkey 9.1+, so older images are skipped entirely.
// Failures are silent by design: telemetry must never fail a reconcile.
func (r *ValkeyClusterReconciler) collectValkeyTelemetry(ctx context.Context, vc *cachev1beta1.ValkeyCluster, password string) {
	if !valkeyImageAtLeast(vc.Spec.Image, 9, 1) {
		return
	}
	port := valkeyPort
	if tlsEnabled(vc) {
		port = valkeyTLSPort
	}
	cert := loadMTLSClientCert(ctx, r, vc)
	sts := statefulSetName(vc)
	headless := headlessServiceName(vc)

	minExpiry, haveExpiry := 0.0, false
	maxUtil, haveUtil := 0.0, false

	for i := int32(0); i < totalReplicas(vc); i++ {
		host := fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local", sts, i, headless, vc.Namespace)
		c := dialReplClient(ctx, host, port, password, tlsEnabled(vc), cert, 2*time.Second)
		if c == nil {
			continue
		}
		// One INFO pass per pod: the TLS fields and the active-time counters live in
		// different sections, and `everything` avoids depending on their names.
		info, err := c.infoSection(ctx, "everything")
		c.close()
		if err != nil {
			continue
		}
		if tlsEnabled(vc) {
			if v, ok := infoFloat(info, "tls_server_cert_expires_in_seconds"); ok && (!haveExpiry || v < minExpiry) {
				minExpiry, haveExpiry = v, true
			}
		}
		if u, ok := podThreadUtilization(vc, host, info); ok && (!haveUtil || u > maxUtil) {
			maxUtil, haveUtil = u, true
		}
	}

	if haveExpiry {
		tlsCertExpiryGauge.WithLabelValues(vc.Namespace, vc.Name).Set(minExpiry)
	}
	if haveUtil {
		threadUtilGauge.WithLabelValues(vc.Namespace, vc.Name).Set(maxUtil)
	}
}

// podThreadUtilization turns the cumulative active-time counters into a 0-1
// utilization ratio using the delta since this pod's previous sample. Reports
// ok=false on the first sample, on a counter reset (pod restart), and on servers
// that don't publish the fields.
func podThreadUtilization(vc *cachev1beta1.ValkeyCluster, host string, info map[string]string) (float64, bool) {
	active, threads := 0.0, 0
	if v, ok := infoFloat(info, "used_active_time_main_thread"); ok {
		active, threads = active+v, threads+1
	}
	// Scan by prefix rather than an index range: the io-thread numbering is not
	// contractual.
	for k, s := range info {
		if !strings.HasPrefix(k, "used_active_time_io_thread_") {
			continue
		}
		if v, err := strconv.ParseFloat(s, 64); err == nil {
			active, threads = active+v, threads+1
		}
	}
	if threads == 0 {
		return 0, false // pre-9.1 server, or the fields are absent
	}

	key := vc.Namespace + "/" + vc.Name + "/" + host
	now := time.Now()
	threadSamplesMu.Lock()
	prev, seen := threadSamples[key]
	threadSamples[key] = threadSample{at: now, activeUsec: active, threads: threads}
	threadSamplesMu.Unlock()

	if !seen {
		return 0, false
	}
	elapsed := now.Sub(prev.at).Seconds()
	delta := active - prev.activeUsec
	if elapsed <= 0 || delta < 0 {
		return 0, false // clock went backwards, or the counters reset
	}
	util := delta / (elapsed * 1e6 * float64(threads))
	return min(max(util, 0), 1), true
}

func infoFloat(info map[string]string, key string) (float64, bool) {
	s, ok := info[key]
	if !ok {
		return 0, false
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// checkZoneColocation reports how many replication groups have every pod in a
// single availability zone. The operator sets a zone topology-spread constraint,
// but it is ScheduleAnyway (soft): the scheduler may silently co-locate a
// shard's primary and replica, quietly voiding the HA the spread implies. This
// needs no Valkey 9.1 (it reads node labels), so it covers every topology.
func (r *ValkeyClusterReconciler) checkZoneColocation(ctx context.Context, vc *cachev1beta1.ValkeyCluster) {
	groups := replicationGroups(vc)
	if len(groups) == 0 {
		return
	}
	zoneOf, err := r.podZones(ctx, vc)
	if err != nil || len(zoneOf) == 0 {
		return
	}

	colocated := 0
	for _, group := range groups {
		zones := map[string]struct{}{}
		for _, pod := range group {
			if z, ok := zoneOf[pod]; ok && z != "" {
				zones[z] = struct{}{}
			}
		}
		// A single-pod group cannot be spread, and an unresolved group tells us
		// nothing — neither is a co-location finding.
		if len(group) > 1 && len(zones) == 1 {
			colocated++
		}
	}
	zoneColocatedGauge.WithLabelValues(vc.Namespace, vc.Name).Set(float64(colocated))
}

// replicationGroups returns the sets of pod names that must not share a zone: one
// group per shard for Cluster (from the surveyed shard details), otherwise the
// whole cluster as a single group.
func replicationGroups(vc *cachev1beta1.ValkeyCluster) [][]string {
	if vc.Spec.Topology == cachev1beta1.TopologyCluster {
		var out [][]string
		for _, s := range vc.Status.ShardDetails {
			group := make([]string, 0, len(s.Replicas)+1)
			if s.Primary != "" {
				group = append(group, s.Primary)
			}
			group = append(group, s.Replicas...)
			if len(group) > 0 {
				out = append(out, group)
			}
		}
		return out
	}
	if vc.Spec.Replicas <= 1 {
		return nil
	}
	group := make([]string, 0, vc.Spec.Replicas)
	for i := int32(0); i < vc.Spec.Replicas; i++ {
		group = append(group, fmt.Sprintf("%s-%d", statefulSetName(vc), i))
	}
	return [][]string{group}
}

// podZones maps this cluster's pod names to their node's zone label.
func (r *ValkeyClusterReconciler) podZones(ctx context.Context, vc *cachev1beta1.ValkeyCluster) (map[string]string, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(vc.Namespace), client.MatchingLabels(labelsFor(vc))); err != nil {
		return nil, err
	}
	out := map[string]string{}
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Spec.NodeName == "" {
			continue
		}
		zone, err := r.nodeZone(ctx, p.Spec.NodeName)
		if err != nil {
			// Most likely a namespace-scoped install with no cluster-wide `nodes`
			// permission: skip the check rather than log-spam every reconcile.
			return nil, err
		}
		out[p.Name] = zone
	}
	return out, nil
}

// nodeZone reads a node's topology zone label through the UNCACHED reader. A
// cached read would make controller-runtime start a cluster-wide Node informer,
// which stalls (waiting for a sync that never comes) on installs whose RBAC is
// namespace-scoped; the uncached path just returns a clean 403 instead. The TTL
// cache keeps that from costing an API call per node per reconcile.
func (r *ValkeyClusterReconciler) nodeZone(ctx context.Context, name string) (string, error) {
	now := time.Now()
	nodeZoneMu.Lock()
	e, ok := nodeZoneCache[name]
	nodeZoneMu.Unlock()
	if ok && now.Sub(e.at) < nodeZoneTTL {
		return e.zone, nil
	}

	var node corev1.Node
	if err := r.APIReader.Get(ctx, client.ObjectKey{Name: name}, &node); err != nil {
		return "", err
	}
	zone := node.Labels[corev1.LabelTopologyZone]

	nodeZoneMu.Lock()
	nodeZoneCache[name] = zoneEntry{zone: zone, at: now}
	nodeZoneMu.Unlock()
	return zone, nil
}
