/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// Custom metrics exported on the operator's /metrics endpoint, in addition to
// the controller-runtime defaults (workqueue depth, reconcile latency, etc.).
//
// Labels are kept low-cardinality on purpose: namespace + cluster (or acl)
// name. We do NOT include pod ordinals or shard indices — Prometheus
// cardinality matters more than per-shard breakdown, which is available via
// kubectl on `status.shardDetails`.
var (
	reconcileTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "valkey_operator_reconcile_total",
			Help: "Total reconcile passes per controller and result.",
		},
		[]string{"controller", labelNamespace, "name", labelResult},
	)

	failoverTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "valkey_operator_failover_total",
			Help: "Operator-driven failover events on Replication topology.",
		},
		[]string{labelNamespace, labelCluster, "kind"},
	)

	bootstrapTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "valkey_operator_cluster_bootstrap_total",
			Help: "valkey-cli --cluster create Job outcomes.",
		},
		[]string{labelNamespace, labelCluster, labelResult},
	)

	scaleEventsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "valkey_operator_cluster_scale_total",
			Help: "Cluster scale-up and scale-down Job outcomes.",
		},
		[]string{labelNamespace, labelCluster, "direction", labelResult},
	)

	shardsHealthGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "valkey_operator_cluster_shards",
			Help: "Number of shards per health state. Sum across `health` labels equals total shards.",
		},
		[]string{labelNamespace, labelCluster, "health"},
	)

	aclApplyTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "valkey_operator_acl_apply_total",
			Help: "ValkeyACL apply outcomes, summed across target nodes.",
		},
		[]string{labelNamespace, "acl", labelResult},
	)
)

func init() {
	metrics.Registry.MustRegister(
		reconcileTotal,
		failoverTotal,
		bootstrapTotal,
		scaleEventsTotal,
		shardsHealthGauge,
		aclApplyTotal,
	)
}

// recordReconcile is the one-liner Reconcile uses in `defer`. Pass the error
// the function will return; nil → success, anything else → error.
func recordReconcile(controller, namespace, name string, errPtr *error) {
	result := "success"
	if *errPtr != nil {
		result = "error"
	}
	reconcileTotal.WithLabelValues(controller, namespace, name, result).Inc()
}

// recordShards rebuilds the shards-health gauge from a fresh observation.
// We always reset for this (namespace, cluster) tuple first so shards that
// disappeared (scale-down) drop out of the time series.
func recordShards(namespace, cluster string, shards []cachev1beta1.ShardStatus) {
	// Zero all known buckets so removed shards don't ghost in dashboards.
	for _, h := range []string{
		cachev1beta1.ShardHealthReady,
		cachev1beta1.ShardHealthDegraded,
		cachev1beta1.ShardHealthDown,
		cachev1beta1.ShardHealthUnknown,
	} {
		shardsHealthGauge.WithLabelValues(namespace, cluster, h).Set(0)
	}
	for _, s := range shards {
		shardsHealthGauge.WithLabelValues(namespace, cluster, s.Health).Inc()
	}
}
