# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project aims to
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

First public release of the operator. Highlights of the initial feature set:

### Added
- `ValkeyCluster` CRD covering four topologies: Standalone, Replication,
  Sentinel, and Cluster.
- `ValkeyACL` CRD for declarative ACL users (applied to all nodes of a shard in
  Cluster mode).
- API versions `v1alpha1` (served) and `v1beta1` (hub/storage) with conversion.
- Dual-track profiles: `Cache` (allkeys-lru, persistence off) and `Durable`
  (noeviction, RDB+AOF).
- Operator-driven failover for Replication (offset-based promotion, split-brain
  protection) and manual failover via the kubectl plugin.
- Proactive zero-downtime rolling restart for Replication / Cluster / Sentinel
  (ADR 0004, opt-in via `valkey.wellcake.io/proactive-rollout`).
- Cluster bootstrap, scale-up/scale-down, and `autoReshard`; Atomic Slot
  Migration on Valkey 9.1+ (ADR 0001).
- Per-shard workload mode (`spec.perShardWorkload`, experimental; ADR 0005) for
  shard-aware anti-affinity and clean shard scale-down.
- S3 backups (CronJob, retention, SSE, per-shard for Cluster) and restore via
  `spec.restoreFrom`.
- Multi-region async replication via `spec.replicateFrom`, including cross-CA
  mTLS through a merged trust bundle (`replicateFrom.caSecret`).
- No-restart secret rotation: in-place password rotation
  (`valkey.wellcake.io/rotate-password`) and TLS cert auto-reload on cert-manager
  renewal.
- TLS via cert-manager Issuer or an existing Secret.
- Prometheus metrics, optional ServiceMonitor and PrometheusRule.
- `kubectl valkey` plugin (status / cli / backup / restart / reshard / failover /
  hibernate / certificate / report).
- Helm charts for the operator and for individual clusters; validating /
  mutating / conversion webhooks (opt-in).
- CEL XValidation for immutable and conditional fields; config-hash-driven
  rolling restarts; version-gated Valkey 9.x resilience directives.

[Unreleased]: https://github.com/melancholictheory/wellcake
