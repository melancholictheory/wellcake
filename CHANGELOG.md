# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project aims to
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- The `valkey-cluster` chart can create the release Namespace and label it with
  the Pod Security Standards (`namespace.create`, `namespace.podSecurityStandard`),
  so the restricted-PSA-compatible Valkey pods are actually enforced (S2).

### Changed
- The validating webhook now **rejects** `profile: Durable` on a `Replication`
  topology at create time — that combination relies on operator-arbitrated
  failover, which has a split-brain window on a network partition (AR1/EC1).
  Acknowledge the risk to proceed by setting the annotation
  `valkey.wellcake.io/accept-replication-durability-risk: "true"`; for durable
  data prefer `Sentinel` or `Cluster`. Existing clusters are unaffected on update.
- The Sentinel ACL user is narrowed from all commands to the minimal Sentinel
  command set (health/role checks, the `__sentinel__:hello` pub/sub, the failover
  transaction, `CONFIG REWRITE`, and `CLIENT`/`SCRIPT KILL`); it still carries no
  key access — least privilege for the user Sentinel authenticates as (S1).

## [0.2.0]

Hardens auth handling and makes every operator-managed pod compatible with the
restricted Pod Security Standard.

### Added
- Pod- and container-level security contexts via new `spec.podSecurityContext`
  and `spec.containerSecurityContext` fields. When unset, the operator applies
  restricted-PSA-compatible defaults to every pod it creates — the StatefulSets,
  the cluster bootstrap/scale/reshard Jobs, and the backup/restore Jobs — so they
  are admitted in namespaces enforcing the `restricted` policy.
- The operator watches a user-managed `spec.auth.existingSecret` and rolls the
  cluster when it changes, so an external password rotation is picked up.

### Changed
- Auth passwords are escaped when rendered into `valkey.conf`, fixing startup
  failures for passwords containing characters meaningful to the config/ACL
  parser (quotes, spaces, `#`, and similar).
- The default ACL user is seeded by SHA-256 hash and re-seeded when the password
  changes, so a rotated `existingSecret` takes effect without manually wiping
  `users.acl`.

## [0.1.1]

### Added
- Artifact Hub annotations on the operator and cluster Helm charts.

## [0.1.0]

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

[Unreleased]: https://github.com/melancholictheory/wellcake/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/melancholictheory/wellcake/releases/tag/v0.2.0
[0.1.1]: https://github.com/melancholictheory/wellcake/releases/tag/v0.1.1
[0.1.0]: https://github.com/melancholictheory/wellcake/releases/tag/v0.1.0
