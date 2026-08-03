# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project aims to
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.7.0]

### Added
- Cluster topology now recovers automatically from the loss of a **majority of
  primaries**. Gossip cannot fix this on its own: voting a replica in needs a
  master quorum that no longer exists, so the cluster would otherwise sit in
  `cluster_state:fail` with unserved slots indefinitely. The
  operator confirms each dead primary is fenced from two independent
  perspectives (the k8s API view of pod/node liveness AND a direct data-path
  check), waits out a debounce so it never races a failover gossip could still
  perform, then issues `CLUSTER FAILOVER TAKEOVER` on the most up-to-date
  surviving replica of each shard. The re-created primaries rejoin as replicas
  via their retained PVCs. Automatic for the Cache profile; opt-in for Durable
  (via `valkey.wellcake.io/quorum-takeover: "true"`, since a forced takeover can
  drop acknowledged writes). Surfaced through `status.quorumDownSince` and the
  `failover_total{reason="cluster-takeover"}` metric. See ADR 0006. Validated
  live on k3d: majority of primaries fenced, cluster recovered to `ok` with 0
  data loss and the fenced primaries rejoining as replicas.

### Changed
- Replicas now authenticate to their primary as a dedicated, least-privilege ACL
  user (`replicator`, granting only `+psync +replconf +ping` and no key access)
  via `masteruser`, instead of the full-access default user, so a leaked
  replication credential can neither read nor write data. It is seeded for every
  replicating topology (all but Standalone). Password rotation re-keys it (and the
  Sentinel user) in place, so an in-place rotation keeps the replica links alive.
  The `replicator` and `sentinel-user` names are reserved and rejected by ValkeyACL.

## [0.6.0]

### Changed
- The operator chart now ships its CRDs as **templates** instead of a top-level
  `crds/` directory, so `helm upgrade` applies schema changes. Helm installs
  `crds/` exactly once and silently ignores it afterwards, which left every
  upgraded install on the schema it was first installed with: fields added since
  (`spec.logging`, `spec.podSecurityContext`, and others) never reached the
  cluster. Two new values control this: `crd.enabled` (skip CRD installation
  when they are managed elsewhere) and `crd.keep` (default `true`, stamping
  `helm.sh/resource-policy: keep` so `helm uninstall` does not delete the CRDs
  and, with them, every ValkeyCluster).

  **Upgrading from chart 0.5.1 or earlier requires a one-time adoption step.**
  The existing CRDs carry no Helm ownership metadata, so the first upgrade fails
  with `invalid ownership metadata`. Relabel them once (nothing is deleted, so
  stored resources are untouched), then upgrade normally:

  ```sh
  for crd in valkeyclusters.cache.wellcake.io valkeyacls.cache.wellcake.io; do
    kubectl label crd "$crd" app.kubernetes.io/managed-by=Helm --overwrite
    kubectl annotate crd "$crd" \
      meta.helm.sh/release-name=<release> \
      meta.helm.sh/release-namespace=<namespace> --overwrite
  done
  ```

## [0.5.1]

### Added
- The published Helm charts are now signed with cosign, keyless, using the
  release workflow's own GitHub OIDC identity, so there is no private key to
  store or rotate. Charts are signed **by digest**, so a signature binds to that
  exact artifact and cannot be voided by moving a tag. Verify one with:

  ```sh
  cosign verify ghcr.io/melancholictheory/charts/valkey-operator:<version> \
    --certificate-identity-regexp '^https://github\.com/melancholictheory/wellcake/\.github/workflows/release\.yml@' \
    --certificate-oidc-issuer https://token.actions.githubusercontent.com
  ```

## [0.5.0]

Data-plane telemetry and resilience work built on Valkey 9.1+. Every directive is
version-gated, so older servers are unaffected.

### Added
- `valkey_operator_tls_cert_expiry_seconds`: seconds until the earliest-expiring
  served certificate across a cluster's pods (Valkey 9.1+). Valkey does not
  refuse to start on an expired certificate, so this is what catches a stalled
  cert-manager renewal that the automatic reload would otherwise hide.
- `valkey_operator_thread_utilization_ratio`: true 0-1 thread utilization from
  the Valkey 9.1+ active-time counters. Under I/O threading, process CPU sits
  near 100% because threads busy-poll, so CPU% misleads rightsizing.
- `valkey_operator_zone_colocated_groups`: replication groups (per shard for
  Cluster) whose pods all landed in one availability zone. The operator's zone
  spread is a soft constraint, so the scheduler can quietly void the HA it
  implies. Works on any Valkey version; requires the chart's ClusterRole variant,
  which now grants read access to nodes.
- `hash-seed` pinned per cluster on Valkey 9.1+, so a primary and its replicas
  return identical SCAN-family results and cursors survive the failovers and pod
  replacements the operator drives.

### Changed
- Durable Cluster primaries on Valkey 9.0+ now also carry the `safe` shutdown
  token, so descheduling that cannot fail over becomes visible instead of
  silently dropping a slot owner. Cache keeps `failover` alone, since `safe`
  would stall a drain on an availability-first profile.

## [0.4.0]

Exploits Valkey 9.1+ features surfaced by a feature-gap review. Every directive
is version-gated, so older servers are unaffected.

### Added
- `spec.logging.format` (`json` | `logfmt` | `legacy`) renders `log-format` for
  structured server logs, gated on Valkey 9.1+ (opt-in; unset keeps the default).
- `cluster-config-save-behavior best-effort` on memory-backed (Cache) Cluster
  pods running Valkey 9.1+, so a failed `nodes.conf` save no longer crashes an
  availability-first pod. Durable/PVC clusters keep the safe `sync` default.

### Changed
- Cluster pods running Valkey 9.0+ now set an explicit
  `terminationGracePeriodSeconds` (30s), giving the graceful SIGTERM manual
  failover a guaranteed time budget instead of relying on the Kubernetes default.

## [0.3.0]

### Added
- The `valkey-cluster` chart can create the release Namespace and label it with
  the Pod Security Standards (`namespace.create`, `namespace.podSecurityStandard`),
  so the restricted-PSA-compatible Valkey pods are actually enforced (S2).

### Changed
- The validating webhook now rejects `profile: Durable` on a `Replication`
  topology at create time. That combination relies on operator-arbitrated
  failover, which has a split-brain window on a network partition (AR1/EC1).
  Acknowledge the risk to proceed by setting the annotation
  `valkey.wellcake.io/accept-replication-durability-risk: "true"`; for durable
  data prefer `Sentinel` or `Cluster`. Existing clusters are unaffected on update.
- The Sentinel ACL user is narrowed from all commands to the minimal Sentinel
  command set (health/role checks, the `__sentinel__:hello` pub/sub, the failover
  transaction, `CONFIG REWRITE`, and `CLIENT`/`SCRIPT KILL`). It still carries no
  key access, the least privilege for the user Sentinel authenticates as (S1).

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

[0.7.0]: https://github.com/melancholictheory/wellcake/releases/tag/v0.7.0
[0.6.0]: https://github.com/melancholictheory/wellcake/releases/tag/v0.6.0
[0.5.1]: https://github.com/melancholictheory/wellcake/releases/tag/v0.5.1
[0.5.0]: https://github.com/melancholictheory/wellcake/releases/tag/v0.5.0
[0.4.0]: https://github.com/melancholictheory/wellcake/releases/tag/v0.4.0
[0.3.0]: https://github.com/melancholictheory/wellcake/releases/tag/v0.3.0
[0.2.0]: https://github.com/melancholictheory/wellcake/releases/tag/v0.2.0
[0.1.1]: https://github.com/melancholictheory/wellcake/releases/tag/v0.1.1
[0.1.0]: https://github.com/melancholictheory/wellcake/releases/tag/v0.1.0
