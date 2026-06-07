# Architecture

## Why a dedicated operator

A typical scenario is running many self-managed Redis/Valkey instances in
Kubernetes, brought up with generic public charts (often the bitnami
`redis-cluster` chart), without a managed service. The official
`valkey-io/valkey-operator` is still alpha ("data loss expected"), and the
available Crossplane compositions are usually limited to a single standalone
instance.

This operator closes that gap. Its architectural decisions are grounded in
typical production workloads, not just generic best practices.

## CRDs

Two kinds in the `cache.wellcake.io` group. The hub and storage version is
`v1beta1` (`+kubebuilder:storageversion`); `v1alpha1` remains a served spoke
with round-trip conversion (`ConvertTo`/`ConvertFrom`). An upstream platform or
GitOps layer may write `v1alpha1`, so new fields are mirrored into `v1alpha1` as
well (otherwise admission prunes them):

| Kind            | Purpose                                                  |
| --------------- | -------------------------------------------------------- |
| `ValkeyCluster` | Deploys Valkey in one of four topologies.                |
| `ValkeyACL`     | Declarative ACL users on top of a `ValkeyCluster`.       |

The single-`ValkeyCluster`-kind-plus-`Topology`-enum approach is a deliberate
departure from operators with separate `RedisReplication` / `RedisCluster` /
`RedisSentinel` kinds (OT-CONTAINER-KIT). The trade-off is discussed in
[topologies.md](topologies.md).

## Reconcile pipeline

```
                       ┌──────────────────────────┐
                       │ ValkeyClusterReconciler  │
                       └──────────────┬───────────┘
                                      │
            ┌─────────────────────────┼─────────────────────────┐
            │                         │                         │
       Replication                Sentinel                  Cluster
       /Standalone                                              │
            │                         │                         │
   shared primitives          shared primitives          shared primitives
   (Service, CM, STS,         + Sentinel STS,            + bootstrap Job,
    PDB, NP, Secret,            sentinel-config CM,        scale-up Job,
    failover survey)            sentinel Service           cluster-mode CM
            │                         │                         │
            └────────► updateStatus / updateClusterStatus ◄──────┘
```

Structure of `internal/controller/`:

- `valkeycluster_controller.go` — the main reconciler, dispatcher, finalizer
  lifecycle, status helpers, RBAC markers.
- `resources.go` — shared builders (Service, ConfigMap, StatefulSet, PVC,
  PDB, NetworkPolicy, password Secret, runtime-config rendering).
- `cluster.go` — Cluster topology: STS in cluster mode, bootstrap Job
  (`valkey-cli --cluster create`), scale-up Job (`add-node`).
- `sentinel.go` — Sentinel topology: sentinel ConfigMap/Service/STS.
- `failover.go` — operator-driven failover for Replication: TCP polling via
  go-redis, promotion via `REPLICAOF NO ONE`, replication enforcement.
- `valkeyacl_controller.go` — the ValkeyACL reconciler.

## Always-PVC

The operator never uses `emptyDir` for Valkey data. In real clusters,
ephemeral-storage pressure (`nodefs.available`) can evict pods sharing the
node's disk; for an in-memory service that is an unacceptable availability
risk. The Cache profile gets a small (1Gi) PVC with persistence disabled;
Durable gets a configured size with RDB+AOF. Optionally,
`spec.storage.medium=Memory` (PoC, opt-in, immutable, Cache only) places the
data directory on tmpfs/RAM with persistence force-disabled; `spec.storage.mode`
(`rdb`/`aof`/`both`/`none`) sets the persistence mode.

This is a deliberate departure from the common tech debt of using emptyDir
instead of a PVC for cache — see the ADR decisions in [docs/adr/](adr/).

## Dual-track Profile

`spec.profile` is `Cache` or `Durable`. It drives a small set of dependent
defaults:

|                                         | Cache         | Durable            |
| --------------------------------------- | ------------- | ------------------ |
| persistence mode                        | `none`        | `both` (RDB + AOF) |
| maxmemory-policy                        | `allkeys-lru` | `noeviction`       |
| default PVC                             | 1Gi           | 10Gi               |
| cluster-require-full-coverage (Cluster) | `no`          | `yes`              |

Profile is **immutable** (CEL `XValidation`): switching it would require
resizing the PVC and changing persistence semantics on the fly, which is
unsafe.

## Failover

The code has two independent strategies:

1. **Replication / Standalone** — operator-driven. On each reconcile the
   operator TCP-polls all pods, parses `INFO replication`, and picks an action:
   - a reachable master exists → adopt it; the other reachable pods get
     `REPLICAOF` pointing at it (this resolves split-brain after a pod-0
     restart);
   - the master is unreachable → the replica with the highest offset is
     selected, `REPLICAOF NO ONE` promotes it, and the rest are retargeted.
2. **Sentinel** — quorum-driven by the Sentinel pods themselves.
   Operator-driven failover is deliberately disabled in this topology.

In the `Cluster` topology there is no operator-driven failover — native Valkey
gossip handles it.

## Proactive rolling restart (ADR 0004)

Opt-in via the `valkey.wellcake.io/proactive-rollout: "true"` annotation
(default OFF; when OFF the StatefulSet stays on `RollingUpdate` — zero
regression risk). It covers the **Replication, Cluster, and Sentinel**
topologies (Standalone is excluded — there is a single node, with nowhere to
fail over). When enabled, the STS switches to `updateStrategy: OnDelete` and the
operator rolls pods one at a time itself; a "stale" pod is identified by
`controller-revision-hash` vs `STS.status.updateRevision` (the same signal the
STS controller uses — it catches any pod-template change). Per topology:

- **Replication:** replicas are rolled first, then the operator promotes a fresh
  replica (`REPLICAOF NO ONE`) **before** deleting the old primary pod.
- **Cluster:** per shard — the shard's replicas, then `CLUSTER FAILOVER` on a
  fresh replica **before** restarting the master; one node at a time.
- **Sentinel:** two planes, one pod at a time — data pods via `SENTINEL
  FAILOVER` (the Sentinel quorum owns the failover; the operator does not
  promote directly, otherwise it would race their election), then the Sentinel
  monitors themselves while preserving quorum.

The loop is stateless/resumable: each reconcile recomputes the next step from
the live pods. The unavailability window on a planned rollout is ~0 instead of
the reactive ~15-20s.

## Cluster bootstrap

When `topology=Cluster` and all `Shards * (1 + ReplicasPerShard)` pods in the
StatefulSet are Ready, the operator creates a one-shot Job `<name>-bootstrap`
that:

1. PING-waits for each pod (guards against a race with the STS rollout).
2. Runs `valkey-cli --cluster create <pod-0:6379> ... <pod-N:6379>
   --cluster-replicas <replicasPerShard> --cluster-yes`.

On `Job.Status.Succeeded > 0`:
- `Status.ClusterInitialized = true`
- `Status.LastAppliedReplicas = totalReplicas(spec)`
- Condition `ClusterInitialized=True`

The bootstrap Job is **idempotent** via the `ClusterInitialized` gate: once set,
it is never re-run.

## Cluster scale

Scale-up is automated: when
`totalReplicas(spec) > Status.LastAppliedReplicas` and the pods are Ready, the
operator creates a `<name>-scaleup` Job that calls `valkey-cli --cluster
add-node` for each new ordinal. Slot rebalancing after add-node is controlled by
the `spec.autoReshard` field (default `true`): the Job follows up with
`valkey-cli --cluster rebalance`. A manual one-off rebalance is also available
via the `valkey.wellcake.io/reshard` annotation.

Scale-down is **implemented**: when
`totalReplicas(spec) < Status.LastAppliedReplicas`, the operator creates a
`<name>-scaledown` Job that reshards the leaving masters' slots onto the
remaining ones and `del-node`s them. For the per-shard layout
(`spec.perShardWorkload`), after resharding the leaving shards'
StatefulSets/Services are deleted (`deleteLeavingShards`). On Valkey 9.1+, all
rebalance/reshard operations go through Atomic Slot Migration (ASM, runtime-gated
on `INFO valkey_version`, ADR 0001) — slots move atomically, with no open slot
if interrupted.

## Config hash

`ConfigMap.Data` is hashed (sha256, 16 hex characters), and the result is
written to the pod template under the `valkey.wellcake.io/config-hash`
annotation. Any change to `valkey.conf` (eviction policy, maxmemory, TLS,
free-form `spec.config`) changes the hash, and the StatefulSet rolls the pods
per its UpdateStrategy.

The password is deliberately excluded from the hash so that rotation does not
trigger a restart on every silent rotation. Password rotation is **implemented**
(AR5): it is triggered by the `valkey.wellcake.io/rotate-password=<token>`
annotation exactly once per token (gated on
`Status.LastPasswordRotationToken`), without a restart — via `ACL SETUSER
default` in two passes (additive password → cutover + `ACL SAVE`). With
`aclfile`, a live `CONFIG SET requirepass` does not change auth, so rotation goes
through ACL. This applies only to operator-managed Secrets; `existingSecret` is
out of scope. TLS certificate reload on a cert-manager renew is automatic,
without a restart (`CONFIG SET tls-cert-file` + TLS-handshake verification).

## Rendering runtime config via an initContainer

The runtime config (`valkey.conf` plus per-pod lines such as `replicaof` or
`cluster-announce-hostname`) is generated by an init container that copies
`valkey.conf` from the ConfigMap and appends per-ordinal lines to
`/data/runtime.conf`. The main container starts directly via `valkey-server
/data/runtime.conf`. There is not a single executable script in the ConfigMap —
that anti-pattern is flagged by CVE scanners and breaks immutable-image
policies.

## Security defaults

- **Auth by default.** A password Secret is auto-generated unless
  `existingSecret` is set; the password lives only in the Secret, never in a
  ConfigMap or in git.
- **TLS opt-in** with cert-manager integration (a reference to an Issuer) or a
  reference to a preallocated Secret.
- **NetworkPolicy opt-in** — default-deny + same-namespace allow +
  Valkey-to-Valkey for replication and gossip.
- **CEL XValidation** on immutable fields: `topology`, `profile`,
  `storage.storageClassName`, `storage.mode`, `storage.medium`,
  `perShardWorkload`. Expand-only on `storage.size` via
  `quantity().compareTo()`. Conditional rules: `shards` is required and valid
  only when `topology=Cluster`; `shards` may only grow, except when
  `perShardWorkload=true`; `perShardWorkload` is valid only with Cluster;
  `storage.medium=Memory` is forbidden with the `Durable` profile;
  `replicateFrom.caSecret` requires `replicateFrom.tls=true` and
  `tls.enabled=true`.
- **mTLS contract.** `tls.mutualTLS=true` forces `tls-auth-clients yes`.

## RBAC

Cluster-wide by default; `rbac.scope=namespace` or `watchNamespace=<ns>` narrows
it to a single namespace (Role/RoleBinding instead of
ClusterRole/ClusterRoleBinding). The manager only needs read/write on the
objects it owns:

- `core/{configmaps, secrets, services, events, pods}`
- `apps/statefulsets`
- `batch/jobs` (bootstrap + scale-up)
- `policy/poddisruptionbudgets`
- `networking.k8s.io/networkpolicies`
- `cache.wellcake.io/{valkeyclusters,valkeyacls}` (+ /status, /finalizers)

## Status model

Conditions follow the well-known-type pattern:

- `Ready` — a kstatus-compatible readiness signal; True iff the STS (and the
  Sentinel STS for the relevant topology) is fully ready. Mirrors `Available`.
- `Available` — a human-readable overall readiness.
- `Progressing` — not yet used; reserved.
- `Degraded` — not yet used; reserved.
- `ClusterInitialized` — Cluster topology only; True after the bootstrap Job
  succeeds.

Plus a lightweight phase string in `status.phase` for a quick glance via
kubectl, and topology-specific fields:
`primary`, `shards`, `readyShards`, `lastAppliedReplicas`,
`clusterInitialized`, `readyReplicas`, `observedGeneration`.

## Multi-DC awareness

`Spec.TopologySpreadConstraints` spreads across
`topology.kubernetes.io/zone` by default (prod is usually spread across several
availability zones). Users can override this with an explicit constraint in the
CR; pod anti-affinity by hostname is always applied unless overridden.
