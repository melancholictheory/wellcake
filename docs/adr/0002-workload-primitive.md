# ADR 0002 — Workload primitive: StatefulSet vs Deployment vs operator-managed pods

- Status: **Accepted** (StatefulSet, with mitigations)
- Date: 2026-06-01 (updated 2026-06-04)
- Context: a design review noted that all four topologies use a StatefulSet
  (STS), with no Deployment path for the stateless Cache profile, and pointed out
  the classic sharp edges of an STS (OrderedReady blocking, immutable
  volumeClaimTemplates, failovers on rolling update). As an example of an
  operator that manages pods directly without an STS/Deployment, the review cited
  CNPG (CloudNativePG).

> **Update (2026-06-04, second review).** The same set of concerns was raised
> again. Cross-checking against the code: 3 of the 4 concrete "the operator
> doesn't understand the constraints" claims **are already addressed and
> commented in the code** (Parallel PMP, `ensurePVCSize`, the Cache/Durable
> profiles), and "3 failovers on a rolling update" is inaccurate (in Replication
> it's ≤1, only bouncing the primary). Two things have landed/been clarified
> since the ADR was accepted:
>
> 1. **Proactive failover has landed** (ADR 0004) for **all topologies** —
>    Replication (`rol-01`), Cluster (`rol-02`) and Sentinel (verified on k3d, 0
>    data loss): the operator promotes a replica / issues `CLUSTER FAILOVER` /
>    `SENTINEL FAILOVER` **before** deleting the primary pod under OnDelete → the
>    unavailability window during a planned rollout is ~0 (for Sentinel the
>    promotion is decided by the Sentinel quorum, not the operator directly). The
>    "failover window on rolling update" minus in "Consequences" below is
>    effectively closed (behind an opt-in annotation, default OFF until soak).
> 2. **A tmpfs path for Cache is implemented** (revisit trigger #1 below) —
>    `storage.medium=Memory` puts the data dir on `emptyDir{medium: Memory}` (RAM)
>    instead of a PVC. This removes the "emptyDir is unsafe" argument: tmpfs is
>    **memory-backed**, counted against the pod's memory limit and NOT against
>    ephemeral-storage → it does not trigger `nodefs` eviction. In other words,
>    the flexibility the review wanted via a Deployment is obtained **without** a
>    second primitive — we stay on StatefulSet (stable identity) but drop the
>    PVC lifecycle for Cache. Verified on k3d (Replication+Cache+Memory: no PVC,
>    tmpfs `/data`, persistence forced off, a replica resyncs from the master
>    after a kill). See CHANGELOG.

## Context

Each topology (Standalone, Replication, Sentinel, Cluster) is rendered as a
single StatefulSet + headless Service. Both the Cache profile (ephemeral data)
and the Durable profile (RDB/AOF on a PVC) use the STS path with a per-pod PVC.

Reviewers raised three concrete STS properties and two alternatives:

1. **OrderedReady blocks the whole set.** With the default `PodManagementPolicy`,
   a single Pending pod (for example, its PVC bound to a cordoned node with local
   storage) blocks all pods with a higher ordinal and all rolling
   updates / scale-ups.
2. **volumeClaimTemplates are immutable.** Storage growth cannot go through the
   STS spec; PVCs must be patched directly.
3. **Rolling update → failovers.** Restarting a primary/master pod triggers a
   failover for each primary.
4. **A Deployment would be more flexible for Cache** (no ordinal identity, no PVC
   lifecycle, faster scale).
5. **CNPG manages bare pods directly** (no STS/Deployment) for full per-pod
   control.

## Decision

**Keep StatefulSet for all topologies** with the following mitigations (some
already shipped):

- **`PodManagementPolicy: Parallel`** (shipped). Removes the OrderedReady
  head-of-line blocking — a Pending pod no longer freezes the rest. Valkey
  tolerates a parallel start: replicas do `replicaof pod-0` with retries, Cluster
  pods are independent until bootstrap, Sentinels discover each other. (Immutable
  on existing STS — applied to newly created clusters.)
- **Operator-managed PVC expansion** (shipped, `ensurePVCSize`). Since
  volumeClaimTemplates are immutable, the operator patches live data PVCs when
  `spec.storage.size` grows (the StorageClass must allow volume expansion).
- **Failovers on rolling update** — see the follow-up ADR on proactive failover
  (issue a graceful promote / `CLUSTER FAILOVER` before restarting a primary to
  skip the failure-detection window). Today failover is reactive (≈15s).

### Why not a Deployment for Cache

- **A disk-backed emptyDir is unsafe in our clusters.** Ephemeral-storage
  pressure causes pod eviction (`nodefs.available`); for an in-memory service
  this is an availability hit — see `feedback-no-emptydir`. **But an important
  distinction:** this is about a disk-backed emptyDir (the default).
  `emptyDir{medium: Memory}` (tmpfs) is **RAM-backed** → counted against the
  pod's memory limit, not ephemeral-storage → the eviction argument does not
  apply. That's why we added `storage.medium=Memory` for Cache (see the update
  above): the data dir on tmpfs, no PVC, **while staying on StatefulSet**. A
  Deployment with a shared PVC still doesn't work (RWO, per-pod identity needed).
- **Stable network identity.** Replication/Sentinel/Cluster need stable per-pod
  DNS (`pod-0.headless…`) for `replicaof`, gossip and bootstrap. Deployment pods
  are interchangeable — identity would have to be built ourselves.
- The Deployment path would benefit only a single Standalone/Cache replica, and
  even there the PVC-for-eviction-protection logic applies. The complexity of a
  second workload primitive isn't justified for that narrow case.

### Why not operator-managed bare pods (the CNPG model)

CNPG gains per-pod control (no OrderedReady, selective restart, no immutable
template) by managing Pods + PVCs directly. This is powerful, but:

- It reinvents a large part of the STS controller (ordinal identity, PVC
  lifecycle, ordered/parallel rollout, recreation) — a significant increase in
  the operator's surface area and bug risk for a stateful data plane.
- CNPG can afford this because the Postgres team has deep, stable lifecycle
  knowledge; the cost is justified by Postgres's complexity. For Valkey the STS
  abstraction plus targeted mitigations cover our needs at a fraction of the risk.

## Consequences

- **Pros:** we keep a battle-tested STS controller; a small, auditable operator;
  two concrete sharp edges (OrderedReady, storage growth) closed.
- **Cons / accepted:** the failover window on rolling update remains until the
  proactive-failover follow-up lands; there's no fast Deployment path for Cache;
  the per-pod control of bare pods is unavailable.

## Revisit triggers

- ~~A concrete Cache workload where the PVC lifecycle measurably hurts and
  emptyDir eviction is demonstrably mitigated → evaluate a Deployment path for
  Standalone+Cache.~~ **Addressed (2026-06-04):** not via a Deployment, but via
  `storage.medium=Memory` (tmpfs data dir on a StatefulSet) — eviction is
  mitigated by construction (RAM-backed, not ephemeral-storage). Remaining,
  optional: soak + an e2e case, measuring tmpfs RAM overhead under load, and the
  behavior of `sizeLimit` (on older k8s the mount shows the node default ~50% of
  RAM; the kubelet still enforces `sizeLimit` via eviction — actual RAM usage =
  only what's written, for Cache ≈0).
- ~~Recurring incidents from STS immutability / rollout ordering → evaluate a
  CNPG-style bare-pod model.~~ **Triggered (2026-06-04):** the single-STS turned
  out to be the root of two problems at once — **C4** (shard scale-down silently
  fails to reduce the number of shards) and **#5** (shard-aware anti-affinity is
  inexpressible with a single pod template). The revisit was moved to
  **[ADR 0005](0005-per-shard-workload.md)** (StatefulSet-per-shard for Cluster),
  with the options/migration/PoC plan there. **Implemented** (opt-in
  `spec.perShardWorkload`, immutable, Cluster-only, experimental): closes C4 (a
  clean shard scale-down = deleting the STS+Service of the departing shards) and
  #5 (hard shard-aware anti-affinity). Verified live on k3d 2026-06-06 (4→3
  shards, 16384 slots intact, 0 loss). Before prod — e2e at the higher-level
  platform layer + soak.
- The official valkey-io operator adopts one of these models → revisit (see
  [ADR 0003](0003-build-vs-adopt.md)).
