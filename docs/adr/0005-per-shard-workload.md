# ADR 0005 — Per-shard workload for Cluster (StatefulSet-per-shard)

- Status: **Accepted and implemented** (`spec.perShardWorkload`, opt-in,
  immutable, Cluster-only; go/no-go = GO 2026-06-04). Increments I1–I4
  implemented and merged, **verified live on a local kind/k3d cluster
  2026-06-06** (REQUIRED shard-scoped anti-affinity; clean shard scale-down 4→3
  via ASM, 0 loss). Remaining (I5, before prod): e2e at the platform layer + soak
  + hardening of live-master reshard. The PoC story is below.
- Date: 2026-06-04
- Context: a revisit of the decision in [ADR 0002](0002-workload-primitive.md)
  ("one StatefulSet per cluster + mitigations") — the revisit trigger recorded
  there fired. Two independent signals converged on **one root cause —
  single-STS**.

## Context

Today the Cluster topology renders as **one StatefulSet** for the whole cluster
(`buildStatefulSet`, resources.go:709): all shards (masters + replicas) are pods
of a single STS, identity by ordinal, bootstrap via `valkey-cli --cluster create`
(`runClusterBootstrap`). Two signals showed this hits an architectural wall:

1. **C4 (found empirically, 2026-06-04).** Decreasing `spec.shards` silently does
   NOT reduce the number of shards: scale-down removes the highest ordinals
   `leaving=[totalReplicas(new), lastApplied)`, but with the masters-first
   bootstrap layout those are **replicas**, not a shard. Result: the prior number
   of masters, 2 masters de-replicated, `phase=Ready` (falsely). Currently closed
   by an honest guard (CEL rejects decreasing `shards`), but that is a refusal,
   not a solution.

2. **#5 community (shard-aware anti-affinity).** The default scheduler can place
   the primary and replica **of the same shard** on the same node → a node
   failure loses the shard. **This is inexpressible with one STS:** anti-affinity
   lives in the **pod template, shared by all pods** of the STS. One template ⇒
   one rule for all pods — you cannot say "the pods of shard X on different
   nodes, but different shards may share a node." `defaultAntiAffinity`
   (resources.go:1032) is `preferred` by the `instance` label (the whole
   cluster), and there is no shard label on the pods at all.

3. **As a bonus** single-STS does not allow per-shard rollout / image / resources
   override (this is [C1](../../ROADMAP.md) in the ROADMAP).

**Key conclusion:** shard-aware anti-affinity ⟺ per-shard pod-template ⟺
**a workload object per shard** (or finer). There is no "within one STS"
workaround. And that same per-shard object cleanly solves C4 (remove a shard =
remove its STS).

## Requirements (forces)

- **Shard-aware anti-affinity:** a shard's master + replicas — on different nodes
  (`required`, not `preferred`).
- **Cross-shard spread:** spread different shards across nodes/zones
  (failure-domains), but allow co-location when nodes are scarce.
- **Clean shard scale-down:** remove a shard = remove its pods + PVCs, without
  touching other shards (truly closes C4).
- **Desirable:** per-shard rollout/image/resources (C1).
- **Migration** of existing single-STS Cluster clusters.
- **Do not touch** Standalone/Replication/Sentinel — single-STS is enough for
  them.

## Options

### O0. Interleaved layout (stay on one STS)
Layout `shard i = ordinals 2i, 2i+1` so the highest ordinals = the shard being
removed.
- ✅ Fixes C4 (shard scale-down correctly removes master+replica).
- ❌ Does **NOT** solve #5: one pod-template ⇒ shard-aware anti-affinity is still
  inexpressible (anti-affinity by instance spreads ALL pods — over-constrains, or
  preferred — no guarantee).
- Cheap but half-measure (C4 only).

### O1. StatefulSet-per-shard  ⟵ recommendation
N StatefulSets (one per shard), each `1+replicasPerShard` pods (the shard's
master+replicas). The operator MEETs them into one cluster and distributes slots.
- ✅ **#5:** its own pod-template per shard → `required` podAntiAffinity among the
  pods of its own shard (by shard label) + `topologySpreadConstraints` between
  shards.
- ✅ **C4:** remove a shard = reshard slots away + delete the shard's STS (+ its
  PVCs). No ordinal puzzles.
- ✅ **C1:** per-shard rollout/image/resources — natural (separate STS).
- ✅ Stay on the **proven STS controller** (unlike O2).
- ❌ Cost: N STS per cluster (across many clusters × shards — object volume and
  reconcile complexity), a new bootstrap (create N STS → wait → MEET → assign
  slots), divergence of the Cluster code path vs the other topologies.

### O2. ValkeyNode / bare-pods (CNPG model, as in valkey-io)
The operator manages pod+PVC per node itself (no STS). Maximum control (per-pod
placement, surgical replacement), but **reinvents the STS controller** — exactly
what [ADR 0002](0002-workload-primitive.md) already rejected for Valkey
(the surface+risk are not justified by Valkey's failover simplicity). The most
expensive.

### O3. Adopt the valkey-io operator
They already have ValkeyNode + shard-aware placement + sequential-roll health
checks being built. But Cluster-only, without backup/restore, a kubectl plugin
and 4 topologies — see [ADR 0003](0003-build-vs-adopt.md) (build-vs-adopt) and
the hybrid variant there.

## Decision (accepted — implemented, opt-in)

**O1 — StatefulSet-per-shard for the Cluster topology.** The minimal primitive
that closes #5 + C4 + C1 at once, while staying on the STS controller (not
reinventing it like O2). Standalone/Replication/Sentinel stay on one STS — they
do not need per-shard.

Design clarifications:
- Within a shard-STS the master/replica roles are **dynamic** (gossip/failover) —
  it is "the pod set that owns the shard's slots," not a fixed master by ordinal.
- Anti-affinity: on the shard-STS template — `requiredDuringScheduling`
  podAntiAffinity by a new `cache.wellcake.io/shard` label (a shard's pods on
  different nodes) + `topologySpreadConstraints` between shards.
- Shard scale-down: reshard the shard's slots away → `valkey-cli del-node` its
  pods → delete the shard's STS + PVCs. Replaces ordinal-removal (drops the
  C4-guard).

## Migration

- **API:** the v1beta1 Hub + conversion-webhook groundwork already exists
  (`api/v1beta1/conversion.go`, E1). The new behavior is behind the Cluster
  topology + a feature flag / a new minor; default single-STS during the soak,
  then flip.
- **Existing single-STS Cluster:** "in place" is impossible (the STS shape is
  immutable; the PMP/VCT lesson from ADR 0002). Migration = **blue-green
  recreation**: stand up a per-shard cluster alongside + restore from backup
  ([C2](../../ROADMAP.md) already does this) + switch clients over. Or an
  admission-warn: existing ones stay single-STS until recreated.
- Staged: PoC → flag (default off) → soak → flip the default.

## Consequences

- **Pros:** #5 shard-aware placement; C4 — real shard scale-down; C1 — per-shard
  rollout/image/resources; better failure-domain.
- **Cons / accepted:** N STS objects per Cluster (volume at scale); a new
  bootstrap/scale code path; divergence of Cluster vs the other topologies;
  migration by recreation only.

## What it unblocks (explicitly)

- **C4** ([ROADMAP](../../ROADMAP.md)) — drops the honest-guard, gives a correct
  shard scale-down.
- **#5** ([scorecard](../community-challenges-scorecard.md)) — shard-aware
  anti-affinity.
- **C1** — per-shard rollout/image/resources.

## PoC results (k3d, 2026-06-04)

A manual PoC of O1 (3 shards × 2 pods = 3 StatefulSets + 3 headless svcs, no
operator) **confirmed all 3 key claims:**

1. **Shard-aware anti-affinity WORKS** (what single-STS cannot). Each shard-STS
   has its own pod-template with `requiredDuringScheduling` podAntiAffinity by the
   `shard` label → each shard's master+replica landed on **different nodes**
   (sh*-0 → server-0, sh*-1 → agent-1). A single-STS with a shared template
   cannot do this.
2. **The cluster assembles on top of the per-shard STS** via `--cluster create`
   (3 masters) + `add-node --cluster-slave` (replicas): `cluster_state:ok`, 16384
   slots, `cluster_size:3`, 2000 keys.
3. **Clean shard scale-down = delete the shard's STS.** Removed shard 2: reshard
   5461 slots away → `del-node` of the master+replica → `kubectl delete sts/svc
   sh2`. Result: `cluster_size:2`, 4 nodes, **2000 keys intact** (reshard moved
   the data), the remaining shards keep anti-affinity. No ordinal-removal and no
   de-replication (unlike C4 on single-STS).

**Observations for the implementation:**
- Bootstrap/scale/failover across the per-shard STS must be driven by the
  operator (the PoC did `valkey-cli` by hand); the logic is close to the current
  Cluster assembly.
- Gossip stability: the operator already supports `cluster-announce-hostname`
  (FQDN) — it survives a pod restart (the PoC used the pod IP, which is static).
- **The volume cost is confirmed:** Cluster now has N×(STS+svc) instead of 1 —
  the main open question for large fleets (see review).

**Conclusion:** O1 is technically sound and closes #5 + C4 + C1. The go/no-go
decision (build O1 ourselves vs O3 adopt valkey-io) is for the team.

## Triggers / plan

1. ~~**PoC on k3d**~~ ✅ **done (2026-06-04, positive)** — see "PoC results"
   above.
2. ~~**Go/no-go**~~ ✅ **GO (2026-06-04): we build O1 ourselves.**
3. Implementation — see "Implementation plan" below.

## Implementation plan (O1, by increment)

Everything behind the feature flag `spec.perShardWorkload` (Cluster-only,
**immutable**, default false) — the single-STS path is untouched, the new one is
opt-in; migration = recreation. Each increment was validated on k3d before the
next. **Status: I1–I4 implemented and merged, verified live on k3d 2026-06-06;
I5 — partial (soak in progress, e2e shd-02 at the platform layer), hardening of
live-master reshard on scale-down — open.**

- **I1 (foundation, current):** the API field `perShardWorkload` (immutable +
  Cluster-only CEL) + a builder for the per-shard STS/Service
  (`buildShardStatefulSet`, `shardStsName`/`shardHeadlessName`) with a `shard`
  label and `required` shard-aware anti-affinity + cross-shard topology-spread.
  Unit tests. Verify the rendering on k3d (pods come up, anti-affinity visible).
  Bootstrap not yet touched.
- **I2 (pod enumeration + bootstrap):** a single source of truth
  `clusterDataPods(vc) []podRef{host,shard,ordinal}` hiding single-STS vs
  per-shard. `runClusterBootstrap` assembles the cluster on top of the per-shard
  STS (MEET + create 3 masters + replicate, as in the PoC). The cluster is formed
  by the operator.
- **I3 (survey/status/failover):** move the FQDN-building places
  (`surveyCluster`, failover, rollout, password/TLS-reload, backup) onto
  `clusterDataPods`. Per-shard cluster management works.
- **I4 (scale):** shard scale-up = add a shard-STS + MEET + rebalance; shard
  scale-down = reshard away (ASM `$ASM_FLAG` on 9.1+, ADR 0001) + del-node +
  delete the shard-STS (`deleteLeavingShards`; drops the C4-guard for per-shard).
  Replaces ordinal-removal.
- **I5:** migration (blue-green via restore C2) + e2e at the platform layer +
  flip/soak.

Production-readiness — after I5 (e2e at the platform layer + soak + hardening of
live-master reshard); the soak is running. Until then `perShardWorkload` is
experimental (opt-in).
