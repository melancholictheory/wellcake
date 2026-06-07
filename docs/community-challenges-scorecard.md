# Self-assessment: community production-challenges vs our operator

> An honest self-assessment against the 5 production-challenges from the weekly meetings of
> [valkey-io/valkey-operator](https://github.com/valkey-io/valkey-operator/wiki/Weekly-meeting).
>
> **Why this document exists.** The underlying principle is sound: a correct managed
> service is built bottom-up — from the tool's constraints toward abstractions; being
> responsible for someone else's data is incompatible with hiding complexity. This document
> applies that same principle to ourselves: every "done" is backed by a link to code / an ADR /
> an e2e test, and every real gap is marked **KNOWN-UNADDRESSED** and "not for production until
> closed." The goal is not to argue about the score, but to measure ourselves honestly.
>
> Cross-checked against the current state of the operator.

## How to read this

- ✅ **Done** — the behavior exists and is confirmed (code + test/e2e).
- 🟡 **Partial** — present, but with a real remainder/caveat.
- ❌ **Gap** — not addressed; marked KNOWN-UNADDRESSED.

Some of the early assessments reflected an **earlier snapshot** of the operator (before ADR 0004 and
`ensurePVCSize`). Below is the version with proofs; where the critique is essentially correct, we
acknowledge it directly.

---

## #1. Race: `CLUSTER FORGET` vs failover election (scale-down)

**Community concern:** del-node (FORGET) during an in-progress failover-election yields an
inconsistent state / data loss. You must check for a live replica before FORGET.

**What we have (`runClusterScaleDown`, internal/controller/cluster.go:573;
script-job cluster.go:636-685):**
- Before del-node, slots are **resharded away** onto the remaining master.
- Guard: `FATAL: still owns slots after reshard; refusing del-node` — del-node
  is not executed while the node still owns slots (protection against slot loss on the
  **planned** path).

**Verdict: 🟡 Partial — but data loss did NOT reproduce (verified).**

**Verification (k3d, 4-shard Durable, 2026-06-04):** the race "kill master kv-1 +
a simultaneous scale-down removing its replica kv-6" → **0 losses** (2000 keys
intact, 16384 slots, `cluster_state:ok`, the kv-1 shard retained its data). The
slot-ownership gate (`cluster.go:682`) + gossip-failover + Durable-PVC all kicked in. Moreover,
the operator's scale-down del-nodes **replicas** (the highest ordinals), not
masters → so the exact race from the critique ("del-node a master during its election") is
**unreachable** via scale-down. The severity of #1 is below the claimed "critical."

**Residual defensive gap:** there is no explicit in-flight-election check before del-node
(it didn't cause data loss, but it's absent as a defense).

**⚠️ By-catch — investigating #1 uncovered a separate confirmed bug:** see the section
["Cluster shard scale-down silently doesn't reduce shards"](#bug-shard-scaledown) below.

---

## #2. Proactive failover before rolling update (zero-data-loss)

**Community concern:** a StatefulSet RollingUpdate hits the primary without a proactive
failover → downtime; you need pre-update health checks + `CLUSTER FAILOVER` before
restarting the primary.

**What we have — this is IMPLEMENTED (ADR 0004); the critique reflects an earlier snapshot:**
- Opt-in annotation `valkey.wellcake.io/proactive-rollout` → the STS switches to
  `OnDelete` (`updateStrategyFor`, resources.go), and the operator drives the rollout itself:
  `proactiveRolloutEnabled` (rollout.go:44), `driveReplicationRollout`
  (rollout.go:110), `driveClusterRollout` (rollout.go:353).
- **Replicas roll first; the primary is promoted BEFORE the old one is deleted**: for
  Replication — promote the most up-to-date replica; for Cluster —
  `clusterFailoverOnPod` (rollout.go:427) on a fresh replica of the shard before restarting
  the old master.
- Confirmed by **live e2e**: `rol-01-proactive-rollout` (Replication) and
  `rol-02-cluster-proactive-rollout` (Cluster) in the e2e harness — the
  unavailability window during a planned rollout is ~0 (see ADR 0004, docs/adr/0004-…).
- **Sentinel (increment 3, verified live on k3d 8.0→9.1, 0 data loss):**
  `driveSentinelRollout` (`rollout_sentinel.go`) rolls two planes one pod
  at a time under `OnDelete` — (1) data pods: first the stale replicas, then a
  handover of the master via **`SENTINEL FAILOVER`** (the failover is owned by the Sentinel
  quorum — the operator does NOT promote directly, otherwise it races their election);
  (2) Sentinel monitors one at a time while preserving quorum. In the higher-level platform /
  GitOps layer, Sentinel is deliberately not exposed (a niche).

**Verdict: ✅ Done** (behind opt-in; default OFF for the duration of the soak — a deliberate
choice, not a missing feature; flipping the default after the soak is in the ROADMAP).
**Caveat:** the health gate "cluster healthy + synced replica before each step" is
covered partially (promotion follows the most up-to-date replica); a formal
pre-update `isClusterHealthy()` as in the community proposal is a candidate for strengthening.

---

## #3. Volume provisioning: working around immutable `volumeClaimTemplates`

**Community concern:** VCTs are immutable; growing storage requires patching live PVCs;
you need auto-patch, not just validation.

**What we have — auto-patch is IMPLEMENTED; the critique reflects an earlier snapshot:**
- `ensurePVCSize` (internal/controller/valkeycluster_controller.go:474) patches
  **live data PVCs** when `spec.storage.size` grows, working around the immutable VCT
  (grow-only; requires a StorageClass with `allowVolumeExpansion`). Covered by the
  `TestEnsurePVCSize` test.
- CEL forbids shrinking (`storage.size cannot shrink`).

**Verdict: ✅ Done.** **Caveat/remainder:** there is no `StorageExpansionPending`
condition; the actual filesystem resize depends on the CSI (online/offline);
PMP/VCT is immutable on existing STSes (new clusters use Parallel; see ADR 0002).

---

## #4. Failover logic placement (proactive vs reactive)

**Community concern:** you need a clear separation of planned (operator → `CLUSTER
FAILOVER`) and unplanned (native election); operator-driven failover for
Replication = SPOF.

**What we have:**
- **Proactive** — see #2 (implemented, ADR 0004).
- **Reactive Replication** — operator-driven (survey roughly every ~15s) +
  an **empty-master guard**: master selection by `DBSIZE`, promoting a replica-with-data
  over an empty restarted self-master (`betterMaster` failover.go:363,
  guard failover.go:278). A Pod watch → immediate reconcile when the primary is lost.
- **Cluster** — native gossip (the operator does not interfere with the election).

**Verdict: ✅ Proactive done; 🟡 reactive-SPOF honestly documented.**
The residual risk (operator down **and** primary down simultaneously → promotion is
deferred) is real and is **written into the ROADMAP as AR1**: diagnostics (cha-04) showed that
liveness restarts a hung primary in ~8s, and the empty-master guard removes the racy
data-loss; a hard gate / a durable→Sentinel default is an optional follow-up. This is not
"incorrect separation," but it is also not "fully closed" — recorded as-is.

---

## #5. Shard-aware anti-affinity (co-location → data loss)

**Community concern:** the default scheduler places a shard's primary+replica on
the same node → a node failure loses the shard. You need shard-aware anti-affinity.

**What we have — CLOSED via per-shard workload (ADR 0005, opt-in):**
- **Single-STS (default):** anti-affinity is **`preferred`** by the `instance` label
  (the whole cluster), **not by shard** — shard identity cannot be expressed in pod
  labels with a single STS. This **remains a gap for single-STS** (preferred under
  scheduler pressure does not save you).
- **`spec.perShardWorkload: true` (ADR 0005 / O1):** one StatefulSet per shard →
  each pod carries the shard label `cache.wellcake.io/shard`, and `shardAntiAffinity`
  (`cluster_pershard.go:397`) sets **REQUIRED** anti-affinity, scoped by
  `instance`+`shard`: a single shard's primary+replicas **cannot** land on
  the same node, while DIFFERENT shards co-locate freely.

**Verdict: ❌ for single-STS → ✅ closed by per-shard workload (opt-in),
verified live on k3d (2026-06-06).** A 3-shard HA cluster with
`perShardWorkload=true`: each shard's primary+replica on DIFFERENT nodes; the scheduler
explicitly excluded the primary's node (`FailedScheduling: didn't match pod
anti-affinity rules`) — proving REQUIRED (not preferred). On the live pod:
`requiredDuringSchedulingIgnoredDuringExecution`, selector
`{instance, cache.wellcake.io/shard}`.
**Remainder:** in the default single-STS mode the gap persists (by design —
shard-aware placement requires a per-shard layout); the recommendation for durable HA is
`perShardWorkload: true`.

---

## Recount

| # | Challenge | Critique | Honest (with proofs) |
|---|---|---|---|
| 1 | FORGET vs failover (scale-down) | ❌ | 🟡 verified: data-loss did NOT reproduce (slot-guard effective); uncovered a by-catch ↓ |
| 2 | Proactive failover (rolling) | ⚠️ | ✅ ADR 0004 + e2e rol-01/rol-02/Sentinel (opt-in, all topologies) |
| 3 | PVC auto-patch | ⚠️ | ✅ `ensurePVCSize` + test |
| 4 | Proactive vs reactive | ❌ | ✅ proactive; 🟡 reactive-SPOF documented (AR1) |
| 5 | Shard-aware anti-affinity | ⚠️ | ✅ closed by per-shard workload (opt-in), verified live; ❌ for default single-STS |

**Bottom line:** not 1/5 — closer to **4 ✅ / 1 🟡** (with opt-in caveats), with honest
remainders. The point is not the number, but that each one is backed by a link/proof (and where
there is a remainder, it is named explicitly: #5 is closed only in per-shard mode, #1 has a
defensive gap).

---

## <a id="bug-shard-scaledown"></a>🐞 By-catch: Cluster shard scale-down silently doesn't reduce shards

Investigating #1 uncovered a **separate confirmed bug** (reproduced cleanly, without
chaos, on k3d, 2026-06-04):

**Symptom:** `spec.shards: 4→3` → `phase=Ready`, but `readyShards=4`, and the cluster
still has **4 masters** (all 16384 slots), while 2 masters are **silently de-replicated**.
The intent to remove a shard is **silently ignored** + false convergence (`Ready`).

**Root cause:** scale-down removes the highest ordinals
`leaving=[totalReplicas(new), lastApplied)`. With a **masters-first** bootstrap layout
(kv-0..S-1 are masters, then replicas) the highest ordinals are
**replicas** → and an STS can only remove the highest ordinals → so a shard (master+replica)
cannot be removed. This is a structural tension of the **single-STS** model → [ADR 0002](adr/0002-workload-primitive.md).
(Reducing `replicasPerShard` works correctly — there the highest ordinals are indeed
replicas.)

**Why it matters:** a silent reduction of redundancy (for the Cache profile, a
subsequent failure of a de-replicated master = data loss) + exactly the
"edge case behind the abstraction" that the critique warned about.

**Honest guard (single-STS):** CEL rejects **reducing `shards`** for
single-STS as unsupported — `api/v1beta1/valkeycluster_types.go`
(XValidation on spec), with a clear message. A loud rejection is better than a silent
misbehavior. Verified on k3d (4→3 reject in single-STS, 4→5 passes).

**Deep fix — DONE via per-shard workload (ADR 0005 / O1), verified live on
k3d 2026-06-06.** With `spec.perShardWorkload: true` each shard is a separate STS,
so reducing `shards` removes **exactly the shard**: the operator reshards the slots of
the leaving master onto a remaining one (`buildShardScaleDownScript`, ASM on 9.1),
del-nodes the replica then the master (FATAL-guard if the master still owns
slots), then deletes the shard's STS + headless Service (`deleteLeavingShards`). CEL
**allows** reducing `shards` specifically for per-shard. A 4→3 run on k3d:
`cluster_size` 4→3, the leaving STS `psw-sh3` + svc removed, `cluster_known_nodes`
6 (no ghost nodes), `cluster_slots_assigned:16384`, `cluster_state:ok`, **3000
keys intact (0 losses)**, `phase=Ready` reflects real convergence (not a
false one). The C4 root cause is closed for per-shard; in single-STS — still an honest CEL
reject.

## Where the critique is essentially right (accepted)

- **#1 — a real defensive gap** (there is no explicit in-flight-election check before
  FORGET, even though it didn't cause data loss). **#5 was an architectural gap of the
  single-STS model — closed by per-shard workload (ADR 0005), but only in opt-in mode**;
  the default single-STS cannot do shard-aware placement (by design).
- **topology-aware placement / weaknesses of the k8s scheduler** — not addressed (the same
  signal from the community).
- **Bus-factor = 1** — a real organizational risk.
- **Breadth of features + good docs create a false sense of production-readiness** —
  this is the main "deception" risk. The antidote is this document and the
  KNOWN-UNADDRESSED markers, not "operator handles failover."

## Honest cross-cutting limitations (not for production until closed)

- Proactive rollout — **opt-in, default OFF** (soak not yet passed).
- Cluster slot-migration — on 9.1 it's **atomic slot migration** (C3/ADR 0001,
  implemented); on 8.x it's key-by-key `MIGRATE` (a latent risk on large keys).
- Chaos coverage for the newer features (password/TLS rotation, tmpfs) — only on local
  k3d, not in CI.
- Shard-aware placement (#5) and correct shard scale-down (C4) — closed only
  with `perShardWorkload: true` (opt-in, verified on k3d); default single-STS —
  preferred-anti-affinity + CEL-reject on reducing `shards`.
- FORGET-vs-election coordination (#1) — a defensive gap (it didn't cause data loss).

## Related documents

- [ADR 0002 — workload primitive (StatefulSet vs ValkeyNode)](adr/0002-workload-primitive.md)
- [ADR 0003 — build vs adopt valkey-io](adr/0003-build-vs-adopt.md)
- [ADR 0004 — proactive failover during rolling restart](adr/0004-proactive-failover-rolling-restart.md)
- [ROADMAP](../ROADMAP.md) (AR1 — reactive-SPOF; C3 — ASM; S/EC — security/edge)
