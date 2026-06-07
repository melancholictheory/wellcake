# ADR 0004 — Proactive failover on rolling restart

- Status: **Implemented (opt-in, default OFF)** for **all topologies** —
  Replication, Cluster and Sentinel — behind the per-cluster annotation
  `valkey.wellcake.io/proactive-rollout: "true"` (Standalone stays on
  RollingUpdate: a single node, nowhere to fail over to). The operator itself
  drives the rollout under OnDelete one pod at a time, promoting/handing over a
  replica BEFORE recreating the primary, with zero data loss. Validated live:
  e2e `rol-01` (Replication), `rol-02` (Cluster) and Sentinel — on a local
  kind/k3d cluster (8.0→9.1, 0 loss). Remaining: flip the default to ON after a
  soak.
- Date: 2026-06-01 (updated 2026-06-02)
- Context: design review asked whether a rolling restart of a 3-pod StatefulSet
  triggers a failover for every primary and whether that is the right trade-off
  for using a StatefulSet with this in mind.

> **Update (chaos C-3).** Test cha-03 revealed that losing the primary pod led
> to **data loss** (racy): the operator adopted the empty restarted self-master
> and resynced data-carrying replicas down to it. The **reactive-safety** part
> has landed: a Pod watch (immediate reconcile on primary loss) + an empty-master
> guard (pick the master by `DBSIZE`, promoting a data-carrying replica over an
> empty master) — see CHANGELOG. This closes the data-loss case for **unplanned**
> primary loss. The **proactive pre-promote before a planned rolling restart**
> described below (promoting a replica BEFORE deleting the primary pod) has since
> been **implemented** (opt-in, all topologies — see below) — it also removes the
> unavailability window during planned restarts, not just protects the data.

## Context

Today both restart triggers — a config change (config-hash on the pod template)
and the `valkey.wellcake.io/restart` annotation — go through the **StatefulSet
RollingUpdate** strategy. The operator does **not** manage per-pod restarts; the
STS controller does, restarting pods from the highest ordinal to the lowest.

Failover is **reactive**: when the primary pod is restarted, the operator's
survey notices it disappear and promotes a replica on the next reconcile
(`requeueAfter ≈ 15s`). So a rolling restart of a Replication set causes **one**
short unavailability window, when pod-0 (the primary) is hit — not three (a
replica restart triggers no failover). For Cluster, restarting a master pod
triggers one cluster failover per master — inherent to rolling any sharded
stateful system. The cost is a ≈15s failure-detection window per primary, not
data loss (confirmed by e2e `fal-01`: kill primary → promote → data intact).

**Improvement:** issue a *graceful* failover **before** hitting the primary so
that the replica is already primary by the time the old one restarts → the
window shrinks to ~0.

## Implemented (increment 1 — Replication, behind the opt-in annotation)
<!-- Increments: 1=Replication, 2=Cluster, 3=Sentinel — all implemented. -->


Proactive rollout for **Replication** has landed behind the per-cluster opt-in
annotation `valkey.wellcake.io/proactive-rollout: "true"` (default OFF —
existing clusters stay on the reactive RollingUpdate, zero regression risk).
When enabled:

- The STS switches to `updateStrategy: OnDelete` (`updateStrategyFor`,
  `resources.go`) — the STS controller no longer hits pods itself.
- The operator drives the rollout (`rollout.go`), computing the next step from
  live pods: a pod is considered "stale" the same way the STS controller
  considers it for RollingUpdate — by the `controller-revision-hash` label ≠ the
  STS `.status.updateRevision`. This catches **any** pod-template change
  (config-hash, restart token, image, resources), not just config — so switching
  to OnDelete does not silently swallow a restart request. No separate cursor is
  needed; the loop is resumable.
- Order (`nextRolloutStep`, a pure function covered by unit tests): roll the
  **replicas** first (in ascending ordinal; restarting a replica needs no
  failover); while the set has not converged (a non-Ready pod exists) — wait, one
  at a time; when **only the primary** remains stale — promote the most
  up-to-date replica (already on the new revision, by offset —
  `bestRolloutTarget` + the reused `promoteReplica`) and **then** hit the old
  primary pod. The planned-rollout unavailability window shrinks from the
  reactive ~15-20s to ~0.
- While the rollout is in progress, reactive failover is skipped (so the two
  loops don't fight over the primary), and the reconcile is requeued after 5s for
  the next step.

**Done: live validation of increment 1 (Replication).** e2e
`rol-01-proactive-rollout` (functional track): enables the annotation, waits for
`OnDelete` on the STS, triggers a rolling restart and verifies that the operator
itself rolls all 3 pods to the new STS revision (under OnDelete the STS
controller rolls nothing → convergence = proof of an operator-driven rollout),
with the replica promoted BEFORE the old primary is recreated: exactly one
master, 500 keys intact, writes resumed, `status.primary` moved off the old pod.
Green against image `git-b0c3e3df`.

**Implemented (increment 2): Cluster.** Same opt-in annotation + `OnDelete`,
per-shard rollout (`nextClusterRolloutStep` + `driveClusterRollout`, reusing
`surveyCluster`): the shard's replicas roll first, then `CLUSTER FAILOVER` on a
fresh shard replica (no-data-loss slot handover) BEFORE the old master pod is
restarted; one node at a time. A shard with no replicas → direct master restart.
This only makes sense with `replicasPerShard >= 1` — so validation requires the
HA-sharded path to support replicas per shard (previously it could not at all).
Validation — e2e `rol-02` (sharded + one replica per shard).

**Sentinel increment (incr. 3) — IMPLEMENTED.** Proactive rolling restart for
the **Sentinel** topology (`driveSentinelRollout` in `rollout_sentinel.go`).
Both StatefulSets (data + Sentinel monitors) are switched to `OnDelete` under the
same opt-in; the rollout proceeds across two planes, one pod at a time, reusing
`nextRolloutStep`:
1. **Data:** roll the stale replicas first; when only the master remains stale
   (and all replicas are already fresh) — hand over via
   `SENTINEL FAILOVER mymaster` (NOT an operator-driven `REPLICAOF`: in a
   Sentinel topology the failover is owned by the Sentinel quorum, and a direct
   promote would race with their election). The old master is demoted to a
   replica by the Sentinels and is rolled as an ordinary stale replica on the
   next pass.
2. **Sentinel monitors:** stateless — rolled one at a time (primary="" in
   `nextRolloutStep`), with `down-after`/wait-for-Ready holding quorum
   (replicas ≥ 3).
The loop is stateless/resumable: each reconcile recomputes the step from live
pods, so a repeated `SENTINEL FAILOVER` during one already in progress (error
`-INPROG`) is harmless and swallowed. Validated on a local kind/k3d cluster
(8.0→9.1 upgrade of both planes, 0 data loss, sequence from the logs:
replicas → `SENTINEL FAILOVER` → old master → Sentinel pods). Along the way a
latent bug was found and fixed: `ensurePVCSize` matched the Sentinel PVCs (shared
`instance` label) and tried to grow them to the data-PVC size → the reconcile
failed on a non-expandable SC; the fix is to exclude the Sentinel PVCs by name.

**Remaining before flipping the default**: a soak on rol-01/rol-02/Sentinel.
Along the way the following were found and fixed: RBAC `pods: delete` for the
driver, `RetryOnConflict` on the PDB, `ensurePVCSize` vs the Sentinel PVCs (see
above). Known Sentinel-topology cosmetics (not caused by the rollout): after pod
restarts a Sentinel accumulates duplicate slave/peer entries (IP + hostname for
one instance with `announce-hostnames yes`), and `num-slaves`/`num-other-sentinels`
are inflated — cleaned up with `SENTINEL RESET` (follow-up: the operator could do
this itself once the rollout completes).

## Decision

**The design below is accepted and implemented** for all topologies
(Replication, Cluster, Sentinel) behind a per-cluster opt-in annotation, default
OFF — the implementation is validated by a live e2e rolling-restart-failover on a
local kind/k3d cluster (`rol-01`/`rol-02`/Sentinel). Originally the full
implementation was deferred until such live validation existed, because it
touches a data-correctness path (an incorrectly ordered or incorrectly detected
failover is exactly the class of risk the review flags) and requires the operator
to take the rollout away from the STS controller. The reactive failover (≈15s)
remains the safe default until the annotation is set.

## Design (implemented)

Switch the StatefulSet to `updateStrategy: OnDelete` and make the operator drive
the rollout itself, per topology:

- **Replication / Sentinel:**
  1. Roll the replicas first (delete pod, wait for Ready) — no failover
     involved.
  2. For the primary: promote the most up-to-date replica
     (`REPLICAOF NO ONE` on the chosen replica + `REPLICAOF newPrimary` on the
     old one — the operator already has `manualFailover`/`reconcileFailover`),
     update `status.Primary`, **then** delete the old primary pod.
- **Cluster (per master):** issue `CLUSTER FAILOVER` on a master's replica, wait
  for the role to change (`CLUSTER NODES`), then delete the old master pod. Go
  shard by shard, one at a time; respect the PDB.
- Gate the whole sequence on a readiness compatible with
  `spec.podManagementPolicy` and on the existing config-hash, so that one rollout
  converges deterministically.

Reuse: `surveyReplication`/`reconcileFailover`/`manualFailover` (Replication),
`surveyCluster`/`clusterNodes` (Cluster). Drive deletion through a rollout cursor
in status so the reconcile is resumable.

## Required before release

- e2e: rolling restart of Replication×3 and Cluster (≥3 shards) under load,
  asserting **zero failed writes** through the restart (proactive) against the
  current reactive window.
- Chaos: a pod that does not become Ready mid-rollout must not hang the set
  (Parallel pod management already removes the OrderedReady trap).

## Consequences

- **Pros:** near-zero unavailability on config rollouts/restarts when the opt-in
  is enabled; parity with the valkey-io proactive approach.
- **Cons / accepted:** the reactive ≈15s window remains on rollouts **until the
  annotation is set** (default OFF); taking the rollout away from the STS
  controller adds operator complexity (a rollout state machine).

## Revisit trigger

Implement when a k3d/e2e window opens for adding a rolling-restart-failover
scenario, or sooner if production rollouts of durable clusters show unacceptable
write-error windows.

## Addendum: node-local failover on SIGTERM (Valkey ≥9.0)

Proactive rollout covers **planned** rolls (the operator deterministically
promotes a chosen up-to-date replica). **Unplanned** primary termination
(node drain / eviction / preemption / `kubectl delete pod`) is not orchestrated
by the operator — the reactive failover loop handles that (≈15s window).

The Valkey 9.0 directive **`shutdown-on-sigterm failover`** (the operator renders
it version-gated for Cluster, see CHANGELOG / `renderValkeyConf`) provides a
**node-local safety net**: on SIGTERM a cluster primary itself performs a
graceful manual failover to a fresh replica as part of graceful shutdown, BEFORE
the process exits. It only fires while the node is still primary → it does not
conflict with a promote the operator has already done (no double failover). It
requires `terminationGracePeriodSeconds` ≥ `cluster-manual-failover-timeout`
(defaults 30s ≥ 5s — satisfied). Source: upstream valkey-io/valkey-operator
(PR valkey#1091). Complements, rather than replaces, proactive rollout.
