# Chaos / integration testing plan

The operator manages a **stateful data plane** — failover and cluster
reconciliation are fundamentally a distributed-consensus problem. Unit + envtest
validate the logic in isolation; they do **not** exercise interaction with a real
Valkey (gossip timing, PSYNC, AOF-rewrite duration) under failure. This plan
closes that gap (design-review P0: "no chaos / fault injection").

## Where chaos tests run

- **Not** in the operator's own CI: envtest has no real Valkey pods — you cannot
  kill a pod, cut the network, or slow down a disk.
- **kind e2e harness** (kuttl): real Valkey via the chart, real pods. This is
  where pod-level failure scenarios live (`tests/e2e-advanced/`, to be extended
  with a `cha-*` series). Base + advanced already run here in CI.
- **k3d local** is unreliable for chaos: the inter-node pod network flaps
  (`No route to host`, nodes blinking `fail?`) — false split-brain signals,
  which is the environment, not the operator. Use single-node CI kind for
  determinism.

## Scenarios

Severity = risk of data loss / split-brain if the behavior is wrong.

| # | Scenario | How we inject it | What we verify | Possible in kind? |
|---|---|---|---|---|
| C-1 | **Majority / quorum loss** (delete 2/3 of the Replication pods) | `kubectl delete pod` ×2 | recovers to 3 Ready, a single primary, data intact | ✅ pod-kill only |
| C-2 | **Operator crash mid scale-down** | delete the operator pod during the cluster scale-down Job | no orphaned pods/slots; converges after restart; `cluster_state:ok` | ✅ pod-kill only |
| C-3 | **Primary data integrity on failover** (stricter fal-01) | write N keys → kill the primary → promote | all N keys present on the new primary; writes resume | ✅ pod-kill only |
| C-4 | **Network partition / split-brain** (isolate the primary from its replicas) | NetworkPolicy / Chaos Mesh netem partition | exactly one primary; on heal the old primary comes back as a replica (no dual-master divergence) | ⚠️ needs a CNI that enforces NetworkPolicy (Calico) or Chaos Mesh — kindnet does not enforce it |
| C-5 | **Slow BGSAVE > down-after** (premature failover) | Chaos Mesh IO delay / `stress` on the data volume | the operator does NOT promote on a slow-but-alive primary; no flapping | ⚠️ needs fault injection (Chaos Mesh `IOChaos`) |
| C-6 | **PVC full during AOF rewrite** | fill the PVC | writes are rejected cleanly; the operator surfaces this; recovery on expansion (see ensurePVCSize) | ⚠️ needs a small fillable PVC + CSI |
| C-7 | **Concurrent CR updates** (reconcile race) | rapid `kubectl patch` of spec + status | no lost update; status converges (optimistic concurrency) | ✅ API only |

## Implementation layers

1. **Layer 1 — pod-kill (no extra infra):** C-1, C-2, C-3 — kuttl cases
   `tests/e2e-advanced/cha-NN-*`, wired into the advanced matrix
   (manual/scheduled, starting as `allow_failure` → blocking once stable). C-7
   (no data plane) lives in the **operator's envtest** (gating), not in kind.
2. **Layer 2 — fault injection:** C-4, C-5, C-6. These require Chaos Mesh
   installed in kind (or Calico to enforce NetworkPolicy). Add a `chaos-mesh`
   setup step to the e2e env (devspace) behind a CI variable, then write the
   netem/IOChaos scenarios.

## Standardize the assertions

Beyond "pods Ready", every chaos case must verify:

- **Data integrity:** a known keyspace written before the failure is fully
  present after recovery (no silent loss).
- **Single primary / slot coverage:** exactly one primary per shard;
  `cluster_slots_assigned:16384` for Cluster.
- **No flapping:** the operator does not oscillate primaries (a bounded number
  of failovers per window).
- **Convergence time:** recovery within an SLO bound (defined per scenario;
  feeds the operator's SLOs — see production-readiness).

## Status

- Plan: this document.
- Layer 1 scenarios:
  - **C-1 (quorum loss)** — done, `tests/e2e-advanced/cha-01-quorum-loss`,
    green. Confirmed: data intact under simultaneous loss of primary + replica.
  - **C-2 (operator crash mid scale-down)** — done, `cha-02-operator-crash-scaledown`.
    **Found a real data-loss bug in scale-down** (leaving nodes were not matched
    because of the `:port` suffix → pods holding slots were deleted →
    `cluster_slots_fail`). Fixed in the operator (`buildScaleDownJob`:
    bare-FQDN match + reshard onto `myself` + a safety gate on slot ownership),
    see CHANGELOG. The test validates the fix.
  - **C-3 (data integrity on failover)** — done, `cha-03-failover-integrity`.
    **Found a real (racy) data loss**: when the primary pod is lost and quickly
    recreated empty, the operator adopted the empty self-master and resynced the
    data-bearing replicas down to it. Fixed in the operator (Pod watch for
    immediate reconcile + empty-master guard: master selection by `DBSIZE`,
    promoting a data-bearing replica over an empty master), see CHANGELOG /
    ADR 0004. The test asserts the data-plane invariant (DBSIZE preserved),
    not "the primary changed".
  - **C-7 (reconcile race / optimistic concurrency)** — done. This is the only
    Layer-1 scenario without a data plane, so it lives in the **operator's own
    envtest** (`internal/controller`, real kube-apiserver, gating) rather than
    the kind e2e harness: a storm of concurrent spec patches against the
    operator's status patches → verifies that there is no lost update (every
    writer's commit survives) and that status converges
    (`observedGeneration == generation`). Confirmed the contract: status patches
    via MergeFrom (subresource) do not roll back spec, and optimistic
    concurrency retries until success.
- **Layer 2 — offline approximations (no Chaos Mesh):**
  - **C-4 (split-brain reconciliation)** — done, `tests/e2e-advanced/cha-04-split-brain`.
    A true netem partition requires Chaos Mesh (see below, blocker); the
    approximation uses `DEBUG SLEEP` on the primary. Invariant: a "hung" primary,
    on returning with its data, does not leave two masters (**exactly one
    master** + data intact + writes). It tests collapsing two data-masters
    (`pickAuthoritativeMaster`).
    **Observation AR1 — investigated (diag run): there is no gap.** The operator
    detects the hung primary as unreachable and promotes a replica (log
    `promoting replica`). "Did not fail over" was an artifact: the liveness probe
    restarts the hung container in ~8s → "hang" = "restart of an empty pod"
    (empty-master guard), and with equal-zero data the original ordinal is
    re-adopted. The diag run also surfaced STS conflict churn in
    `ensureStatefulSet` → fixed (RetryOnConflict).
  - C-5 (slow BGSAVE → no premature failover) — **TODO** (approximation via
    `rdb-key-save-delay`: the operator must not promote a slow-but-alive primary).
- **Layer 2 — true fault injection (Chaos Mesh): BLOCKED on offline infra.**
  Requires mirroring into an internal registry: the Chaos Mesh helm chart
  (`charts.chaos-mesh.org`) + images (`ghcr.io/chaos-mesh/*`). C-6 (PVC-full) is
  not reproducible on kind: local-path does not enforce PVC size and cannot do
  expansion — a CSI with quotas is needed.
  → org/infra dependency (mirroring + CSI/Calico-in-kind).
- Existing adjacent coverage: `fal-01` (basic operator-driven failover),
  `shd/scl/bkp` (bootstrap/scale/backup) — happy-path, no fault injection.
