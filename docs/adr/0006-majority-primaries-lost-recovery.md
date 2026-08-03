# ADR 0006 — Recovery from a lost primary majority (Cluster topology)

- Status: **Implemented** for **Cluster** topology. Automatic for the **Cache**
  profile; **opt-in for Durable** via the per-cluster annotation
  `valkey.wellcake.io/quorum-takeover: "true"` (a forced takeover can drop
  acknowledged writes, which the Durable profile promises to keep). Validated
  live on k3d: a 3-shard cluster, majority of primaries fenced, recovered to
  `cluster_state:ok` with **0 data loss** and the fenced primaries rejoining as
  replicas. See "Validation" below.
- Date: 2026-08-04
- Context: ADR 0004 handles *planned* restarts and *single* primary loss (a
  replica is promoted, reactively or proactively). It does not cover the
  disaster where a Cluster loses the **majority of its primaries at once** — an
  AZ/node-group outage, a bad drain, a correlated crash.

## Context

Valkey Cluster fails a primary over by **vote**: a replica asking to take its
dead primary's slots needs acknowledgement from a majority of the *masters*.
That is exactly what a majority-primary loss removes. With `N` primaries and more
than `floor(N/2)` of them down, the survivors are not a quorum, so **no replica
can be voted in**. The cluster sits in `cluster_state:fail` with unserved slots
**indefinitely** — the surviving replicas of the dead primaries are healthy and
current, but gossip will never promote them. This is a genuine SPOF for the
Cluster topology (the AR1 gap): the data is there, but nothing brings it back.

The one Valkey primitive that breaks the deadlock is `CLUSTER FAILOVER TAKEOVER`:
a replica promotes itself **unilaterally**, bumping its `configEpoch` above every
peer and claiming its primary's slots **without a vote**. It is powerful and
correspondingly dangerous — run it while the old primary is still serving and you
get two owners for the same slots (split-brain). Gossip therefore never issues it
on its own; a human (or an operator) must decide the old primary is really gone.

An operator is well placed to make that call, because it sees something gossip
cannot: **pod and node liveness through the k8s API**, plus a direct out-of-band
connection to every node.

## Decision

Add an operator-driven recovery path (`maybeRecoverClusterQuorum`, ahead of the
allReady-gated scale/survey steps so it runs *during* the outage). It intervenes
only when **all** of the following hold:

1. **Enabled for this cluster** — Cache by default; Durable only with the opt-in
   annotation. Availability-first vs durability-first is a per-workload choice.
2. **Below a voting quorum** — from `CLUSTER NODES`, `healthyMasters*2 <=
   totalMasters`. With a quorum intact, gossip can (and should) heal on its own,
   or deliberately refuse under the Durable replica-validity factor; the operator
   must not race or overrule it. A minority of primaries down is left alone.
3. **Stuck for the debounce** — `cluster_state != ok` continuously for
   `quorumRecoveryDownAfter` (45s). Gossip promotes a recoverable failure within
   seconds; only an unrecoverable one stays stuck this long.
4. **Each dead primary is fenced from two independent perspectives:**
   - **k8s view** — the pod is missing, not Running, not Ready, or on a NotReady/
     absent node; AND
   - **data-path view** — the operator cannot reach it, or reaches it and it
     reports `cluster_state:fail` (it has detected its own minority isolation).
     A primary reachable **and** reporting `cluster_state:ok` is genuinely
     serving and is **never** taken over.

For each such shard the operator deletes the dead primary's pod (fencing it and
letting the StatefulSet re-create it) and issues `CLUSTER FAILOVER TAKEOVER` on
the shard's most up-to-date reachable replica. The re-created old primary rejoins
through its retained data PVC, sees the higher `configEpoch`, and demotes itself
to a replica — the cluster self-heals back to full redundancy.

### Why this is not split-brain (the honest argument)

Write safety does **not** rest on proving the old primary's process is dead — no
k8s operator can do that without STONITH, and on a control-plane partition a
pod on a NotReady node may still be running. It rests on a guarantee Valkey
Cluster already makes: **a primary partitioned from the majority of masters for
longer than `cluster-node-timeout` stops accepting writes** (it returns
`CLUSTERDOWN`, because a replica on the majority side may have been promoted).
The debounce (45s) is an order of magnitude above the operator's node-timeout
(5s), so by the time a takeover fires, any still-running-but-isolated old primary
has already stopped serving writes. The k8s fence and the data-path check are
defence in depth on top of that: they decide *when* to intervene and refuse to
overrule a primary that is demonstrably still serving.

Residual, accepted limits: a minority primary may still answer **stale reads**
until it rejoins (the Cache profile, the only one that recovers automatically,
accepts this AP trade-off); and a pathological *asymmetric* partition is outside
what any voteless takeover can fully rule out. Durable clusters opt in
deliberately, or stay stuck for a human to resolve.

## Consequences

- The Cluster topology recovers automatically from a lost primary majority
  (Cache), closing the AR1 SPOF, with a clear, auditable trail (structured logs
  + the `failover_total{reason="cluster-takeover"}` metric + the
  `status.quorumDownSince` debounce marker).
- Durable clusters keep their no-silent-loss contract by default; recovering
  them is a conscious opt-in.
- A shard that lost **every** pod (primary and all replicas) is *not* recovered
  by takeover — there is nothing to promote. That is a restore, out of scope
  here, and is logged as such.
- The feature is Cluster-only; Replication/Sentinel already fail over reactively
  (ADR 0004) and have no quorum to lose.

## Validation

Live on a dedicated k3d cluster (3 shards, 1 replica each, Cache profile,
Valkey 8.0):

1. Loaded 500 keys; cordoned all nodes; force-deleted 2 of 3 primary pods so they
   stayed Pending (fenced) while their replicas kept running — a true majority
   loss (`cluster_state:fail`, `healthy=1/3`).
2. The operator armed the debounce (`quorum lost and recoverable; starting
   recovery debounce`), waited 45s, then issued `CLUSTER FAILOVER TAKEOVER` on
   both surviving replicas.
3. Result: `cluster_state:ok`, all 16384 slots served, `cluster_size:3`, **500/500
   keys intact**. The takeover replicas carried `configEpoch` bumped above the
   fenced primaries.
4. Uncordoned: the two fenced primaries rescheduled (retained PVC) and rejoined
   as **replicas** of the new primaries — full redundancy restored, no operator
   action needed. `status.quorumDownSince` cleared on `cluster_state:ok`.
