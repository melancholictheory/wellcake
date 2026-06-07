# ADR 0001 — Atomic Slot Migration (ASM) for cluster resharding

- Status: **Accepted — IMPLEMENTED** (operator; verified on a local k3d cluster
  with Valkey 9.1 — per-shard scale-down 4→3, data intact). The implementation
  diverged from the original design sketch below — see the inline notes.
- Date: 2026-06-04 (originally 2026-05-30)
- Context: roadmap item C3. Unblocks reliable resharding of a live primary
  during shard scale-down (the C4 finding, see
  [community-challenges-scorecard](../community-challenges-scorecard.md) and
  ADR 0005).

## Context

Resharding in the operator (scale-up `add-node` + rebalance, scale-down
reshard-away + `del-node`, manual trigger `valkey.wellcake.io/reshard`) is
performed via `valkey-cli --cluster rebalance` / `--cluster reshard`. This is the
**classic** mechanism: key-by-key `MIGRATE` (`DUMP`/`RESTORE`) with
`CLUSTER SETSLOT MIGRATING/IMPORTING` and `ASK`/`MOVED`/`TRYAGAIN` redirects.

The classic path is **interruptible**: an interruption (OOM, pod restart, Job
timeout) leaves a slot in a half-migrated state (open slot, `importing` on one
side) — exactly what the **C3/C4** finding surfaced: resharding a live primary
during scale-down fails on a cluster poisoned by retries, and `--cluster fix`
does not always repair it. This is the root cause behind the fragility of shard
scale-down.

## ASM mechanism (verified on Valkey 9.1.0)

> **Key clarification vs the earlier sketch of this ADR:** the operator does
> **NOT** need to manually drive the `CLUSTER SYNCSLOTS ESTABLISH/ACK/...`
> handshake (these are **internal** node-to-node commands). There is a **single
> high-level command** that drives the entire state machine itself.

- **`CLUSTER MIGRATESLOTS SLOTSRANGE <start> <end> [<start> <end> ...] NODE <target-id> [...]`**
  — issued against the **source primary** (the current owner of the slots). The
  engine itself runs snapshot → stream → pause → atomic cutover. `+OK` = job
  accepted (all-or-nothing: on error, none of them starts). Since Valkey 9.0.0.
- **`CLUSTER GETSLOTMIGRATIONS`** — progress polling. Fields:
  `name, operation (EXPORT/IMPORT), slot_ranges, target_node, source_node,
  create_time, last_update_time, last_ack_time, state, message, cow_size,
  remaining_repl_size`. Terminal `state` values: `success` / `failed` /
  `cancelled`; anything else means the migration is active.
- **`CLUSTER CANCELSLOTMIGRATIONS [ALL]`** — cancellation (on the source). When
  none is active — `ERR No migrations ongoing`.

**Atomicity:** slot ownership does not move until the very end — during
snapshot+stream the slots belong entirely to the source, and clients hitting the
target receive `MOVED` (not ASK). On any failure/interruption the slots **remain
entirely with the source** → safe retry. A half-migrated slot cannot exist — and
that is precisely the C3 fix.

### What was verified on a local k3d cluster (3-shard, Valkey 9.1.0, unmodified operator)

- `CLUSTER MIGRATESLOTS SLOTSRANGE 0 1000 NODE <B>` → `OK`; `GETSLOTMIGRATIONS`
  went `snapshotting → success`. Slots 0-1000 moved to B, 34 keys moved,
  **500/500 readable**, `cluster_state:ok`, 16384 slots. The GETSLOTMIGRATIONS
  fields matched the documentation exactly.
- **Full-shard drain** (all of a shard's slots → another primary): the shard is
  left with **0 slots** → eligible for removal. This is the direct scale-down
  scenario.
- **Scale-down gotcha:** an emptied primary **auto-demotes to a replica** (its
  dbsize grows because it replicates the dataset of the new owner of its former
  slots). On scale-down the operator must account for this: after draining a
  node, it must `CLUSTER FORGET` it + delete the STS (rather than waiting for it
  to "disappear" on its own).
- On 9.1, `valkey-cli --cluster rebalance/reshard` **with the
  `--cluster-use-atomic-slot-migration` flag** drives ASM itself (the CLI
  internally performs the slot-math via `CLUSTER MIGRATESLOTS`) — so the operator
  does **NOT** need to call `MIGRATESLOTS` directly. Without the flag, rebalance
  stays key-by-key. This is exactly the flag added by the implemented
  `asmDetectSnippet` on 9.1+ (which is how it was built).

## Decision

The ASM resharding path is **implemented** in the operator, **version-gated**
(≥9.1), with the classic path as a fallback for 7.x/8.x/9.0. The gate is
`asmDetectSnippet` (`cluster.go`): version detection via
`INFO server valkey_version` at runtime, with the
`--cluster-use-atomic-slot-migration` flag enabled only on Valkey ≥ 9.1 (on 9.1
the `valkey-cli` performs the atomic slot-math itself); otherwise the classic
key-by-key path is used unchanged.

## Integration design

> **The implementation diverged from this sketch.** A manual helper
> `asmMigrateSlots` issuing direct `CLUSTER MIGRATESLOTS` + polling
> `GETSLOTMIGRATIONS` was **NOT** built. Instead — `asmDetectSnippet`
> (`cluster.go`): a shell prelude in the Job that determines the version via
> `INFO server` and, on ≥9.1, sets `$ASM_FLAG=" --cluster-use-atomic-slot-migration"`,
> appended to every `valkey-cli --cluster rebalance/reshard` invocation. The
> slot-math, snapshot/stream/atomic-cutover and progress polling are all done by
> `valkey-cli` itself on 9.1. The sketch below is preserved as the original
> intent (NOT implemented).

### Orchestration helper (sketch, NOT implemented)
`asmMigrateSlots(ctx, sourceHost, targetNodeID, ranges)`:
1. `CLUSTER MIGRATESLOTS SLOTSRANGE <ranges> NODE <targetNodeID>` against the source.
2. Poll `CLUSTER GETSLOTMIGRATIONS` (interval ~1–2 s; upstream CLI uses 100 ms)
   by `name`/`slot_ranges` until `state` is terminal.
3. `success` → done; `failed`/`cancelled` → return an error with `message` (slots
   remain entirely with the source → reconcile retries idempotently); active →
   wait, using `remaining_repl_size` as a backlog indicator.

Issued via the same in-pod `valkey-cli` (like the current Jobs) **or** directly
by dialing from the operator (`dialReplClient`, as with survey/failover/rotation)
— to be chosen at implementation time; the dialing variant removes the Job and
gives the operator direct control over polling/cancellation.

### Version gate (implemented as `asmDetectSnippet`, `cluster.go`)
The gate is Valkey **≥ 9.1** (maj>9 OR maj==9 && min>=1), not merely major ≥9:
the `--cluster-use-atomic-slot-migration` flag only appears in `valkey-cli` in
9.1. The version source is `INFO server valkey_version` **at runtime inside the
Job** (sed on `valkey_version:`), which is robust to the form of the image tag.
< 9.1 → classic path unchanged.

### Replacement points (classic → ASM at ≥9.1; implemented via `$ASM_FLAG`)
- `buildScaleUpJob` / `buildShardScaleUpScript` (`cluster.go`,
  `cluster_pershard.go`): after `add-node` — instead of `--cluster rebalance
  --cluster-use-empty-masters`, distribute a share of slots to the new (empty)
  primaries via a series of `CLUSTER MIGRATESLOTS` from the existing ones.
- `buildScaleDownJob` / `buildShardScaleDownScript`: instead of `--cluster reshard
  --cluster-slots 16384` + `--cluster fix` — `CLUSTER MIGRATESLOTS` all of the
  leaving primary's slots onto the remaining ones, then (accounting for the
  auto-demote to replica) `FORGET` + remove the node/STS. **Removes the C3/C4
  fragility** (no open slot on interruption).
- `buildReshardJob` (manual `valkey.wellcake.io/reshard`): ASM rebalance.

### Failure handling
- An interrupted ASM migration → slots stay with the source, `state=failed` →
  reconcile restarts (idempotently: before starting, read `GETSLOTMIGRATIONS`,
  don't duplicate an active one).
- `CLUSTER CANCELSLOTMIGRATIONS` on reconcile/teardown abort.
- Never leave a slot half-done — ASM guarantees this at the engine level;
  preserve the property (don't mix with classic `SETSLOT` on the same slots).

### Validation (done)
- Unit: `$ASM_FLAG` rendering for scale-up/down/manual + version-gate branching —
  covered.
- k3d (via the operator, 9.1): scale-up echoes "via atomic slot migration";
  per-shard scale-down 4→3 — 16384 slots intact, no data lost. ✅
- e2e at the higher-level platform layer — follow-up (asserting the ASM fact on 9.x).

## Consequences

- Reliable shard reshard/scale-down on Valkey 9.x — closes the root of C3/C4 (the
  ADR 0005 per-shard scale-down becomes genuinely safe).
- Version-gated branching in the data path: extra complexity, but isolated (the
  classic path is untouched for <9).
- Requires a 9.x fleet to take effect; the operator's default image is
  `valkey:8.0`, so the ASM path is enabled only with an explicit
  `image: valkey:9.x`.
- Module caveat: ASM requires module opt-in
  (`VALKEYMODULE_OPTIONS_HANDLE_ATOMIC_SLOT_MIGRATION`); **plain Valkey without
  modules works** (verified). It constrains future use of modules that persist
  data via aux-save.

## Sources

- valkey.io/commands/cluster-migrateslots, /cluster-getslotmigrations,
  /cluster-cancelslotmigrations, /cluster-syncslots; valkey.io/topics/atomic-slot-migration
- valkey-io/valkey PR #1949 (state-machine, FAILOVER-GRANTED), `src/valkey-cli.c`
  branches 9.0 (no MIGRATESLOTS) / 9.1 (`--cluster-use-atomic-slot-migration`)
- Empirical: local k3d cluster, ValkeyCluster 3-shard `valkey:9.1.0`, 2026-06-04.
