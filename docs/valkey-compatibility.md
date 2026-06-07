# Valkey Version Compatibility

Which Valkey versions the operator supports, the default, and what to check when a
new Valkey major is released. The version is the **image tag** (`spec.image`); the
operator does not bundle Valkey.

## Matrix

| Operator | Valkey 7.x | Valkey 8.x | Valkey 9.x | Default |
|---|---|---|---|---|
| 0.1.x | ✅ supported | ✅ supported (baseline) | ✅ opt-in (probed on k3d) | `valkey/valkey:8.0` |

- **8.x** — baseline default: stable, all topologies validated.
- **9.x** — opt-in (`spec.image: valkey/valkey:9.x`).
  Probed on k3d. On **9.1+** the operator **uses Atomic Slot Migration**
  for all rebalance/reshard operations (version-gated `asmDetectSnippet`,
  `--cluster-use-atomic-slot-migration`; on <9.1 — classic key-by-key reshard;
  see [adr/0001](adr/0001-atomic-slot-migration.md)). The operator also renders
  Cluster resilience directives. Version-agnostic (all versions):
  `cluster-allow-replica-migration no`, and — for the **Cache** profile only —
  `cluster-replica-validity-factor 0` (availability-first: a stale replica can
  still win an election, avoiding a stuck shard; **Durable** keeps Valkey's
  default gate so it never promotes a replica that would silently lose acked
  writes). Version-gated: `shutdown-on-sigterm failover` (≥9.0) and
  `tls-auto-reload-interval` (TLS+≥9.1).
- **7.x** works (the cluster-specific features of 8/9 are not required), but is legacy.

## What to check when bumping the Valkey major

1. **Cluster bootstrap/scale/reshard** — the flags and behavior of
   `--cluster create/add-node/rebalance` (ASM vs classic on 9.x).
2. **Config directives** — renamed/removed keys in the `valkey.conf` render
   (`renderValkeyConf`), repl-backlog, cluster-node-timeout.
3. **ACL syntax** — `ACL SETUSER` tokens, the Sentinel ACL command set.
4. **INFO / CLUSTER NODES output** — the parsers in `failover.go` / `cluster.go`.
5. **Image availability** in the registry/mirror in use.
6. Run the **advanced e2e** suite (shd/scl/fal/bkp) against the new tag.
7. Update this matrix and the `CHANGELOG`.

## Process

Valkey ships a major roughly every 6–12 months; budget about 2–4 weeks for
adaptation plus testing per major. Track upstream release notes; gate the default
version bump (8.x → 9.x) on a staging soak (ROADMAP P4).
