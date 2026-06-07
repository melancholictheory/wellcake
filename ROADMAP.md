# Roadmap

Open questions and directions for development. Grouped by type: what blocks
production, what strengthens code/ops health, what belongs to organizational
agreements, and what is groundwork for the future.

Sources: architectural review against spotahome/redis-operator,
OT-CONTAINER-KIT/redis-operator, valkey-io/valkey-operator,
bitnami/valkey-operator, kubeblocks; plus requirements derived from common
production Redis/Valkey workloads.

## Production gates (needed before prod rollout)

| # | What | Owner | Status |
|---|---|---|---|
| P1 | Agree SLO/SLA/RTO/RPO with consuming teams and the platform team | Platform | open |
| P2 | Agree security requirements: mTLS mandatory by default? ACLs mandatory? audit trail? at-rest encryption? | Security | open |
| P3 | Identify pilot consumers and the migration order | Platform | open |
| P4 | Decide the target Valkey version: 8.x baseline, 9.x for Cluster | Platform | groundwork ready (composition XRD `version` 7/8/**9**, default 8; 9.x verified on a local kind/k3d cluster) — decision pending |
| P5 | Deploy to a staging cluster and run a 2-week soak | Platform | open |

> **Critical path — non-engineering.** P1–P5 are agreements (SLO, security,
> pilots, version) and a soak, not code. Their `open` status means that what's
> missing before prod is primarily **decisions** rather than implementation. The
> engineering T/C/S/O tasks don't block this chain — but they don't unblock it
> either. P1–P3 should move in parallel with the architectural risks below.

## Architectural risks (priority gaps)

Extracted from a critical review of the codebase. These are not "future
features" but properties of the current architecture that must be closed or
consciously accepted **before durable production**.

| # | Risk | Severity | Substance and direction |
|---|---|---|---|
| AR1 | Operator is the sole failover arbiter for Replication | high (warning ✅) | `surveyReplication`/promotion runs via requeue every 15s and **only while the operator is alive**. RTO = up to 15s + reconcile; with the operator down there is no failover. For durable workloads this is weaker than data-plane arbitration. **Done:** admission warning on durable+Replication (a soft nudge toward Sentinel/Cluster). **Investigated (cha-04 → diagnostic run, operator log dump): no gap.** The operator **detects a hung primary as unreachable and promotes a replica** (log `promoting replica`). The initial "no failover" was an artifact: the liveness probe **restarts** the hung container within ~8s (DEBUG SLEEP → liveness-fail → restart), so the "hang" turns into a "restart of an empty pod" (handled by the empty-master guard), and with equal/zero data the operator re-adopts the original ordinal. Side note: the diagnostic exposed an **STS conflict churn** (`ensureStatefulSet` failed on "object has been modified" → the whole reconcile errored before the failover step) → **fixed** (RetryOnConflict, see CHANGELOG) — removes promotion delay under racing reconciles. Optional remainder: a hard gate / default durable→Sentinel. Related to EC1. |
| AR2 | Bootstrap idempotency tied to a status flag | **done** | Before `--cluster create` the operator checks the actual `CLUSTER INFO` (pod-0): an already-formed cluster is adopted (`AdoptedExisting`) instead of being re-initialized. Closes the footgun of a lost status flag. |
| AR3 | Backup/restore asymmetry | high (mostly closed) | Backup is mature (per-shard fanout, retention, SSE, ~~verify~~ ✅ B1). Restore: single-shard ✅, Cluster — ~~per-shard restore-init + bootstrap-safety + adopt + slot manifest in the backup~~ ✅ (C2 incrementally), the remaining gap is **automatic cluster assembly** (ADDSLOTSRANGE/MEET/REPLICATE from the manifest) — currently manual via runbook. |
| AR4 | Defaulting in reconcile + no conversion webhook (✅ both addressed) | low | ~~`applyDefaults` in reconcile~~ ✅ E2 (mutating webhook). ~~No API versioning~~ ✅ E1 groundwork — v1beta1 Hub/storage + v1alpha1 spoke + conversion + multiversion CRD; the safe-evolution window is open (remaining: activate `conversion: Webhook` when schemas diverge). |
| AR5 | Secret rotation requires a manual restart | **done** | The config hash deliberately excludes secrets → a password/TLS change did not roll the pods. ~~Password~~ ✅ S3: in-place rotation without restart via the `valkey.wellcake.io/rotate-password` annotation (live ACL SETUSER on the default user + masterauth, no replication blip). ~~TLS~~ ✅: automatic cert reload on cert-manager renewal (watch on the TLS Secret → live `CONFIG SET tls-cert-file/key/ca`, verified through the TLS handshake itself to bypass the kubelet sync delay, no restart). Verified on a local kind/k3d cluster (Replication+TLS). ~~Remaining: operator dial-ins under mTLS~~ ✅: `dialReplClient` now presents a client cert (`loadMTLSClientCert`) → failover/survey/password rotation work under `tls.mutualTLS` (verified locally: survey→primary, failover kv-0→kv-1). |

## Design review #2

A second external review (Q1–Q6: topologies and the operator's role,
failover SPOF, scalability to 500+ installations, docs/framework, SOLID/single
CRD, immutability). Verdicts were broadly positive — immutability enforcement
was called "the best among the evaluated operators", docs/runbook "above
average". Concrete actionable items:

| # | From | What | Severity | Direction |
|---|---|---|---|---|
| SC1 | Q3 | **Pod watch became cluster-wide** → **done**. The failover fix raised an informer on `corev1.Pod` across all watch namespaces (caching every pod). **Done:** the Pod cache was narrowed to the label selector `app.kubernetes.io/managed-by=valkey-operator` (`cache.Options{ByObject:...}` in `cmd/main.go`) — we cache only our own pods, fast failover detection is preserved, and the event storm at 500+ is eliminated. | **done** | — |
| SC2 | Q3 | `MaxConcurrentReconciles` = default **1** → reconcile serialized, a bottleneck at 500+ CRs. → **done**. **Done:** a `--max-concurrent-reconciles` flag (default 1) + Helm value `maxConcurrentReconciles`, wired into `WithOptions`. Raise (≈5) for 500+. | **done** | — |
| SC3 | Q3 | No ops guide for horizontal scaling to 500+. → **done** (runbook Op4: maxConcurrentReconciles, namespace sharding, sizing, Sentinel, scoped Pod cache). | **done** | — |
| SC4 | Q6 | `spec.storage.mode` is **mutable** — changing the persistence mode on the fly (`rdb→none` / `none→aof`) is dangerous. → **done**. **Done:** CEL XValidation `self == oldSelf` on `Mode` (API-server enforcement, like `topology`/`profile`/`storageClassName`) + an envtest case. | **done** | — |
| SC5 | Q5 | In a single CRD, fields outside their topology are confusing. → **done**: CEL rejects `replicasPerShard` outside Cluster (like `shards`); the `replicas` doc was clarified (ignored in Cluster) + envtest. | **done** | — |

Strengths confirmed by the review (no action required): the failover SPOF for
Replication (AR1) is already mitigated by the admission warning + the Sentinel
recommendation (Q2 — Sentinel failover in the data plane, independent of the
operator's liveness); the single CRD is a deliberate pragmatic trade-off (Q5);
immutability is best-in-class (Q6).

## Tests & quality

| # | What | Priority |
|---|---|---|
| T1 | envtest reconciler suite (`make envtest`) — real cases on top of the API server: defaulting, CEL, full reconcile, ValkeyACL | **done** |
| T2 | e2e in kind: bootstrap → scale-up → failover → backup → restore | **done (except restore)** — the kuttl advanced line: `fal-01` (failover), `shd-01` (bootstrap), `scl-01` (scale-up + autoReshard), `bkp-01` (backup→S3/MinIO). Restore — see C2 |
| T3 | Coverage of cluster/sentinel/failover/backup/valkeyacl with tests | **mostly done** — sentinel builders, bootstrap job, networkpolicy, config profiles, ACL fanout (unit), finalizer (envtest), go-redis wrappers (RESP mock: info/replicaOf/applyACLUser). Remaining: reconcileFailover/surveyReplication orchestration (needs live Valkey — covered by e2e `fal-01`) |
| T4 | Webhook tests | **done** — table-driven fake-client tests for both validators (referenced Secret, conditional topology, ACL duplicates/default/secret) |
| T5 | CI lint gate: blocking gofmt + golangci-lint (stage `test`); e2e harness — yamllint + shellcheck | **done** |
| T6 | Lint ratchet: clear the parked linters and enable them — all enabled and blocking. ~~unparam~~ ✅ ~~gocyclo~~ ✅ ~~lll~~ ✅ ~~modernize~~ ✅ ~~goconst~~ ✅ | **done** |
| T7 | envtest **in CI** | **done** — linux/amd64 kubebuilder-tools published in the project's generic package registry; the CI `envtest` job pulls them via the registry API and runs the full `TestControllers\|TestAPIs`. Republish: `make envtest-assets-upload`. (An image-based variant was reverted — pushing to a registry required extra privileges, the package path is self-contained.) |

## Cluster-topology depth

| # | What | Priority |
|---|---|---|
| C1 | Split into per-shard workload (STS-per-shard) — **IMPLEMENTED** (`spec.perShardWorkload`, opt-in, immutable, Cluster-only; ADR 0005 / O1). Provides per-shard rollout/image/resources override **+ shard-aware placement (#5) + correct shard scale-down (C4)** — both **verified live on a local kind/k3d cluster**: REQUIRED shard-scoped anti-affinity (primary+replica on different nodes) and clean scale-down (reshard→del-node→delete STS, 0 loss). Single-STS remains the default (per-shard is opt-in). | **done** (verified) |
| C2 | Cluster restore from a backup set | **done** — per-shard restore-init (`{shard}` placeholder), bootstrap-safety (don't run `--cluster create` on non-empty nodes), slot manifest in the backup, **automatic cluster reassembly** by an assembly Job from the manifest (gap-fills missing slots — RDB claims only data slots; epoch from RDB; MEET by getent IP; REPLICATE from the reply text). Verified on a live cluster: 3+3 HA, 16384 slots, data intact, adopt. Remaining: run e2e `rst-01` in CI when the runner frees up (validated locally) |
| C3 | Atomic Slot Migration (9.1+) instead of key-by-key reshard — **IMPLEMENTED** (ADR 0001 accepted + code). `asmDetectSnippet` version-gates `$ASM_FLAG=--cluster-use-atomic-slot-migration` across all rebalance/reshard (`cluster.go` + `cluster_pershard.go`); on 9.1, valkey-cli does the slot math atomically, with no open slot on interruption (fixes the root of C3/C4). An emptied primary is auto-demoted → scale-down FORGET+remove. Verified on a local kind/k3d cluster (used in per-shard scale-down 4→3, data intact). | **done** |
| C4 | **Cluster shard scale-down — CLOSED via per-shard workload (verified on a local kind/k3d cluster).** The original bug (single-STS): `spec.shards` down → the highest ordinals are deleted = **replicas** (bootstrap is masters-first), not a shard → the previous number of masters, a false `Ready`. **single-STS:** CEL rejects shrinking `shards` (honest guard). **per-shard (`perShardWorkload: true`, C1/ADR 0005):** shrinking `shards` reshards the leaving master's slots onto a remaining one (ASM on 9.1), del-nodes the replica+master (FATAL guard on slot ownership), and deletes the shard's STS+svc (`deleteLeavingShards`). A 4→3 run: `cluster_size` 4→3, leaving STS+svc deleted, `cluster_known_nodes` 6 (no ghost nodes), 16384 slots intact, `cluster_state:ok`, **3000 keys intact (0 loss)**, true `Ready`. | guard ✅ (single-STS); deep fix ✅ (per-shard, verified) |

## Backups / Restore

| # | What | Priority |
|---|---|---|
| B1 | Verify dump.rdb via a `valkey-check-rdb` step between init and upload | **done** — the dump-init container runs `valkey-check-rdb` after each `--rdb` (single-shard + per-shard Cluster); a corrupt RDB fails the job before upload via `set -eu` |

## Security / hardening

| # | What | Priority |
|---|---|---|
| S1 | ACL for Sentinel | **mostly done** — Sentinel talks to the master through a dedicated `sentinel-user` (no key access) via `sentinel auth-user`, not as default+requirepass. Remaining: narrow to a minimal per-command set (needs e2e failover validation on a cluster); ACL for the client-to-sentinel/inter-sentinel port — optional |
| S2 | Pod Security Standards labels on the namespace (`pod-security.kubernetes.io/enforce: restricted`) — add to the Helm chart | low |
| S3 | Password rotation without restart | **done** — operator-initiated in-place rotation via the `valkey.wellcake.io/rotate-password=<token>` annotation (operator-managed Secret). The operator knows the old password from the current Secret → rolls the new one to live pods without restart: an additive pass (the default user accepts old+new, replicas switch masterauth → **no replication blip**), then cutover (drop the old, `ACL SAVE` to users.acl → restart-safe). Once-per-token via an uncached APIReader guard. Verified on a local kind/k3d cluster (3-node Replication: live rotation, 0 restarts, `sync_full` did not grow, restart-safe). ExistingSecret is out of scope (the operator doesn't know the old password). |
| S4 | Multi-region mTLS with an explicit CA mount | **done** — `replicateFrom.caSecret` (S4): the operator mounts the source cluster's CA itself and merges it with the local CA into a single trust bundle (`/data/ca-bundle.crt`), since Valkey reads one global `tls-ca-cert-file`. The bundle is built in `config-init`, and `tls-reload` points at it too (so renewing the local cert doesn't drop trust in the source CA). Previously the user mounted the source cluster's CA bundle by hand. CEL+webhook require `replicateFrom.tls=true`+`tls.enabled=true`. |

## Observability

| # | What | Priority |
|---|---|---|
| O1 | Tracing (OpenTelemetry) for the reconcile loop + replClient calls | low |
| O2 | Data-plane monitoring of Valkey instances | **done** — redis_exporter sidecar + per-cluster ServiceMonitor + a `valkey-dataplane.rules` group (InstanceDown/ReplicaLinkDown/ClusterStateNotOk/MemoryNearMaxmemory/HighFragmentation/RdbBgsaveFailing/FullResyncStorm); enabled by default in the platform composition (`monitoring.enabled`) |
| O3 | PrometheusRule with baseline alerts (unhealthy shards > 5m, reconcile error rate, bootstrap/scale failures) — in the Helm chart under `metrics.prometheusRule.enabled` | **done** |

## CRD evolution

| # | What | Priority |
|---|---|---|
| E1 | v1alpha1 → v1beta1 + conversion webhook | **groundwork done** — v1beta1 Hub/storage, v1alpha1 spoke + `ConvertTo`/`ConvertFrom`, controller/webhooks/plugin on v1beta1, multiversion CRD, round-trip test. Remaining: flip the CRD to `conversion: Webhook` + cert injection in the chart (once schemas diverge; strategy `None` is correct while they're identical) |
| E2 | Mutating defaulting webhook instead of reconcile-side `applyDefaults` | **done** — `Default()` in the api package, mutating webhook (`/mutate-…`) + `MutatingWebhookConfiguration` in the chart; reconcile calls the same `Default()` defensively |

## Platform integration

| # | What | Priority |
|---|---|---|
| I1 | Crossplane composition wrapper (XCluster) — for embedding into a GitOps platform | **done** — an XRD + composition (topology/mode/sizing/tls/auth/backup/monitoring/version), with kuttl e2e |
| I2 | DC-aware scheduling: confirm whether `topology.kubernetes.io/zone` is sufficient, or whether a custom `topology.wellcake.io/dc` is needed | low |
| I3 | kubectl plugin `kubectl valkey`: `status`, `cli`/`exec`, `backup`, `restart`, `reshard`, `failover`, `hibernate`, `certificate`, `report` — **done** | done |

## Operations / hardening

| # | What | Priority |
|---|---|---|
| Op1 | Operator HA testing: actually test handover between leader-elected replicas | medium |
| Op2 | Recovery after `Phase=Failed/BootstrapFailed` | **done** — procedure in [runbook#recovery-after-phasefailed--bootstrapfailed-op2](docs/runbook.md): diagnose → delete the failed Job → reconcile recreates it; PVCs survive the delete |
| Op3 | Operator upgrade procedure | **done** — [runbook#operator-upgrade-op3](docs/runbook.md): the data plane is untouched, apply the CRD separately (multiversion), leader-election/rollback |

## Edge cases / known issues

| # | What | Priority |
|---|---|---|
| EC1 | Split-brain in Replication between two network partitions: enforceReplication closes the window after a pod-0 restart but is not protected against a network split — needs Sentinel | warning ✅ (AR1) — durable+Replication gives an admission warning; a hard gate is optional |
| EC2 | Scale-down + multi-region combination is untested (should work, but edge cases are possible) | low |
| EC3 | Active-active / bidirectional replication between ValkeyClusters — impossible in OSS Valkey (only Redis Enterprise) | out of scope |

## Won't do

| # | What | Why |
|---|---|---|
| N1 | REST/gRPC API inside the operator | The Kubernetes API is already built in; if a self-service portal is needed, that's a separate `valkey-platform-api` microservice on top of the kube-API |
| N2 | Cross-cluster active-active in OSS | Not supported by Valkey; a task for Redis Enterprise |

## What I'd pick up next (by value)

Priority has shifted to the architectural risks (AR1–AR5) — they're closer to
durable production than the remaining quality/ops tasks.

1. ~~**AR3 / B1 verify dump.rdb**~~ ✅ — `valkey-check-rdb` after each `--rdb` in the dump container.
2. ~~**AR1 / EC1 gate in the webhook**~~ ✅ — admission warning on durable+Replication (a soft nudge toward Sentinel/Cluster).
3. ~~**AR3 / C2 Cluster restore**~~ ✅ — per-shard restore-init + bootstrap-safety + adopt + automatic cluster reassembly by an assembly Job from the slot manifest (see C2).
4. ~~**AR4 / E2 mutating defaulting webhook**~~ ✅ — `Default()` in admission + a defensive call in reconcile.
5. ~~**T7 envtest in CI**~~ ✅ (infra ready: assets + job).
6. ~~**S1 Sentinel ACL**~~ ✅ ~~**Op2/Op3 procedures**~~ ✅ — closed.

The priority roadmap backlog is exhausted. Also closed are the design-review
items: STS Parallel + PVC expansion (confirmed bugs), ADR 0002
(workload primitive), 0003 (build-vs-adopt), 0004 (proactive failover — design +
implementation: operator-driven rolling restart for Replication/Cluster/Sentinel,
opt-in via the `valkey.wellcake.io/proactive-rollout` annotation, default OFF),
0001 (C3/ASM — implemented), 0005 (C1 per-shard workload — implemented),
CODEOWNERS/CONTRIBUTING/prod-readiness/compat-matrix. Residual follow-ups (as
needed, all requiring live/e2e): a hard gate for AR1, activating
`conversion: Webhook` (E1), a minimal per-command Sentinel ACL (S1),
**chaos/integration tests** (plan — [docs/chaos-testing.md](docs/chaos-testing.md);
Layer 1 pod-kill scenarios → Layer 2 under Chaos Mesh) + bus factor + flipping
the proactive-rollout default after a soak. Low-priority S2/I2/EC2 — on request
(C1/C3/S4 — **done**; O1 tracing — low). **Production blockers P1–P5 are
non-engineering (platform/security decisions).**

From the second design review, added **SC1** (narrow the Pod informer to a label
selector — a regression from the failover fix, prioritized), **SC2**
(`MaxConcurrentReconciles` > 1), **SC4** (immutability on `spec.storage.mode`);
SC3 (ops guide for 500+) and SC5 (CEL cleanliness) — low priority.

All priority AR problems from the critical review are addressed: AR1 (warning),
AR2 (partially via restore safety), AR3 (B1 + C2 reassembly), AR4 (E2 mutating
webhook + E1 v1beta1 groundwork). Residual follow-ups: a hard gate for AR1
(optional), activating `conversion: Webhook` in the CRD/chart (E1, at the first
v1alpha1/v1beta1 schema divergence).

Done: T1 (envtest suite), T2 (kuttl advanced e2e: fal/shd/scl/bkp), T3 (mostly —
unit+RESP mock+envtest finalizer), T4 (webhook tests), T5 (CI lint gate), T6
(lint ratchet — all linters enabled: unparam/gocyclo/lll/modernize/goconst),
O2/O3 (monitoring + alerts), I1 (Crossplane composition), groundwork for P4
(Valkey 9 opt-in).

## Project history

See [CHANGELOG.md](CHANGELOG.md).
