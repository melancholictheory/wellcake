# Testing

This document describes what is covered by tests in `valkey-operator` and how,
how the test levels differ, and how to run them. A separate e2e harness keeps
the end-to-end line on kuttl — it is described at the end.

## Test levels

| Level | Where | What it checks | Cluster needed? |
|---|---|---|---|
| Unit (builders/pure) | `internal/controller/*_test.go`, `cmd/...` | Pure functions: config rendering, object builders, parsing, defaults | no |
| RESP mock | `internal/controller/respmock_test.go` | go-redis wrappers against an in-house mini RESP server | no |
| Reconcile (fake client) | `internal/controller/reconciler_test.go` | Reconcile passes over a fake client (no API server) | no |
| Webhook (fake client) | `internal/webhook/v1beta1/validators_test.go` | Admission webhook validator logic | no |
| Plugin | `cmd/kubectl-valkey/*_test.go` | `kubectl valkey` CLI commands (formatting, arguments, patches) | no |
| envtest | `internal/controller/*_test.go` (Ginkgo) | Real API server: defaulting, CEL, full reconcile, finalizer, TLS | yes (kube-apiserver+etcd) |
| e2e (kuttl) | `e2e harness: tests/e2e*` | End-to-end scenarios against live Valkey in kind/k3d | yes (full-featured) |

The unit, RESP, reconcile, webhook, and plugin levels run under `go test
-short` and form the `unit-test` CI gate. The envtest suites are skipped under
`-short` (the offline CI image has no kube-apiserver/etcd binaries) — see
ROADMAP T7.

## Unit and pure functions (`internal/controller`)

### `resources_test.go` — core builders and helpers

- `TestApplyDefaults` — spec defaults (topology/profile/image/replicas, durable → PVC+both).
- `TestTotalReplicas`, `TestStatefulSetReplicasHoldsDuringScaleDown` — replica-count computation; the STS holds at the old size until the scale-down job completes.
- `TestComputeMaxmemory` — `maxmemory` derived from `resources.limits`.
- `TestPersistenceMode` — selecting aof/both/none/rdb based on the profile and `spec.storage.mode`.
- `TestConfigHashFromDataIsDeterministicAndSensitive` — the config hash is deterministic and reacts to changes (rolling restart).
- `TestRenderValkeyConfHasProfileDefaults`, `TestRenderValkeyConfMutualTLS`, `TestRenderValkeyConfClusterDirectives`, `TestRenderValkeyConfReplBacklog` — rendering `valkey.conf`: profiles, mTLS, cluster directives, auto-sized repl-backlog.
- `TestRenderInitScriptSeedsDefaultUserACL`, `TestRenderInitScriptNoPasswordLeavesACLEmpty` — the init script seeds the default user's ACL only when a password is present.
- `TestInternalEndpoint` — host:port of the client Service in the status.
- `TestBuildExporterTLS`, `TestBuildExporterAuthSecret` — the redis_exporter sidecar: TLS skip-verify and `auth.existingSecret`.
- `TestBuildHeadlessServiceHasGossipPortOnlyForCluster` — gossip port only for Cluster.
- `TestBuildStatefulSetInitContainerCount`, `TestBuildStatefulSetPropagatesRestartToken` — init containers and propagation of the restart token into the pod template.
- `TestParseClusterNodes`, `TestSlotsCountHandlesRangesAndSingles`, `TestSummarizeShards` — parsing `CLUSTER NODES`, counting slots, summarizing shards.
- `TestBuildReshardJobRebalances`, `TestBuildScaleUpJobRebalanceGatedByAutoReshard`, `TestBuildScaleDownJobRebalanceGatedByAutoReshard` — reshard/scale jobs; rebalance is gated by `spec.autoReshard`.
- `TestPickFailoverTarget` — selecting a replica candidate for promotion.
- `TestSetConditionReadinessContract` — the contract for updating conditions.
- `TestBuildPDBHonoursSpec` — PodDisruptionBudget built from spec.

### `builders_coverage_test.go` — builder application scenarios

- `TestRenderValkeyConfProfiles` — the cache/durable matrix (eviction, persistence, maxmemory, override via `spec.config`).
- `TestRenderSentinelConf`, `TestBuildSentinelStatefulSet` — Sentinel: quorum/auth (incl. `auth-user` dedicated ACL user)/TLS, StatefulSet.
- `TestRenderInitScriptSeedsSentinelACLUser` — the Sentinel topology seeds a `sentinel-user` with no key access (S1); other topologies do not.
- `TestBuildBootstrapJob` — the bootstrap job: node list, `--cluster-replicas`, preflight, auth/TLS.
- `TestACLTargets` — ACL fan-out targets (the primary for Replication, all nodes for Cluster).
- `TestBuildNetworkPolicy` — ports (valkey/exporter/TLS) and peers.
- `TestBuildRestoreInitContainerPerShard` — restore-init: Cluster → per-shard (`{shard}` substituted by ordinal, masters only); single-shard → key verbatim.

### `rollout_test.go` / `rollout_sentinel_test.go` — proactive rolling restart (ADR 0004)

- `TestNextRolloutStep` — the pure Replication state machine: replicas roll first (in ascending ordinal order), the primary only via promotion and only when it is the sole stale node; a not-ready pod parks the rollout.
- `TestNextClusterRolloutStep`, `TestNextClusterRolloutStepNoReplicaShard` — Cluster: a shard's replicas first → `CLUSTER FAILOVER` on a freshly rolled replica → a shard without replicas rolls the master directly.
- `TestAnyStale`, `TestSentinelDataMaster`, `TestSentinelNames`, `TestSentinelDataRolloutSequence`, `TestSentinelPlaneRolloutNoPrimary` — Sentinel: the data plane (replicas → `SENTINEL FAILOVER` of the master), the Sentinel plane (`primary=""` → roll-replica only), STS/FQDN names.
- `TestProactiveRolloutEnabled`, `TestUpdateStrategyFor`, `TestOrdinalFromPodName` — the opt-in annotation, OnDelete vs RollingUpdate, parsing the ordinal from a pod name.

### `cluster_pershard_test.go` — per-shard workload (ADR 0005)

- `TestPerShardEnabled`, `TestBuildShardStatefulSet` — the opt-in/Cluster-only gate; a per-shard STS with a shard label and REQUIRED shard-scoped anti-affinity.
- `TestClusterDataPods`, `TestBuildShardCreateCmds`, `TestBuildShardHeadlessService` — a single source of pod enumeration (single-STS vs per-shard), masters-only bootstrap commands, a per-shard headless Service.

### `respmock_test.go` — go-redis wrappers

An in-house minimal RESP server (miniredis does not implement
`REPLICAOF`/`ACL SETUSER`/`INFO replication`).

- `TestReplClientInfoParsing` — parsing `INFO replication`.
- `TestReplClientReplicaOf` — `replicaOf`/`replicaOfNoOne` (promotion).
- `TestApplyACLUserCommand` — constructing `ACL SETUSER … reset … >pw|nopass`.
- `TestReplClientClusterNodes` — parsing `CLUSTER NODES`.

## Reconcile over a fake client (`reconciler_test.go`)

Running Reconcile without an API server — fast checks of transition logic:

- `TestReconcileReplicationCreatesOwnedObjects` — owned objects are created with an owner-ref.
- `TestReconcileFinalizerLifecycle` — the finalizer is added on the first pass.
- `TestReconcileClusterRequiresShards`, `TestReconcileSentinelRequiresSentinelSpec` — InvalidSpec phases.
- `TestReconcileBackupCreatesCronJob`, `TestReconcileDisablingBackupDeletesCronJob` — the backup CronJob driven by `spec.backup.enabled`.
- `TestReconcileClusterScaleUpHoldThenAdvance` — scale-up: hold until Ready, then add-node.
- `TestReconcileHibernateScalesToZero` — the hibernate annotation → 0 replicas.
- `TestReconcileClusterReshardOnAnnotation` — manual reshard driven by a token annotation.

## envtest — real API server (Ginkgo)

The `TestControllers` (`suite_test.go`) and `TestAPIs` (webhook) suites bring up
kube-apiserver+etcd via `envtest`.

### `valkeycluster_controller_test.go`

- **CRD defaulting** — the API server fills in topology/profile/image/replicas on a bare spec.
- **CEL conditional-field** — rejected: Cluster without shards; shards on a non-Cluster; Sentinel without a sentinel block; `replicateFrom` on a Cluster.
- **CEL immutability** — topology cannot be changed; storage size cannot be shrunk.
- **Reconcile** — owned objects with an owner-ref + status; removal of the finalizer on deletion; wiring TLS through the config, STS volume/mount, and Services.
- **Finalizer lifecycle** — the finalizer is set on create and removed on delete (GC).

### `valkeyacl_controller_test.go`

- `ClusterMissing` when the target ValkeyCluster is absent.
- Requeue with `ClusterNotReady` while the target has no primary.

## Webhook validators (`internal/webhook/v1beta1/validators_test.go`)

Table-driven tests of both validators on a fake client (no envtest):

- `TestValkeyClusterValidator` — referenced Secrets (auth/tls/backup/restore/replicateFrom), conditional topology (Cluster→shards, Sentinel→block), durable+Replication warning, cluster-restore `{shard}` guard.
- `TestValkeyClusterDefaulter` — the mutating defaulter: defaults for topology/profile/image/replicas, durable→storage, preserving explicit values.
- `TestValkeyACLValidator` — duplicate users, the reserved `default`, missing passwordSecret → error; an absent cluster → warning (not error).

## API conversion v1alpha1↔v1beta1 (`api/v1alpha1/conversion_test.go`)

- `TestValkeyClusterConversionRoundTrip`, `TestValkeyACLConversionRoundTrip` —
  v1alpha1 → v1beta1 (hub) → v1alpha1 lossless over Spec/Status/ObjectMeta
  (the versions are structurally identical; the round-trip is insurance against
  future schema divergence).

## kubectl-valkey plugin (`cmd/kubectl-valkey`)

- `commands_test.go` — `TestBuildCliExecArgs*` (exec arguments with auth/TLS/topology), `TestTargetPod`, `TestJobFromCronJob` (one-off backup from a CronJob).
- `status_test.go` — `TestFormatStatus*` (Cluster/Replication, empty values → "-").
- `ops_test.go` — `TestPatchClusterAnnotations*` (setting operator annotations), `TestTopologyGuards`.
- `diag_test.go` — `TestRunCertificate*` (exporting TLS material / stdout / no-TLS), `TestRunReportWritesFiles`.
- `cmd/main_test.go` — `TestWatchNamespaces` (parsing the namespace list).

## How to run

```sh
# Unit + reconcile + webhook + plugin (fast, no cluster):
go test -short ./...

# Everything, including envtest (needs kube-apiserver/etcd):
make envtest               # once: download the assets into bin/k8s/...
export KUBEBUILDER_ASSETS="$(pwd)/bin/k8s/<version>-<os>-<arch>"   # absolute path!
go test ./internal/controller/ -run TestControllers -count=1

# Linters (the whole set is blocking):
golangci-lint cache clean && golangci-lint run    # cache clean — see feedback
```

> `KUBEBUILDER_ASSETS` must be an **absolute** path: `go test` runs from the
> package directory, and controller-runtime launches etcd from this path — with
> a relative one you get `fork/exec .../etcd: no such file or directory`.

### envtest in CI (T7)

Offline CI cannot pull the binaries via setup-envtest (storage.googleapis.com is
blocked by the proxy). Therefore the linux/amd64 kubebuilder-tools
(kube-apiserver/etcd/kubectl) are **published once into the project's internal
package registry**, and the `envtest` CI job pulls them from there (not via the
external proxy) and runs the full `TestControllers|TestAPIs` suites — this gates
the reconcile/CEL/finalizer/defaulting/restore-safety/conversion cases.

(Re)publishing the assets is done on a machine **with network access to the
registry**:

```sh
make envtest-assets-upload        # pulls the assets, packs them, PUTs into the package registry
# path: envtest-assets/<ENVTEST_PKG_VERSION>/envtest-<ver>-linux-amd64.tar.gz
```

The package version (`ENVTEST_PKG_VERSION` in the Makefile) must match the
version the CI configuration expects. When bumping the k8s version, re-upload
and update both.

## End-to-end (kuttl)

The end-to-end scenarios live in a separate e2e harness and run live Valkey in
kind/k3d via kuttl. The baseline line (`tests/e2e`):

| Case | What it checks |
|---|---|
| `crt-01-no-parameters` | Creation with defaults, endpoint readiness |
| `dat-01-write-read-lifecycle` | Writing/reading data through the endpoint |
| `ops-01-lifecycle` | Parameter update → rolling |
| `sec-01-tls-auth` | TLS + auth (mTLS, requirepass) |
| `val-01-reject-invalid` | Rejection of an invalid XR |
| `del-01-delete-clean` | Clean deletion, no orphans |

The advanced line (`tests/e2e-advanced`, requires operator capabilities):

| Case | What it checks |
|---|---|
| `shd-01-cluster-bootstrap` | Bootstrapping the Cluster topology (`--cluster create`) |
| `scl-01-scale-up` | Scale-up + autoReshard convergence |
| `fal-01-failover` | Promoting a replica when the primary fails (Replication) |
| `bkp-01-backup` | Backup → S3/MinIO, object presence |

See the e2e harness's `tests/README.md` for how to run it and the CI matrix.
