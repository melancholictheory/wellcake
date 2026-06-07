# wellcake

[![Tests](https://github.com/melancholictheory/wellcake/actions/workflows/test.yml/badge.svg)](https://github.com/melancholictheory/wellcake/actions/workflows/test.yml)
[![Lint](https://github.com/melancholictheory/wellcake/actions/workflows/lint.yml/badge.svg)](https://github.com/melancholictheory/wellcake/actions/workflows/lint.yml)
[![Release](https://github.com/melancholictheory/wellcake/actions/workflows/release.yml/badge.svg)](https://github.com/melancholictheory/wellcake/actions/workflows/release.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/melancholictheory/wellcake)](https://goreportcard.com/report/github.com/melancholictheory/wellcake)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
![Status: experimental](https://img.shields.io/badge/status-experimental-orange.svg)

A Kubernetes operator for [Valkey](https://valkey.io/). It manages Valkey across
four topologies — **Standalone, Replication, Sentinel, and Cluster** — with
operator-driven failover, proactive zero-downtime rolling restarts, cluster
bootstrap/scale/reshard, S3 backups, multi-region replication, and no-restart
secret rotation.

`wellcake` ships the `valkey-operator` controller plus Helm charts for the
operator and for individual clusters. It is an independent project; the goal is
to replace hand-rolled StatefulSets and chart-based Redis/Valkey deployments with
a single declarative CRD that understands Valkey's failure and scaling semantics.

> **Status:** early / experimental. APIs are `v1alpha1` (served) and `v1beta1`
> (hub/storage). Run it in a non-critical environment first.

## Documentation

- [docs/architecture.md](docs/architecture.md) — how the operator works internally
- [docs/topologies.md](docs/topologies.md) — Standalone / Replication / Sentinel / Cluster
- [docs/crd-reference.md](docs/crd-reference.md) — full CRD field reference
- [docs/runbook.md](docs/runbook.md) — operational procedures and failure scenarios
- [docs/kubectl-plugin.md](docs/kubectl-plugin.md) — the `kubectl valkey` plugin (status / cli / backup / restart / reshard / failover / hibernate / certificate / report)
- [docs/development.md](docs/development.md) — developing the operator
- [CONTRIBUTING.md](CONTRIBUTING.md) — maintaining the operator (workflow, codegen, review)
- [docs/testing.md](docs/testing.md) — test map: levels (unit / RESP-mock / reconcile / webhook / envtest / e2e), coverage and how to run
- [docs/chaos-testing.md](docs/chaos-testing.md) — chaos/integration test plan (network partition, quorum loss, slow BGSAVE, operator crash)
- [docs/production-readiness.md](docs/production-readiness.md) — production-readiness checklist
- [docs/valkey-compatibility.md](docs/valkey-compatibility.md) — operator ↔ Valkey version matrix
- [docs/adr/](docs/adr/) — ADRs: [0001 ASM](docs/adr/0001-atomic-slot-migration.md), [0002 workload primitive (STS vs Deployment)](docs/adr/0002-workload-primitive.md), [0003 build vs adopt](docs/adr/0003-build-vs-adopt.md), [0004 proactive failover on rolling restart](docs/adr/0004-proactive-failover-rolling-restart.md), [0005 per-shard workload](docs/adr/0005-per-shard-workload.md)
- [docs/migration-bitnami.md](docs/migration-bitnami.md) — migrating from chart-based deployments
- [ROADMAP.md](ROADMAP.md) — open questions and direction
- [CHANGELOG.md](CHANGELOG.md) — change history

## Why

Running Valkey on Kubernetes well means encoding its operational rules — quorum,
failover, slot ownership, reshard safety — into the control loop, not into
runbooks. Generic StatefulSet charts leave those rules to humans. `wellcake`
aims to:

- cover **all four common deployment shapes** behind one CRD, with cluster-mode
  as a first-class topology rather than a bolt-on;
- make **failover and rolling restarts safe by default** — promote a healthy
  replica *before* taking down a primary, instead of reacting after an outage;
- keep **data-correctness paths** (bootstrap, scale, reshard, restore) atomic and
  idempotent, including Valkey 9.1+ Atomic Slot Migration;
- treat **persistence, TLS, ACLs, backups and secret rotation** as declarative
  spec rather than out-of-band procedures.

## Topologies

| Topology    | Status | Failover                          |
| ----------- | ------ | --------------------------------- |
| Standalone  | ✅     | none                              |
| Replication | ✅     | operator-driven via `REPLICAOF`   |
| Sentinel    | ✅     | Sentinel quorum                   |
| Cluster     | ✅     | native gossip                     |

## CRDs

| Kind            | Purpose                                                   |
| --------------- | -------------------------------------------------------- |
| `ValkeyCluster` | Deploy Valkey in one of the four topologies.             |
| `ValkeyACL`     | Declarative ACL users on a `ValkeyCluster`.              |

## Features

- All four topologies: Standalone, Replication, Sentinel, Cluster
- **Dual-track profiles**: `Cache` (allkeys-lru, persistence off, 1Gi PVC) and
  `Durable` (noeviction, RDB+AOF, 10Gi PVC)
- **Operator-driven failover** for Replication: TCP survey every 15s, promote the
  replica with the max offset, split-brain protection after a pod-0 restart
- **Proactive rolling restart** (ADR 0004, opt-in via annotation
  `valkey.wellcake.io/proactive-rollout: "true"`, default OFF) for Replication /
  Cluster / Sentinel: the StatefulSet switches to `OnDelete`, the operator rolls
  pods one at a time and performs handover (replica promote / `CLUSTER FAILOVER`
  / `SENTINEL FAILOVER`) *before* restarting the old primary — downtime window
  ~0 instead of the reactive ~15–20s
- **Cluster bootstrap** via a one-shot Job with `valkey-cli --cluster create`
- **Cluster scale-up / scale-down** via `add-node` / reshard-away + `del-node`;
  `autoReshard` (default) rebalances slots automatically, so changing `shards`
  converges with no manual steps; on Valkey 9.1+ resharding uses Atomic Slot
  Migration (ASM, version-gated, ADR 0001) — atomic, no open-slot left behind on
  interruption
- **Per-shard workload** (`spec.perShardWorkload`, opt-in / immutable /
  Cluster-only, experimental): one StatefulSet + Service per shard — enables
  shard-aware anti-affinity and clean shard scale-down (delete the leaving
  shard's StatefulSet)
- **Sentinel** with its own StatefulSet, ConfigMap and Service on port 26379
- **ValkeyACL** — a separate CRD for managing users via `ACL SETUSER ... reset`
  (idempotent through the `reset` token); for Cluster it is applied to all nodes
  of a shard
- **S3 backups** (CronJob with `valkey-cli --rdb`, retention, SSE, per-shard for
  Cluster) and restore via `spec.restoreFrom`
- **Multi-region** async replication between clusters via `spec.replicateFrom`
  (including `replicateFrom.caSecret` — the operator merges the local CA and the
  source cluster's CA into a single trust bundle for cross-CA mTLS)
- **No-restart secret rotation**: password rotation in place via annotation
  `valkey.wellcake.io/rotate-password` (live `ACL SETUSER`, no replication blip);
  TLS cert auto-reload on cert-manager renewal (live `CONFIG SET tls-cert-file`)
- **Prometheus metrics** + optional ServiceMonitor and PrometheusRule with alerts
- **`kubectl valkey` plugin** (status / cli / backup / restart / reshard /
  failover / hibernate / certificate / report)
- **TLS** via a cert-manager Issuer or an existing Secret
- **PDB + TopologySpreadConstraints** across zones by default
- **NetworkPolicy** (optional): default-deny + same-namespace + valkey-to-valkey
- **CEL XValidation**: immutable `topology` / `profile` / `storageClassName`,
  expand-only `storage.size`, conditional `shards` / `sentinel`
- **Config hash → rolling restart**: sha256 of `ConfigMap.Data` in the pod template
- **`maxmemory` auto** = 60% of `resources.limits.memory`
- **Version-gated 9.x resilience directives** in `renderValkeyConf` (by image
  version): `cluster-allow-replica-migration no` (Cluster), `shutdown-on-sigterm
  failover` (≥9.0), `tls-auto-reload-interval 3600` (TLS + ≥9.1) — overridable
  via `spec.config`
- **InitContainer** for runtime-config generation (no scripts baked into the CM)
- **Always PVC, never emptyDir** — ephemeral-storage pressure can trigger
  eviction, which for an in-memory service means downtime
- **Deletion finalizer** with a topology-aware cleanup hook

## Install the operator (one command)

```sh
make deploy-operator
```

Installs/upgrades the operator via Helm into the `valkey-system` namespace
(created automatically): CRDs + RBAC + Deployment, pulling the image from
`ghcr.io/melancholictheory/wellcake:latest`. Overrides: `OPERATOR_TAG=git-<sha>`,
`OPERATOR_NAMESPACE=<ns>`, `HELM_ARGS="--set webhooks.enabled=true"`. Remove with
`make undeploy-operator`.

Equivalent directly via Helm:

```sh
helm upgrade --install valkey-operator charts/valkey-operator \
  --namespace valkey-system --create-namespace --set image.tag=latest
```

> Webhooks (validating / mutating / conversion) are opt-in
> (`--set webhooks.enabled=true`, require cert-manager) and disabled by default.

Operator options:
- `rbac.scope=namespace` or `watchNamespace=<ns>` — single-namespace mode
- `metrics.serviceMonitor.enabled=true` — for the Prometheus Operator
- `--skip-crds` — skip CRDs when managing them via GitOps

## Deploy a cluster (Helm)

```sh
helm upgrade --install my-cache charts/valkey-cluster -n apps \
  --set topology=Replication --set profile=Cache --set replicas=3
```

## Quickstart (out-of-cluster)

```sh
make manifests generate
make install
make run
kubectl apply -f config/samples/cache_v1alpha1_valkeycluster.yaml
```

See [docs/development.md](docs/development.md) for details.

## Layout

```
api/v1beta1/                   — CRD types (hub/storage version — edit here)
api/v1alpha1/                  — served spoke + conversion to hub
cmd/
  main.go                      — manager entrypoint
  kubectl-valkey/              — kubectl plugin (status/cli/backup/…)
internal/controller/
  valkeycluster_controller.go  — Reconcile + topology dispatch + finalizer + hibernate
  resources.go                 — Service/CM/STS/Secret/PDB/NP builders
  cluster.go                   — Cluster topology: bootstrap/scale/reshard Jobs
  sentinel.go                  — Sentinel topology
  failover.go                  — operator-driven + manual failover (go-redis)
  backup.go                    — S3 backup CronJob
  metrics.go                   — custom Prometheus metrics
  valkeyacl_controller.go      — ValkeyACL reconciler
docs/                          — documentation
charts/                        — Helm charts (operator, cluster)
config/                        — kustomize manifests (kubebuilder)
```

## License

[Apache 2.0](LICENSE).
