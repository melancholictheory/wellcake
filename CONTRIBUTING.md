# Contributing to valkey-operator

This operator manages a **stateful data plane** — a bug in failover, scaling, or
backup can cause data loss, not just downtime. Treat changes accordingly:
review carefully, write tests, and prefer reversible steps.

## Dev environment

- Go (toolchain pinned in `go.mod`; CI uses `golang:1.25` with
  `GOTOOLCHAIN=local`).
- Dependencies are managed with Go modules (no `vendor/` directory) and fetched
  from the module proxy; CI caches the module download between runs.
- Tooling lives in `bin/` via the Makefile (`make controller-gen kustomize
  envtest …`).

## Build, test, lint

```sh
go build ./...
go test -short ./...        # unit + reconcile(fake) + webhook + plugin; no cluster
golangci-lint cache clean && golangci-lint run   # cache clean after --fix (see below)

# Full envtest (real kube-apiserver+etcd):
make envtest
export KUBEBUILDER_ASSETS="$(pwd)/bin/k8s/<ver>-<os>-<arch>"   # ABSOLUTE path
go test ./internal/controller/ ./internal/webhook/... -run 'TestControllers|TestAPIs' -count=1
```

Test layers and what each one covers: [docs/testing.md](docs/testing.md).

**Gotcha:** golangci-lint caches analysis per package; after `--fix` (or any
auto-rewrite) run `golangci-lint cache clean` before trusting a clean result —
CI runs with a cold cache and will otherwise catch what the cache hid.

**Gotcha:** `KUBEBUILDER_ASSETS` must be an **absolute** path — `go test` runs
from the package directory, and controller-runtime execs etcd via that path.

## Code generation (run after API changes)

```sh
make generate    # deepcopy
make manifests   # CRD + RBAC + webhook manifests (config/)
# sync regenerated CRDs into the chart:
cp config/crd/bases/cache.wellcake.io_*.yaml charts/valkey-operator/crds/
```

RBAC and webhook config live BOTH in `config/` (kustomize) AND in the Helm chart
(`charts/valkey-operator/templates/`) — update both when adding a permission or
webhook.

## Common change recipes

- **New topology / reconcile path:** add it under `internal/controller/`, keep
  the `reconcile*` helpers below the gocyclo threshold (extract sub-steps), add
  unit + reconcile(fake) tests and an envtest case if it touches CRD/CEL/status.
- **New Valkey version:** update the image map / probe availability in your
  registry, update the compatibility matrix
  (docs/valkey-compatibility.md), and run the advanced e2e suite
  (shd/scl/fal/bkp) against it.
- **API change:** v1beta1 is the storage/hub version; v1alpha1 is a maintained
  spoke with conversion (`api/v1alpha1/conversion.go`). Identical-schema changes
  are lossless; at the first divergence, write an explicit per-field conversion
  and switch the CRD to `conversion: Webhook`.
- **CHANGELOG:** keep `CHANGELOG.md` current (Keep a Changelog) — required.

## Review expectations

- Data-correctness paths (`failover.go`, `cluster.go`, `resources.go`,
  `valkeycluster_controller.go`, `api/`) require maintainer review — see
  `CODEOWNERS`.
- Operator changes that touch the data plane should be validated against the
  end-to-end suite (base + advanced) before promotion; the operator's own CI
  runs unit + envtest, while the deeper behavioral checks live in a separate
  e2e harness.
- Prefer additive, reversible changes; document any operation that can lose data
  in a runbook.

## Release / promote

Master CI publishes `image:git-<sha>` + `:latest` and a chart `0.1.0-<sha>-dev`.
Promotion is driven by a higher-level platform or GitOps layer that pins a
specific operator version and lets its own e2e validate the bump. See the
production-readiness checklist before any prod rollout:
[docs/production-readiness.md](docs/production-readiness.md).
