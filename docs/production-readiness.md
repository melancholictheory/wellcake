# Production-readiness checklist

Gate before adopting valkey-operator for a prod workload. Derived from the design review.
An honest status: the operator is **well-built, but young**; this list is what
stands between "works in e2e" and "run someone's prod data on it."

## Engineering / code (mostly done)

- [x] CRD validation: CEL XValidation + validating webhook (defense-in-depth)
- [x] Defaulting via mutating webhook (+ defensive reconcile-side)
- [x] API versioning: v1beta1 hub/storage + v1alpha1 conversion
- [x] Lint gate (16 linters, blocking) + gofmt gate
- [x] envtest in CI (real kube-apiserver) — reconcile/CEL/finalizer/conversion
- [x] Backup verification (`valkey-check-rdb`) before loading
- [x] StatefulSet: Parallel pod management; operator-managed PVC expansion
- [x] Dedicated ACL user for Sentinel→master (no data access)
- [x] Operator metrics + PrometheusRule (control-plane + data-plane alerts)
- [x] Leader election (HA-capable)
- [x] e2e in kind (separate e2e harness): create/connect/ops/TLS+auth/delete +
      bootstrap/scale/failover/backup (base + advanced)

## Testing gaps (before prod)

- [ ] **Integration/chaos tests** — network partition (split-brain),
      simultaneous loss of replicas (quorum), slow BGSAVE (premature
      failover), PVC-full, operator crash mid scale-down. Today: unit +
      fake-client + envtest + happy-path e2e.
- [ ] **Operator upgrade test** — v0.1.0 → next with existing CRs (no data
      loss, config-hash rolling only when the rendered output changes).
      Procedure: runbook Op3.
- [ ] **Conversion-under-load test** — v1alpha1↔v1beta1 once the schemas diverge.
- [ ] **Scale test** — 50+ CRs: reconcile latency, load on the API server.

## Process / org (the real blockers — see DOUBTS Risk 1–2)

- [ ] **Bus factor > 1** — ≥2 maintainers with knowledge of Valkey internals
      (CODEOWNERS currently lists a single one — that's the risk itself, not the fix).
- [x] CONTRIBUTING.md + CODEOWNERS (review accountability)
- [ ] **Security audit** — RBAC scope, webhook bypass, secret handling
- [ ] **Operator SLO** — acceptable reconcile latency / failover time, under
      monitoring
- [x] Operator runbook (installation, Op2 recovery, Op3 upgrade,
      troubleshooting)
- [x] Valkey compatibility matrix ([docs/valkey-compatibility.md](valkey-compatibility.md))
- [x] ADR: build our own operator vs adopt valkey-io ([adr/0003](adr/0003-build-vs-adopt.md))

## Recommended rollout order

1. **Cache / Standalone** non-critical workloads first (minimal blast radius).
2. **Replication** with an understanding of the durable-profile admission warning
   (the operator arbitrates failover — for strict RTO, prefer Sentinel/Cluster).
3. **Cluster** once the integration/chaos suite exists.
4. Durable prod data — only after closing out bus-factor, chaos tests, and the
   security audit, and after passing the staging soak (ROADMAP P5).

> The verdict echoes the review: production-readiness ≈ bus-factor × testing ×
> operational maturity. The code/architecture are solid; the open items above are
> the gate.
