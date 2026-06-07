# ADR 0003 — Build a dedicated operator vs adopt valkey-io

- Status: **Accepted** (building it now, with explicit revisit triggers)
- Date: 2026-06-01
- Context: a design review asked why maintain a bespoke operator when the
  official `valkey-io/valkey-operator` exists and is maturing, and flagged the
  bus-factor / maintenance risk.

## Context

`valkey-io/valkey-operator` is the official operator of the Valkey project:
upstream support, proactive failover, Atomic Slot Migration (9.0+), a per-node
CR. But at the time of writing it covers **only the Cluster topology** and has no
backup/restore, no cert-manager integration, no ACL management and no kubectl
plugin.

Our target is to run Valkey as a self-service primitive across a fleet that needs
all of the common deployment shapes — Standalone / Replication / Sentinel /
Cluster (with cluster-mode the majority shape) — a dual-track profile model
(Cache / Durable), S3 backup/restore, ACLs for multi-tenancy, TLS via
cert-manager, and a higher-level platform or GitOps layer wrapping it for
consumers.

## Decision

**Build and maintain a bespoke operator now**, because no upstream operator
covers the required surface:

| Need | valkey-io | bespoke |
|---|---|---|
| Standalone / Replication / Sentinel | ❌ (Cluster only) | ✅ |
| Cluster | ✅ | ✅ |
| S3 backup/restore | ❌ | ✅ |
| ACL management (CRD) | ❌ | ✅ |
| cert-manager TLS / mTLS | ❌ | ✅ |
| Cache/Durable profiles | ❌ | ✅ |
| Higher-level platform/GitOps wrapper | needs an operator | ✅ |
| kubectl plugin | ❌ | ✅ |

Adopting valkey-io today would discard three of the four topologies and most of
the platform features we need.

### Mitigating the choice risk

- **Lock-in limited to a higher-level abstraction.** Consumers use a higher-level
  platform/GitOps abstraction rather than the operator's CRDs directly — so the
  underlying operator can be swapped without changing the consumer-facing API.
- **API versioning in place** (v1alpha1 → v1beta1 hub + conversion), so the CRDs
  can evolve without breaking stored objects.
- **Bus factor / maintenance** — a real risk: see CONTRIBUTING.md + CODEOWNERS
  for review accountability and the production-readiness checklist as the gate
  before prod.

## Revisit triggers

Re-evaluate adopt-vs-build (or a hybrid: valkey-io for Cluster, bespoke for
Replication/Sentinel) when **any** of the following holds:

- valkey-io gains the Sentinel **and** Replication topologies, **and**
  backup/restore + ACL — i.e. feature parity across our surface.
- The cost of maintaining adaptation to Valkey versions (each major ≈2–4 weeks)
  becomes a bottleneck, while upstream tracks releases faster.
- A class of security/correctness bugs emerges that upstream has already solved
  (for example, cluster gossip edge cases) while we keep rediscovering them.

In that case the migration path is: map the consumer-facing abstraction →
valkey-io CRDs in a new wrapping layer, keep the consumer API stable, and migrate
topology by topology.

## Consequences

- **Pros:** full control over the required feature surface; consumers insulated
  by the higher-level abstraction; clear conditions for a revisit.
- **Cons / accepted:** ongoing maintenance + Valkey version tracking is on us;
  the bus-factor risk is mitigated by process (review, onboarding, checklist),
  not by code.
