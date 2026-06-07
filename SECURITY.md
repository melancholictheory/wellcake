# Security Policy

`wellcake` is early/experimental software. We still take security seriously and
appreciate responsible disclosure.

## Supported versions

The project has not yet cut a stable release. Security fixes land on `main` and
in the latest pre-release images/charts.

| Version            | Supported          |
| ------------------ | ------------------ |
| `main` / latest    | :white_check_mark: |
| older pre-releases | :x:                |

## Reporting a vulnerability

**Please do not open a public issue for security vulnerabilities.**

Report privately using one of:

1. **GitHub Security Advisories** — preferred. Go to the
   [Security tab](https://github.com/melancholictheory/wellcake/security/advisories/new)
   and open a private vulnerability report.
2. **Email** — <selimvhorst@gmail.com> with the details and, ideally, a
   reproduction.

Please include:

- a description of the vulnerability and its impact,
- steps to reproduce or a proof of concept,
- affected versions/commits, and
- any suggested remediation.

We aim to acknowledge reports within a few days and will keep you informed of
progress. Once a fix is available we will coordinate disclosure and credit you
in the advisory unless you prefer to remain anonymous.

## Scope

This operator runs with cluster privileges and manages stateful workloads.
Reports of particular interest include: privilege escalation via RBAC, secret
exposure (passwords, TLS keys, backup credentials), data-loss paths during
failover/reshard/restore, and injection into rendered manifests or generated
Valkey configuration.
