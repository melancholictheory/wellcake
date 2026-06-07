# kubectl plugin (`kubectl valkey`)

A standalone CLI plugin providing operational commands on top of the ValkeyCluster CR. The
binary is named `kubectl-valkey`; kubectl discovers any `kubectl-<name>` on the `PATH` and
invokes it as `kubectl <name>`.

## Installation

```sh
make install-plugin
# or manually:
go build -o kubectl-valkey ./cmd/kubectl-valkey && mv kubectl-valkey ~/.local/bin/
```

Verify:

```sh
kubectl plugin list | grep kubectl-valkey
kubectl valkey --help
```

Via krew (after a release with artifacts): the manifest template lives at
`dist/krew/valkey.yaml` — the `version`/`uri`/`sha256` fields are filled in by the
release pipeline.

In every command the namespace is taken from the current kube context; it can be overridden
with `-n/--namespace`.

## Commands

| Command | Type | Purpose |
| ------- | ---- | ------- |
| `status <cluster>` | read-only | Cluster state summary |
| `cli <cluster> [-- args]` | exec | valkey-cli on the primary/pod-0 |
| `backup <cluster>` | write | On-demand backup Job from the CronJob |
| `restart <cluster>` | trigger | Rolling restart of pods |
| `reshard <cluster>` | trigger | Slot rebalance (Cluster) |
| `failover <cluster> [--to pod]` | trigger | Promote a replica (Replication/Sentinel) |
| `hibernate on\|off <cluster>` | trigger | Freeze (scale to 0) / unfreeze |
| `certificate <cluster> [-o dir]` | read-only | Show/export TLS material |
| `report <cluster> [-o dir]` | read-only | Diagnostic dump of the CR + resources |

Trigger commands (`restart`/`reshard`/`failover`/`hibernate`) do not perform the operation
themselves — they set a token annotation `valkey.wellcake.io/...` on the CR, and the actual
work is done by the operator, which records the processed token in `status`. One token = one
action (idempotent); this maps naturally onto GitOps/Crossplane.

### `status`

```sh
kubectl valkey status web-cache
kubectl valkey status app-cache -n app-cache
```

Reads the CR `status` and prints:

- a header — `cluster`, `topology`, `profile`, `image`, `phase`;
- for Standalone/Replication/Sentinel — `primary` and `ready/desired`;
- for Cluster — `readyShards/shards`, the bootstrap flag, and a table of shards
  (`#`, primary, replicas, slots, health, replica lag in bytes);
- a `conditions` table (`Available`, `ShardsHealthy`, `ClusterInitialized`).

The command changes nothing and requires no operator-side work — the output is assembled
from fields that the reconciler already keeps fresh.

### `cli` (alias `exec`)

```sh
kubectl valkey cli web-cache                       # interactive valkey-cli on the primary
kubectl valkey cli web-cache -- INFO replication   # one-off command
kubectl valkey cli app-cache --pod app-cache-2 -- CLUSTER NODES
```

Runs `kubectl exec -it` into a pod (by default the primary from `status`; for Cluster,
`<cluster>-0` with the `-c` flag), launching `valkey-cli`. The port and TLS flags are set
automatically from `spec.tls`. The password is read from the cluster Secret and passed into
the pod as `REDISCLI_AUTH`.

> During the exec the password is visible in the process list (locally in the `kubectl` argv
> and in the pod). This is an admin tool; for strict environments use a plain `kubectl exec`
> and `AUTH` manually.

Requires `kubectl` on the `PATH` (it is there — the plugin is itself launched through kubectl).

### `backup`

```sh
kubectl valkey backup web-cache
```

Creates a one-off Job from the backup CronJob `<cluster>-backup` (equivalent to
`kubectl create job --from=cronjob/<cluster>-backup`). Requires `spec.backup.enabled` on the
cluster; otherwise it returns a clear error about the missing CronJob. Prints the name of the
created Job.

### `restart`

```sh
kubectl valkey restart web-cache
```

Sets `valkey.wellcake.io/restart=<token>` on the CR. The operator propagates the token into
the StatefulSet's pod-template annotation — this rolls the pods (the same mechanism as the
config hash). Works for all topologies. Idempotent: invoking it again with the same state
simply sets a new token and triggers one more rollout.

### `reshard` (Cluster only)

```sh
kubectl valkey reshard app-cache
```

Sets `valkey.wellcake.io/reshard=<token>`. The operator (once the cluster is initialized and
all pods are Ready) runs a one-off Job `<cluster>-reshard` with
`valkey-cli --cluster rebalance --cluster-use-empty-masters` and writes the token into
`status.lastReshardToken`. For non-Cluster topologies the command returns an error.

### `failover` (Replication/Sentinel)

```sh
kubectl valkey failover web-cache             # promote the most up-to-date replica
kubectl valkey failover web-cache --to web-cache-2
```

Sets `valkey.wellcake.io/failover=<token>` (and `valkey.wellcake.io/failover-target` when
`--to` is given). The operator promotes the chosen replica (or the most up-to-date one by
offset), even if the current primary is alive, repoints the remaining replicas, and writes
the token into `status.lastFailoverToken`. If the target pod is not a reachable replica, the
request is not processed (it will be retried) until a valid target appears. For Cluster the
command returns an error (there, failover is handled by native gossip).

### `hibernate on|off`

```sh
kubectl valkey hibernate on  web-cache    # go to sleep: scale to 0, PVCs are retained
kubectl valkey hibernate off web-cache    # wake up: bring the replicas back
```

`on` sets `valkey.wellcake.io/hibernate=true` — the operator scales the StatefulSet to 0
(the StatefulSet controller retains the PVCs), sets `status.phase=Hibernated`, and stops
active management (no bootstrap/survey/failover). `off` sets the annotation to `false` — a
normal reconcile rebuilds the StatefulSet to its regular size, and the pods come back up on
the same PVCs.

### `certificate`

```sh
kubectl valkey certificate web-cache              # prints ca.crt to stdout
kubectl valkey certificate web-cache -o ./certs   # writes ca.crt/tls.crt/tls.key
```

Reads the cluster's TLS Secret (cert-manager or `spec.tls.existingSecret`). Without `-o` it
prints `ca.crt`. With `-o <dir>` it exports `ca.crt`/`tls.crt`/`tls.key` so a client can
connect over TLS. If TLS is not enabled — error.

### `report`

```sh
kubectl valkey report web-cache               # ./valkey-report-web-cache/
kubectl valkey report web-cache -o /tmp/dump
```

Collects a diagnostic dump: the CR (`cluster.yaml`) plus the owned objects matched by the
`app.kubernetes.io/instance` label (StatefulSet, Services, ConfigMaps, PDB, Pods) and the
Events related to the cluster — each in its own YAML file. Read-only, convenient to attach to
a ticket.
