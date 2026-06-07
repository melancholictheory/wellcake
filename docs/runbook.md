# Runbook

Operational procedures for valkey-operator. Each section lists the
command, the expected kubectl output, and where to look if something goes
wrong.

## Install / upgrade

```sh
helm upgrade --install valkey-operator charts/valkey-operator \
  --namespace valkey-system --create-namespace \
  --set image.repository=ghcr.io/melancholictheory/wellcake \
  --set image.tag=latest
```

Verification:

```sh
kubectl -n valkey-system rollout status deploy/valkey-operator
kubectl get crd valkeyclusters.cache.wellcake.io valkeyacls.cache.wellcake.io
```

For GitOps: add `--skip-crds` and apply the CRDs through your own pipeline.

## Operator commands and flags

The operator binary (`/manager`) has no subcommands — it's a
controller-runtime manager configured via flags and a couple of env
variables. The Helm chart sets these for you; below is what's actually
available if you run it by hand or debug the Deployment.

Full list of flags:

```sh
kubectl -n valkey-system exec deploy/valkey-operator -- /manager --help
```

### Manager flags

| Flag | Default | Purpose |
| ---- | ------- | ------- |
| `--metrics-bind-address` | `0` | Address of the metrics endpoint. `0` — disabled, `:8443` — HTTPS, `:8080` — HTTP. The chart sets it from `metrics.port`, or `0` when `metrics.enabled=false`. |
| `--metrics-secure` | `true` | Serve metrics over HTTPS with an authn/authz filter. `--metrics-secure=false` — plain HTTP. |
| `--health-probe-bind-address` | `:8081` | Address of the liveness/readiness probes (`/healthz`, `/readyz`). Chart sets it from `probes.port`. |
| `--leader-elect` | `false` | Leader election: guarantees a single active manager. The chart enables it when `replicaCount > 1`. Lease ID is `1e0a58e3.wellcake.io`. |
| `--enable-http2` | `false` | HTTP/2 for the metrics/webhook servers. Disabled by default due to a CVE (Rapid Reset). |
| `--webhook-cert-path` | `""` | Directory holding the webhook certificate. Empty → controller-runtime generates a self-signed one. The chart mounts the cert-manager secret here when `webhooks.enabled=true`. |
| `--webhook-cert-name` / `--webhook-cert-key` | `tls.crt` / `tls.key` | File names of the cert/key in the directory above. |
| `--metrics-cert-path` | `""` | Directory holding the metrics server certificate (for TLS metrics via cert-manager). |
| `--metrics-cert-name` / `--metrics-cert-key` | `tls.crt` / `tls.key` | File names of the cert/key for metrics. |

Logging (zap flags from controller-runtime):

| Flag | Default | Purpose |
| ---- | ------- | ------- |
| `--zap-log-level` | `info` | `debug`/`info`/`error`, or a number. |
| `--zap-encoder` | `json` (prod) | `json` or `console`. |
| `--zap-stacktrace-level` | `error` | The level from which to emit stacktraces. |
| `--zap-time-encoding` | `epoch` | Timestamp format. |
| `--zap-devel` | `false` | Dev mode (console + debug). |

### Environment variables

| Env | Purpose |
| --- | ------- |
| `ENABLE_WEBHOOKS` | `="false"` disables registration of the validating webhooks (enabled by default). The chart sets it via `webhooks.enabled`. |
| `WATCH_NAMESPACE` | Restricts the operator's watch to the listed namespaces (comma-separated). Empty/unset → cluster-scoped (all namespaces). The chart sets it via `watchNamespace`; combine with `rbac.scope=namespace` so the ServiceAccount only gets namespaced RBAC. |

## kubectl plugin (`kubectl valkey`)

Operational commands (`status`, `cli`, `backup`, …) live in a separate
kubectl plugin. Installation, the full list of commands, and examples are
in [kubectl-plugin.md](kubectl-plugin.md).

```sh
make install-plugin
kubectl valkey status <cluster> -n <ns>
```

## Creating a cluster

Replication:

```sh
helm upgrade --install web-cache charts/valkey-cluster -n web \
  --set topology=Replication --set profile=Cache --set replicas=3
```

Cluster (sharded):

```sh
cat <<EOF | kubectl apply -f -
apiVersion: cache.wellcake.io/v1alpha1
kind: ValkeyCluster
metadata: { name: app-cache, namespace: app-cache }
spec:
  topology: Cluster
  profile: Cache
  shards: 6
  replicasPerShard: 1
  resources:
    requests: { cpu: 1, memory: 8Gi }
    limits:   { cpu: 2, memory: 16Gi }
EOF
```

Watching:

```sh
kubectl -n app-cache get valkeycluster app-cache -w
# NAME    TOPOLOGY   REPLICAS   READY   PHASE
# app-cache   Cluster    12         0       Creating
# ...
# app-cache   Cluster    12         12      Ready
```

Status fields for Cluster:

```sh
kubectl -n app-cache get valkeycluster app-cache -o jsonpath='{.status}' | jq
# { "phase": "Ready", "shards": 6, "readyShards": 6, "clusterInitialized": true,
#   "lastAppliedReplicas": 12, "readyReplicas": 12, ... }
```

## Retrieving the auth password

If `auth.existingSecret` is not set, the operator generated its own:

```sh
kubectl -n <ns> get secret <cluster>-auth -o jsonpath='{.data.password}' | base64 -d
```

### Password rotation without a restart (operator-managed Secret)

For clusters with auth and an **operator-managed** Secret
(`auth.existingSecret` NOT set), password rotation is performed in place,
without restarting pods, via an annotation carrying a unique token:

```sh
kubectl -n <ns> annotate valkeycluster <cluster> \
  valkey.wellcake.io/rotate-password="$(date +%s)" --overwrite
```

The operator generates a new password, rolls it out to the live instances
(a live `ACL SETUSER` on the `default` user + `masterauth`, with no
replication blip) and rewrites `<cluster>-auth`. The action is idempotent
by token — a repeat reconcile with the same annotation value does nothing.
Verify that the operator processed the rotation:

```sh
kubectl -n <ns> get valkeycluster <cluster> -o jsonpath='{.status.lastPasswordRotationToken}'
# should match the value of the rotate-password annotation
```

After rotation, re-read the password from the Secret (command above) for
new connections. Already-connected clients using the old password will be
rejected on their next AUTH — switch them to the new password from the
Secret.

**Limitation:** for a **user-managed** Secret (`auth.existingSecret` set),
in-place rotation is not available — the operator does not know the old
password if the user changes it. There, rotation is the old way: edit the
source Secret and restart the pods manually
(`kubectl rollout restart sts/<cluster>`).

### Reloading the TLS certificate without a restart

With TLS enabled (`tls.enabled`), the operator reloads the certificate on
the live pods itself whenever the TLS Secret changes — **no action is
required**. The typical case: cert-manager renews the certificate on its
own schedule.

How it works:
- The operator watches the TLS Secret (`<cluster>-tls` or
  `tls.existingSecret`) and compares the certificate's SHA-256 fingerprint
  against `status.lastTLSCertFingerprint`.
- On a change, it issues a live
  `CONFIG SET tls-cert-file/tls-key-file/tls-ca-cert-file` on the same
  paths — Valkey re-reads the mounted files. No restart, no break in
  replication/clients.
- Success is recorded only once the pod actually starts serving the new
  certificate (verified via a TLS handshake) — this accounts for the
  kubelet's delay in syncing the mounted volume (which can be up to
  ~60-90s after the Secret is updated).

Verify that the operator applied the current certificate:

```sh
# fingerprint in the status
kubectl -n <ns> get valkeycluster <cluster> -o jsonpath='{.status.lastTLSCertFingerprint}'; echo
# fingerprint of the certificate the pod actually serves (via port-forward)
kubectl -n <ns> port-forward <cluster>-0 16380:6380 >/dev/null 2>&1 &
echo | openssl s_client -connect 127.0.0.1:16380 2>/dev/null | openssl x509 -outform DER | shasum -a 256
# the two values should match; certificate in the Secret:
kubectl -n <ns> get secret <cluster>-tls -o jsonpath='{.data.tls\.crt}' | base64 -d | openssl x509 -outform DER | shasum -a 256
```

If under mutual TLS (`tls.mutualTLS: true`) and you need a manual forced
reload — just update the TLS Secret (even with the same certificate the
fingerprint won't change → no-op; to force it you need a new certificate).

### Multi-region replication over TLS with a separate CA (S4)

When a target cluster replicates from an external primary in another region
(`spec.replicateFrom` + `tls=true`) and the source is signed by **its own**
CA (different from the local one), set `replicateFrom.caSecret` — the
operator establishes trust itself. Previously you had to mount the source's
CA into the pod by hand.

```yaml
spec:
  topology: Replication          # or Standalone
  tls:
    enabled: true                # local pods need their own cert/key for outbound tls-replication
  replicateFrom:
    host: src-primary.dc2.example.svc.cluster.local
    port: 6380
    tls: true
    passwordSecret: { name: source-pass }
    caSecret:                    # ← S4: CA bundle of the source cluster
      name: source-ca            #   Secret in the same namespace (key defaults to ca.crt)
```

Preparing the Secret with the source CA (copy ca.crt from the source
cluster):

```sh
# on the source cluster:
kubectl -n <src-ns> get secret <src-cluster>-tls -o jsonpath='{.data.ca\.crt}' | base64 -d > /tmp/source-ca.crt
# on the target cluster:
kubectl -n <ns> create secret generic source-ca --from-file=ca.crt=/tmp/source-ca.crt
```

How it works: Valkey reads **one global** `tls-ca-cert-file` for all TLS
connections. In `config-init`, the operator merges the cluster's local CA
and the source CA into a single bundle `/data/ca-bundle.crt` and points
`tls-ca-cert-file` at it — so both internal replication (local CA) and the
outbound link to the source primary (source CA) pass verification. Live
rotation of the local certificate (`tls-reload`) is also pointed at this
bundle, so a renewal doesn't drop trust in the source.

Verify the link is up (on target pod-0):

```sh
kubectl -n <ns> exec <cluster>-0 -- valkey-cli -a "$PW" --no-auth-warning \
  --tls --cacert /data/ca-bundle.crt -p 6380 info replication
# master_link_status:up; master_host = source primary
```

> **mTLS symmetry (important).** S4 only automates the **target→source**
> direction: the replica verifies the source master's certificate. But the
> replication handshake is bidirectional — with `tls.mutualTLS` or the
> default `tls-auth-clients optional` on the source, the master also
> verifies the replica's **client** certificate (the Valkey replica
> presents its own `tls-cert-file`). If the target is signed by **its own**
> CA, the source won't accept it (`certificate verify failed` in the
> source's logs, `tlsv1 alert unknown ca` on the target,
> `master_link_status:down`). Therefore **the source must also trust the
> target's CA**:
> - the simplest option — a shared root CA for the client certificates of
>   both clusters; or
> - add the target's CA to the source cluster's `ca.crt` (on the source
>   side; out of scope for S4 — the source doesn't use `replicateFrom`, its
>   trust is set by the admin of the source region).
>
> This is a symmetric property of mTLS, not an S4 bug. If the source really
> runs `tls-auth-clients no` (client certificates not verified) — reverse
> trust isn't needed, and S4 alone is enough.

> **Limitations.** Rotation of the source CA itself is picked up on the
> pod's next restart (the bundle is built in init) — after changing the CA
> on the source, roll the target (annotation
> `valkey.wellcake.io/restart`). `replicateFrom` is only supported for
> Standalone/Replication (not Sentinel/Cluster — see ADR / CRD reference).

## Connecting a client

Service DNS:
- `<cluster>.<ns>.svc.cluster.local:6379` (or `:6380` for TLS).
- For Sentinel, ask Sentinel first:
  `<cluster>-sentinel.<ns>.svc.cluster.local:26379`, command
  `SENTINEL get-master-addr-by-name mymaster`.

Smoke test from a pod:

```sh
kubectl -n <ns> run vk-test --rm -it --image=valkey/valkey:8.0 -- \
  valkey-cli -h <cluster>.<ns> -a "$PASSWORD" PING
```

## Scaling operations

### Replication: changing the number of replicas

```sh
kubectl -n <ns> patch valkeycluster <cluster> --type=merge \
  -p '{"spec":{"replicas":5}}'
```

The StatefulSet expands; the failover loop adapts the topology.

### Cluster: scale-up (add replicas or shards)

```sh
kubectl -n <ns> patch valkeycluster <cluster> --type=merge \
  -p '{"spec":{"shards":8,"replicasPerShard":1}}'
```

Sequence:
1. The StatefulSet grows to `shards*(1+replicasPerShard)` pods.
2. Once all are Ready, the operator creates a `<cluster>-scaleup` Job that
   calls `valkey-cli --cluster add-node` for each new ordinal.
3. On success: `Status.LastAppliedReplicas` advances, the Job is deleted.

**Resharding then has to be done manually** to move slots onto the new
shards:

```sh
kubectl -n <ns> exec -it <cluster>-0 -- valkey-cli \
  -a "$PASSWORD" --cluster reshard <cluster>-0:6379 \
  --cluster-from <source-id> \
  --cluster-to <new-id> \
  --cluster-slots <count> \
  --cluster-yes
```

Get the node ID via `valkey-cli -h <cluster>-0 cluster nodes`.

Alternatively: set `spec.autoReshard=true` so the scale-up Job adds a
`valkey-cli --cluster rebalance --cluster-use-empty-masters --cluster-yes`
step and spreads the slots evenly on its own.

### Cluster: scale-down

```sh
kubectl -n <ns> patch valkeycluster <cluster> --type=merge \
  -p '{"spec":{"shards":4,"replicasPerShard":1}}'
```

Sequence:
1. `statefulSetReplicas()` holds the STS at the old size — the operator
   does not let the StatefulSet delete pods that still own slots.
2. A `<cluster>-scaledown` Job is created:
   - Finds the `primaryNodeID` for each leaving pod.
   - `valkey-cli --cluster reshard --cluster-from <leaving> --cluster-to
     <remaining> --cluster-slots 16384 --cluster-yes` (no-op for an
     already-empty shard).
   - `valkey-cli --cluster del-node` for each leaving ordinal.
3. On success: `Status.LastAppliedReplicas` decreases, the StatefulSet
   shrinks, the Job is deleted.

## Failover scenarios (Replication)

### Primary pod crashes

Expected sequence:

1. Pod-0 dies. The kubelet marks it NotReady.
2. A reconcile fires (Owns(STS) watch event).
3. `surveyReplication` polls all pods (a 2-second timeout each). Pod-0 is
   unreachable; pod-1 and pod-2 are `slave` with
   `master_link_status=down`.
4. `reconcileFailover` picks the replica with the largest
   `slave_repl_offset`, sends `REPLICAOF NO ONE`, and the rest receive
   `REPLICAOF <new-primary>`.
5. Status is updated: `status.primary = <new-pod>`, and the new primary
   appears in the message of the Available condition.
6. The StatefulSet eventually recreates pod-0. Its init container by
   default sets `replicaof <cluster>-0` (i.e. itself!) — the next reconcile
   sees two masters and re-targets the new pod-0 at the real primary.

### Primary pod Running but hung at the application level

The watch-event approach would miss this. The operator requeues every 15s
precisely for this case. After ~15s the survey will time out on the hung
primary and kick off a promotion.

### All pods unreachable (zone failure)

`reconcileFailover` exits early — `reachable < 2`, and takes no action.
When the pods become reachable again, the survey adopts whichever one
turned out to be the master. There is no quorum here; if you need one — use
Sentinel.

## Failover scenarios (Sentinel)

The operator does not intervene. The Sentinel quorum elects a new primary;
clients learn it via `SENTINEL get-master-addr-by-name`. The operator only
maintains the StatefulSets, ConfigMaps, and Services.

## Cluster per-shard workload (ADR 0005, experimental)

With `spec.perShardWorkload: true`, a Cluster is rendered as **one
StatefulSet per shard** (`<cluster>-sh<i>`) instead of a single STS for the
whole cluster. This provides shard-aware anti-affinity (a shard's primary +
replicas on different nodes) and clean shard scale-down (removing a shard =
deleting its STS). The field is **immutable** and **Cluster-only**.

```sh
# pods: <cluster>-sh<i>-<ord>.<cluster>-sh<i>.<ns>.svc.cluster.local
kubectl -n <ns> get sts -l app.kubernetes.io/instance=<cluster>   # one STS per shard
# shard scale-down (perShardWorkload only): removing a shard = deleting its STS
kubectl -n <ns> patch valkeycluster <cluster> --type=merge -p '{"spec":{"shards":3}}'
```

### Migrating a single-STS Cluster → per-shard (blue-green)

`perShardWorkload` is immutable → you can't flip an existing cluster;
migration is via recreate-with-restore (C2):

1. Back up the source cluster to S3 (`spec.backup` / a manual backup Job).
2. Stand up a **new** Cluster with `perShardWorkload: true` + `restoreFrom`
   (per-shard restore, `{shard}` placeholder) — the operator assembles the
   cluster from the backup's slot manifest.
3. Switch clients to the new client Service, tear down the old cluster.

> Experimental: before production — soak + harden the reshard on scale-down
> (interrupted retries can leave an open slot; on a clean cluster it
> succeeds). See ADR 0005.

## Failover scenarios (Cluster)

Handled by native Valkey gossip + auto-failover. The operator only
maintains membership via `add-node`; failover inside the cluster is on its
own.

## Config change → rolling restart

Change `spec.config` (or any field that lands in `valkey.conf`):

```sh
kubectl -n <ns> patch valkeycluster <cluster> --type=merge \
  -p '{"spec":{"config":{"maxmemory-policy":"allkeys-lfu"}}}'
```

Sequence:
1. The reconcile renders a new `valkey.conf` and updates the ConfigMap.
2. `configHashFromData` returns a new sha256.
3. The pod template gets the new value of the
   `valkey.wellcake.io/config-hash` annotation.
4. The StatefulSet UpdateStrategy rolls the pods one at a time.

**Proactive rolling restart (ADR 0004, opt-in).** By default the pods are
rolled by the StatefulSet controller (RollingUpdate); for a Replication
primary this gives a ~15-20s reactive failover window. With the annotation
`valkey.wellcake.io/proactive-rollout: "true"` on the CR, the STS switches
to `OnDelete` and the operator drives the rollout itself one pod at a time,
performing a handover (promote a replica / `CLUSTER FAILOVER` /
`SENTINEL FAILOVER`) **before** restarting the old master — making the
window ~0. Works for Replication/Cluster/Sentinel, default OFF. Enable it:

```sh
kubectl -n <ns> annotate valkeycluster <cluster> valkey.wellcake.io/proactive-rollout=true
```

The password is excluded from the hash — password rotation does NOT roll
the pods and **requires no manual restart**: the operator rotates the
password in place via the annotation (see "Password rotation without a
restart" above).

## Expanding storage

```sh
kubectl -n <ns> patch valkeycluster <cluster> --type=merge \
  -p '{"spec":{"storage":{"size":"20Gi"}}}'
```

CEL only allows values `>=` the old one. **The operator patches the live
PVCs itself** when `spec.storage.size` grows (`ensurePVCSize`): a
StatefulSet's volumeClaimTemplates are immutable, so a size change won't
reach the PVCs via the STS — the operator updates them directly. The
StorageClass must have `allowVolumeExpansion: true`, otherwise the CSI will
reject the PVC expansion.

After expansion, a running pod sees the new size only after a restart on
some CSI drivers — restart it explicitly (`kubectl valkey restart` or a
rolling restart) if you need the space immediately. Shrinking is not
supported (both CEL and the operator only grow).

## Working with Cluster shards

For the Cluster topology, `status.shardDetails` lists each shard with its
primary pod, replicas, slot ranges, and health. The operator updates this
every 30 seconds via `CLUSTER NODES`.

### Viewing the topology

```sh
kubectl -n app-cache get valkeycluster app-cache -o jsonpath='{.status.shardDetails}' | jq
```

Output (abbreviated):

```json
[
  {
    "index": 0,
    "primary": "app-cache-0",
    "primaryNodeID": "aaa11...",
    "replicas": ["app-cache-3"],
    "slotRanges": ["0-5460"],
    "slotCount": 5461,
    "health": "Ready"
  },
  {
    "index": 1,
    "primary": "app-cache-1",
    "primaryNodeID": "bbb22...",
    "replicas": ["app-cache-4"],
    "slotRanges": ["5461-10922"],
    "slotCount": 5462,
    "health": "Degraded"
  }
]
```

Quick health overview:

```sh
kubectl -n app-cache get valkeycluster app-cache \
  -o jsonpath='{range .status.shardDetails[*]}{.index}: {.health} ({.slotCount} slots) primary={.primary} replicas={.replicas}{"\n"}{end}'
```

### Watching the ShardsHealthy condition

```sh
kubectl -n app-cache get valkeycluster app-cache -o jsonpath='{.status.conditions[?(@.type=="ShardsHealthy")]}'
```

When some shard isn't Ready, the condition flips to False with a message
like `unhealthy shards: 2:Degraded, 3:Down`. Wire alerts off of this.

### Finding a shard's node ID for a reshard

`primaryNodeID` is what `valkey-cli --cluster` expects in `--cluster-from`
/ `--cluster-to`. For example, to move 1000 slots from shard 0 to shard 1:

```sh
SRC=$(kubectl -n app-cache get valkeycluster app-cache -o jsonpath='{.status.shardDetails[0].primaryNodeID}')
DST=$(kubectl -n app-cache get valkeycluster app-cache -o jsonpath='{.status.shardDetails[1].primaryNodeID}')
PASSWORD=$(kubectl -n app-cache get secret app-cache-auth -o jsonpath='{.data.password}' | base64 -d)
kubectl -n app-cache exec app-cache-0 -- valkey-cli -a "$PASSWORD" --no-auth-warning \
  --cluster reshard app-cache-0:6379 \
  --cluster-from "$SRC" --cluster-to "$DST" \
  --cluster-slots 1000 --cluster-yes
```

(Or enable `spec.autoReshard=true` for an automatic even rebalance after
scale-up; the reshard above is the manual option for targeted moves.)

### A shard with zero slots after scale-up

If `slotCount=0` on a just-added shard, the cluster accepted it into gossip
but no slots have been handed over yet. Run the reshard above or enable
`spec.autoReshard`.

### Replica lag

Each `ShardStatus` shows `primaryOffset`, `replicaOffsets[]`, and
`maxLagBytes`. Quick look:

```sh
kubectl -n app-cache get valkeycluster app-cache \
  -o jsonpath='{range .status.shardDetails[*]}shard {.index}: maxLag={.maxLagBytes}B (primary {.primaryOffset}){"\n"}{end}'
```

Or the offsets of the individual replicas of a specific shard:

```sh
kubectl -n app-cache get valkeycluster app-cache \
  -o jsonpath='{.status.shardDetails[2].replicaOffsets}' | jq
```

For a deeper investigation (or to check the operator's numbers) — `exec`
into the pod:

```sh
kubectl -n app-cache exec app-cache-0 -- valkey-cli -a "$PASSWORD" --no-auth-warning info replication
```

### Restarting a single shard

`OrderedReady` rollouts don't know about shards — the StatefulSet rolls
pods by ordinal. To restart only the pods of one shard:

```sh
kubectl -n app-cache delete pod app-cache-3 app-cache-7  # primary + its replicas
```

The operator does not intervene in single-pod restarts; cluster gossip
re-elects / reconnects.

## Restore from an S3 backup

Restore is **opt-in at creation time** via `spec.restoreFrom`. The operator
adds an init container that downloads the specified RDB object onto the
data PVC before Valkey starts.

```yaml
apiVersion: cache.wellcake.io/v1alpha1
kind: ValkeyCluster
metadata: { name: web-cache, namespace: web }
spec:
  topology: Replication
  profile: Cache
  replicas: 3
  restoreFrom:
    sourceKey: backups/web-cache-20260528-030000.rdb
    s3:
      bucket: my-backups
      region: us-east-1
      credentialsSecret: web-cache-s3-creds
```

Behavior:
- The init container fires on every pod start but exits immediately if
  `/data/dump.rdb` already exists. So for ordinary restarts it's a no-op;
  only the very first start of an empty PVC downloads the object.
- Replicas get the data from the primary via ordinary replication after the
  RDB is loaded.
- For the Cluster topology — see [Cluster restore](#cluster-restore) below
  (per-shard, partially automated).

### Re-restoring a running cluster

The init step is skipped if dump.rdb is present. To force a re-restore:

```sh
# First scale the STS to 0 so Valkey doesn't hold the file.
kubectl -n <ns> scale sts/<cluster> --replicas=0
# Attach the PVC to a debug pod and delete the file.
kubectl -n <ns> debug -it --image=busybox \
  --target-pvc=data-<cluster>-0 -- rm /data/dump.rdb
# Bring the cluster back — the init container downloads again.
kubectl -n <ns> scale sts/<cluster> --replicas=3
```

<a id="cluster-restore"></a>
### Cluster restore (per-shard)

Cluster restore is **partially automated**: the operator lays out the
per-shard RDBs onto the masters via restore-init, but **assembling the
cluster** (slot assignment, gossip, binding replicas) is done by a human
operator — `--cluster create` cannot be run on non-empty nodes (it aborts).
Full automation of the assembly is a follow-up (C2).

**Requirements:** the number of `shards` in the target cluster **must
match** the snapshot (slot ranges are assigned deterministically in master
order). A Cluster backup writes files like
`<prefix><cluster>-<stamp>-shard-<i>.rdb`.

**1. Create the cluster with `restoreFrom` and the `{shard}` placeholder:**

```yaml
spec:
  topology: Cluster
  shards: 6            # == number of shards in the snapshot
  replicasPerShard: 1
  restoreFrom:
    sourceKey: backups/web-20260528-030000-shard-{shard}.rdb
    s3: { bucket: my-backups, region: us-east-1, credentialsSecret: web-s3-creds }
```

What the operator does automatically:
- restore-init on pod-`i` (i < shards, i.e. a master) downloads
  `…-shard-i.rdb` → `/data/dump.rdb`; replicas (i ≥ shards) skip the
  download (they'll sync from the master after assembly);
- `--cluster create` is **not** run; the CR carries the condition
  `ClusterInitialized=False, Reason=AwaitingManualRestore`.

**2. Wait for all pods to be Ready** (data loaded, the cluster not yet
assembled — `CLUSTER INFO` will show `cluster_state:fail`).

**3. Assemble the cluster manually.** Slots are split evenly: for S shards,
master `m` owns `[m*16384/S, (m+1)*16384/S - 1]`. For each master pod-`m`
(port 6379, or 6380 with TLS):

```sh
# assign the slot range and a unique config-epoch
kubectl -n <ns> exec <cluster>-<m> -c valkey -- \
  valkey-cli -a "$PW" --no-auth-warning CLUSTER ADDSLOTSRANGE <lo> <hi>
kubectl -n <ns> exec <cluster>-<m> -c valkey -- \
  valkey-cli -a "$PW" --no-auth-warning CLUSTER SET-CONFIG-EPOCH $((m+1))
```

Bring the nodes into the cluster (MEET with all the others by pod IP/FQDN),
then bind the replicas to their masters:

```sh
# from pod-0, "introduce" all the other pods
kubectl -n <ns> exec <cluster>-0 -c valkey -- \
  valkey-cli -a "$PW" --no-auth-warning CLUSTER MEET <pod-j-IP> 6379   # for each j>0
# on each replica, point at its master (replica ordinal r → master r%shards)
kubectl -n <ns> exec <cluster>-<r> -c valkey -- \
  valkey-cli -a "$PW" --no-auth-warning CLUSTER REPLICATE <master-node-id>
```

**4. Verify and let the operator adopt it.** Once `CLUSTER INFO` on pod-0
shows `cluster_state:ok` and `cluster_slots_assigned:16384`, the operator
will set `ClusterInitialized=True` (Reason `RestoreAssembled`) on the next
reconcile by itself — no manual status patch needed.

> This is a "sharp edge" of DR; automatic assembly (ADDSLOTS/MEET/REPLICATE
> from the operator) is on the roadmap as the C2 follow-up.

## Deleting a cluster

```sh
kubectl -n <ns> delete valkeycluster <cluster>
```

First the finalizer `valkey.wellcake.io/cleanup` fires (currently a no-op
hook reserved for a future graceful Cluster teardown). The StatefulSet and
its pods are reaped by the garbage collector via ownerRefs.

**The operator does NOT delete the PVCs.** This is intentional — to avoid
an unexpected data loss. Delete them manually if needed:

```sh
kubectl -n <ns> delete pvc -l app.kubernetes.io/instance=<cluster>
```

## Operational procedures

### Recovering from Phase=Failed / BootstrapFailed (Op2)

`Phase=Failed` is a terminal state: the operator stops actively
re-initializing the cluster so as not to loop a destructive operation. The
Reason hints at the cause (see Troubleshooting below). General recovery
procedure:

1. **Diagnose — understand why it failed before doing anything:**
   ```sh
   kubectl -n <ns> get valkeycluster <cluster> -o jsonpath='{.status.phase}{" / "}{.status.conditions}'
   kubectl -n <ns> get jobs -l app.kubernetes.io/instance=<cluster>
   kubectl -n <ns> logs job/<cluster>-bootstrap   # or -scaleup / -scaledown / -reshard
   ```

2. **Bootstrap (Cluster) failed** (`reason=BootstrapFailed`). The bootstrap
   Job is idempotent on `status.clusterInitialized`, but a Failed Job
   blocks it. After fixing the cause (DNS/NetworkPolicy/TLS/too few nodes —
   see Troubleshooting):
   ```sh
   # delete the failed Job — the operator recreates it on the next reconcile
   kubectl -n <ns> delete job <cluster>-bootstrap
   # nudge the reconcile (optional): touch a harmless annotation
   kubectl -n <ns> annotate valkeycluster <cluster> valkey.wellcake.io/kick="$(date +%s)" --overwrite
   ```
   `clusterInitialized` is still `false` → the operator sees all pods Ready
   and runs the bootstrap again. No data is lost (the RDB/AOF on the PVCs is
   preserved).

3. **Scale-up/down/reshard Job failed.** Same steps: fix the cause, delete
   the relevant Job (`<cluster>-scaleup` / `-scaledown` / `-reshard`); the
   operator recreates it. Scale-down holds the StatefulSet at the old size
   until the reshard-away finishes — the leaving masters' data is not lost
   until a successful del-node.

4. **If the cluster is intact but the status is stuck** (e.g. after manual
   intervention): `clusterInitialized` is the source of truth for "don't
   run --cluster create again". Do NOT reset it on a running cluster (it
   leads to a re-initialization attempt; on non-empty nodes `--cluster
   create` will fail — no loss, but BootstrapFailed). For cluster-restore
   the behavior is special — see [Cluster restore](#cluster-restore).

5. **PVCs survive a CR delete** (the operator doesn't touch them). A full
   from-scratch re-initialization = delete the CR, then `kubectl -n <ns>
   delete pvc -l app.kubernetes.io/instance=<cluster>`, then recreate the
   CR. **Only with deliberate data loss.**

### Upgrading the operator (Op3)

`helm upgrade` (see [Install / upgrade](#install--upgrade)) only changes
the controller Deployment — existing `ValkeyCluster`/`ValkeyACL` CRs and
their StatefulSets/pods are **not recreated** when the operator image is
upgraded:

- **The data plane is untouched.** On start the new controller reconciles
  all CRs; the Valkey pods are rolled only if the render changed
  (config-hash) — for example, a new version changed `valkey.conf`/the init
  script. This is a routine one-pod-at-a-time rolling restart, and the PDB
  protects the quorum. Check the `CHANGELOG` for render changes before
  upgrading durable clusters.
- **CRDs.** The chart places CRDs in `crds/` (Helm does NOT upgrade them
  automatically!). When the API changes, apply the CRDs separately:
  ```sh
  kubectl apply -f charts/valkey-operator/crds/
  ```
  As of the v1beta1 CRD it became multi-version (v1alpha1 served + v1beta1
  stored); old v1alpha1 objects are read and written transparently
  (strategy `None`, the schemas are still identical — see ROADMAP E1).
- **Leader election.** With >1 controller replica, an upgrade is a rolling
  Deployment; the new leader is acquired via the lease, and the data plane
  does not depend on this window.
- **Rollback.** `helm rollback valkey-operator` restores the previous
  image; CRs/data are not affected. Rolling back the CRD schema is manual
  and careful (only if the new version hasn't yet written objects in the
  new format).

### Scaling the operator to 500+ installations (Op4)

With a large number of `ValkeyCluster` CRs, a single operator instance on
defaults hits the serialization of reconciles and the cache size. What to
tune (from the second design review, Q3 / ROADMAP SC1–SC3):

- **`maxConcurrentReconciles`** (Helm value, flag
  `--max-concurrent-reconciles`, default 1). Raise it for a large fleet
  (≈5 for 500+ CRs). controller-runtime never reconciles a single object in
  parallel, so a value >1 parallelizes only **different** clusters — it's
  safe. (SC2)
- **Namespace sharding.** Run N operator instances, each with its own
  `WATCH_NAMESPACE` (comma-separated), to split the fleet by team
  namespaces. The manager's cache is then scoped to those namespaces.
- **Sizing.** The default (50m/128Mi req, 500m/256Mi lim) is sized for
  dozens of CRs; for 500+, budget ~500m CPU / 512Mi RAM (via the Helm
  `resources`).
- **The pod cache is already narrowed** to its own pods (label
  `managed-by=valkey-operator`, SC1) — the informer doesn't cache another
  cluster's pods.
- **Sentinel for critical workloads.** The Sentinel topology doesn't load
  the operator with TCP polling (the operator failover loop is disabled,
  failover happens in the data plane), which lowers the load at scale and
  gives failover without depending on the operator's liveness.
- **Multiple operator replicas** (`replicaCount`) — this is HA via leader
  election (cold standby), **not** horizontal load sharding; for sharding,
  use namespaces.

## Troubleshooting

### CR Phase=Failed, reason=NotImplemented

The topology is set to a value that the running operator version doesn't
implement. Sentinel and Cluster were added in 0.1.0; check the `appVersion`
of the installed chart.

### CR Phase=Failed, reason=BootstrapFailed (Cluster)

```sh
kubectl -n <ns> logs job/<cluster>-bootstrap
```

Common causes:
- The pods weren't reachable yet when the bootstrap started. The pre-flight
  loop should prevent this; if you see it — check NetworkPolicy / DNS.
- TLS is misconfigured. The bootstrap uses `--insecure` in the TLS variant;
  the chain must be sufficient for the client to start TLS.
- Not enough nodes for `--cluster-replicas`. You need
  `shards * (1+replicasPerShard)` Ready pods.

### Available=False, reason=Bootstrapping (Cluster)

The bootstrap Job is still running. `kubectl logs job/<cluster>-bootstrap -f`.

### Available=False, reason=ClusterNotReady (ValkeyACL)

The target ValkeyCluster doesn't have `status.Primary` yet. Wait until the
cluster becomes Available; the ACL controller retries every 15s.

### Available=False, reason=PrimaryUnreachable (ValkeyACL)

The operator can dial the pods, but the survey returns nothing. Check the
NetworkPolicy on the namespace — the operator pod must reach the Valkey
pods.

### Pods Pending

```sh
kubectl -n <ns> describe pod <cluster>-0
```

Usually it's PVC binding (no StorageClass / `allowVolumeExpansion`
mismatch) or anti-affinity (too few nodes for the spread constraints).
Override `spec.affinity` or `spec.topologySpreadConstraints` for small
clusters.

### `helm template` fails after a new chart version

The CRDs in `charts/valkey-operator/crds/` are regenerated when the API
changes. If you forked the chart — re-sync them:

```sh
make manifests
cp config/crd/bases/cache.wellcake.io_*.yaml charts/valkey-operator/crds/
```

## Monitoring

Per-cluster metrics via the `redis_exporter` sidecar (enable
`spec.metrics.enabled: true`). Scrape the pod directly or, once
ServiceMonitor support is added, via the Prometheus Operator selector on
the labels `app.kubernetes.io/name=valkey`,
`app.kubernetes.io/instance=<cluster>`.

The operator itself exposes its controller-runtime metrics on a Service
created by the Helm chart (`valkey-operator-metrics:8443`). Plus custom
ones:

| Metric | Type | Description |
| ------ | ---- | ----------- |
| `valkey_operator_reconcile_total{controller,namespace,name,result}` | counter | Reconciles by controller and result. |
| `valkey_operator_failover_total{namespace,cluster,kind}` | counter | Operator-driven failover on Replication. kind: `promotion` / `adoption`. |
| `valkey_operator_cluster_bootstrap_total{namespace,cluster,result}` | counter | Bootstrap Job outcomes. |
| `valkey_operator_cluster_scale_total{namespace,cluster,direction,result}` | counter | Scale Job outcomes (`up`/`down`). |
| `valkey_operator_cluster_shards{namespace,cluster,health}` | gauge | Number of shards by health. |
| `valkey_operator_acl_apply_total{namespace,acl,result}` | counter | ValkeyACL applications. |
