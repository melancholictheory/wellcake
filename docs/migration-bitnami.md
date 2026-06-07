# Migration: bitnami chart → valkey-operator

Step-by-step migration from the two bitnami charts widely used for
self-managed Redis/Valkey to the `ValkeyCluster` CR.

The guide assumes that you have:
- valkey-operator already installed in the cluster (`helm upgrade --install
  valkey-operator charts/valkey-operator -n valkey-system --create-namespace`)
- an S3-compatible store for the intermediate RDB
- enough namespace capacity to run the source and target side by side
  during cutover

If you can afford a brief read-only outage, the migration is simple.
If you can't, see "Online migration via replicateFrom" at the end.

## Inventory: what to migrate

Run this in your namespace to figure out the chart, its topology, and the
data size:

```sh
NS=<your-namespace>
helm -n $NS list -o yaml | yq '.[] | {name: .name, chart: .chart, status: .status}'
kubectl -n $NS get statefulset
kubectl -n $NS get pvc
kubectl -n $NS exec <pod> -- redis-cli -a "$PW" info memory | grep used_memory_human
kubectl -n $NS exec <pod> -- redis-cli -a "$PW" info keyspace
```

Pick the target topology based on what you found:

| bitnami chart                | What it deploys                        | Target topology |
| ---------------------------- | -------------------------------------- | --------------- |
| `bitnami/redis` w/ replication | 1 master + N replicas (opt. Sentinel) | `Replication` (operator-driven failover) or `Sentinel` (if you want a quorum) |
| `bitnami/redis` standalone   | 1 pod                                  | `Standalone`    |
| `bitnami/redis-cluster`      | Sharded cluster (6 nodes by default)   | `Cluster`       |

And the profile:

| Workload signal                                | Profile  |
| ---------------------------------------------- | -------- |
| `maxmemory-policy: allkeys-lru/lfu`, evictions > 0 | `Cache` |
| `maxmemory-policy: noeviction`, AOF enabled    | `Durable`|

## Mapping values

Fields you most likely need to carry over.

### bitnami/redis with replication

```yaml
# bitnami values
architecture: replication
auth.password: "<set>"
master.persistence.size: 10Gi
master.resources.limits.memory: 2Gi
replica.replicaCount: 2
tls.enabled: true
metrics.enabled: true
```

→

```yaml
apiVersion: cache.wellcake.io/v1alpha1
kind: ValkeyCluster
metadata: { name: web-cache, namespace: web }
spec:
  topology: Replication
  profile: Cache               # or Durable depending on usage
  replicas: 3                  # 1 master + 2 replicas → 3
  image: valkey/valkey:8.0
  auth:
    enabled: true
    existingSecret: web-cache-pass   # bring your own; or omit for auto-generation
  tls:
    enabled: true
    issuerRef: { name: cluster-ca, kind: ClusterIssuer }
  storage:
    size: 10Gi
  resources:
    requests: { cpu: 200m, memory: 512Mi }
    limits:   { cpu: 1,    memory: 2Gi }
  metrics:
    enabled: true
```

### bitnami/redis-cluster

```yaml
# bitnami values
cluster.nodes: 6
cluster.replicas: 1
persistence.size: 8Gi
resources.limits.memory: 4Gi
```

→

```yaml
apiVersion: cache.wellcake.io/v1alpha1
kind: ValkeyCluster
metadata: { name: shard-cache, namespace: shard-cache }
spec:
  topology: Cluster
  profile: Cache
  shards: 3                 # 3 primaries
  replicasPerShard: 1       # 1 replica per primary → 6 pods total
  image: valkey/valkey:9.0  # Cluster needs the 9.x branch
  storage:
    size: 8Gi
  resources:
    requests: { cpu: 500m, memory: 1Gi }
    limits:   { cpu: 2,    memory: 4Gi }
  autoReshard: true         # even slot rebalance after scale-up
  metrics: { enabled: true }
```

### Sentinel topology

If the source used Sentinel, the target should be Sentinel too; the
mapping is the same as Replication plus a `sentinel` block:

```yaml
spec:
  topology: Sentinel
  replicas: 3
  sentinel:
    replicas: 3
    quorum: 2
```

## Procedure (offline, with a short read-only window)

Recommended for most workloads. Total downtime is the dump + restore
time, usually from seconds to a few minutes.

### 1. Take a snapshot of the source

Replication / standalone (single shard):

```sh
NS=<source-namespace>
SOURCE_POD=<source-master-pod>
PW=$(kubectl -n $NS get secret <source-secret> -o jsonpath='{.data.redis-password}' | base64 -d)
kubectl -n $NS exec $SOURCE_POD -- redis-cli -a "$PW" --no-auth-warning BGSAVE
# wait for last_save_time to advance
kubectl -n $NS exec $SOURCE_POD -- redis-cli -a "$PW" --no-auth-warning LASTSAVE
kubectl cp $NS/$SOURCE_POD:/data/dump.rdb ./dump.rdb
aws s3 cp ./dump.rdb s3://my-backups/migration/<cluster>-source.rdb
```

For `bitnami/redis-cluster`, take a separate snapshot of each primary
(one RDB per shard); use the same per-shard naming as the operator:
`<cluster>-source-shard-NN.rdb`.

### 2. Freeze writes on the source

Either:
- scale clients down to 0 (preferred, no changes on the Redis side), or
- `CONFIG SET maxmemory-policy noeviction` + `CLIENT PAUSE 60000` if you
  can't stop clients but can stall them for the cutover window.

### 3. Provision the target ValkeyCluster

For Standalone/Replication/Sentinel:

```yaml
apiVersion: cache.wellcake.io/v1alpha1
kind: ValkeyCluster
metadata: { name: web-cache-new, namespace: web }
spec:
  topology: Replication
  profile: Cache
  replicas: 3
  auth:
    enabled: true
    existingSecret: web-cache-pass
  restoreFrom:
    sourceKey: migration/web-cache-source.rdb
    s3:
      bucket: my-backups
      region: us-east-1
      credentialsSecret: s3-creds
```

The operator's `restore` init container downloads the RDB to
`/data/dump.rdb` before Valkey starts; pod-0 loads it on first start;
the replicas resync from pod-0.

For the Cluster topology, the single-RDB path is not supported (a single
bitnami RDB can't be split across shards directly). Use a per-shard
restore (`sourceKey` with a `{shard}` placeholder) plus automatic cluster
assembly — see
[runbook.md → Cluster restore (per-shard)](runbook.md#cluster-restore-per-shard).

### 4. Verify

```sh
TARGET=web-cache-new
kubectl -n web get valkeycluster $TARGET -o jsonpath='{.status}' | jq
# expect Phase=Ready, ReadyReplicas=spec.replicas, primary set
kubectl -n web exec ${TARGET}-0 -- valkey-cli -a "$PW" --no-auth-warning DBSIZE
# compare with the source DBSIZE before the freeze
kubectl -n web exec ${TARGET}-0 -- valkey-cli -a "$PW" --no-auth-warning info replication
# all replicas `online`, `slave_repl_offset` close to the master
```

For Cluster, also check the shards:

```sh
kubectl -n shard-cache get valkeycluster shard-cache-new -o jsonpath='{.status.shardDetails}' | jq
# each shard Health=Ready, slotCount > 0, no missing slot ranges
```

### 5. Traffic switchover

Clients need to point at the new Service. The fastest option is to
rename the Service:

```sh
# Take the old chart's Service out of selection.
kubectl -n web label svc web-cache-master legacy=true --overwrite
# Re-label the target's client Service so it answers on the old DNS name.
# Simpler: leave the new Service in place and update clients to the new
# DNS (web-cache-new.web.svc).
```

Recommended: update the client configuration (env, ConfigMap, ...) to
point at the new Service rather than playing games with renaming
Services — fewer DNS-cache surprises.

### 6. Cleanup

```sh
# Once you're confident:
helm -n web uninstall web-cache
# Decide about the PVCs: bitnami chart PVCs survive uninstall.
kubectl -n web delete pvc -l app.kubernetes.io/instance=web-cache
```

## Online migration via `replicateFrom`

If you can't afford to freeze writes, use `spec.replicateFrom` to make
the target a downstream replica of the source while clients are still
writing to the source.

### 1. Make the source reachable

The target's operator needs to reach the source primary over TCP. The
simplest options:

- Same cluster, same / peered namespace: use the source Service DNS
  directly.
- Different clusters: expose the source via a LoadBalancer or NodePort,
  or set up cross-cluster networking (mesh / VPN).

### 2. Provision the target

```yaml
apiVersion: cache.wellcake.io/v1alpha1
kind: ValkeyCluster
metadata: { name: web-cache-new, namespace: web }
spec:
  topology: Replication
  profile: Cache
  replicas: 3
  auth:
    enabled: true
    existingSecret: web-cache-target-pass
  replicateFrom:
    host: web-cache-master.web.svc.cluster.local   # source primary
    port: 6379
    passwordSecret:
      name: web-cache-source-pass
      key: redis-password
    # tls: true                                     # if the upstream uses TLS
    # caSecret:                                     # if the source is on its own CA (multi-region, S4)
    #   name: web-cache-source-ca                   #   Secret with the source CA bundle (default key ca.crt)
```

Every target pod (including pod-0) connects as a replica to the source
primary. Operator-driven local failover is disabled while
`replicateFrom` is set.

> **Multi-region / cross-Kubernetes (S4).** If the source primary is
> signed by **its own** CA (not the same one as the target), set
> `replicateFrom.caSecret` — a Secret with the source cluster's CA bundle
> in the target's namespace. The operator merges it with the local CA into
> a single trust bundle (Valkey reads a single global
> `tls-ca-cert-file`), so the outgoing TLS link to the source passes
> verification without manually mounting the CA. This requires
> `replicateFrom.tls=true` **and** `tls.enabled=true` on the target (the
> local pods need their own cert/key for outgoing `tls-replication`). If
> the source and target are on the same CA, `caSecret` is not needed.

### 3. Wait for the lag to converge

```sh
kubectl -n web exec web-cache-new-0 -- valkey-cli -a "$TARGET_PW" --no-auth-warning info replication
# slave_repl_offset should approach master_repl_offset; lag → 0
```

### 4. Cutover

Atomically:

```sh
# Briefly stop writes at the application level.
kubectl -n web scale deploy/web-app --replicas=0   # or however you prefer

# Check the last copied bytes:
kubectl -n web exec web-cache-new-0 -- valkey-cli -a "$TARGET_PW" --no-auth-warning info replication \
  | grep -E 'master_link_status|slave_repl_offset'

# Promote the target by removing replicateFrom.
kubectl -n web patch valkeycluster web-cache-new --type=json \
  -p='[{"op":"remove","path":"/spec/replicateFrom"}]'

# The normal operator failover for Replication takes over: pod-0 becomes
# primary (survey now sees a master), the replicas reconnect.

# Point clients at web-cache-new.web.svc and bring them back.
kubectl -n web scale deploy/web-app --replicas=N
```

### 5. Cleanup

Same as in the offline procedure — uninstall the bitnami chart, delete
the old PVCs after the soak.

## Field-mapping cheat sheet

| bitnami values key                 | ValkeyCluster spec                                  |
| ---------------------------------- | --------------------------------------------------- |
| `architecture`                     | `topology` (`replication`→`Replication`)            |
| `auth.password` / `auth.existingSecret` | `auth.existingSecret` (key: `password`)        |
| `master.persistence.size`          | `storage.size`                                      |
| `master.persistence.storageClass`  | `storage.storageClassName`                          |
| `master.resources`                 | `resources`                                         |
| `replica.replicaCount`             | `replicas` − 1 (the operator counts total pods)     |
| `cluster.nodes`                    | `shards * (1 + replicasPerShard)`                   |
| `cluster.replicas`                 | `replicasPerShard`                                  |
| `tls.enabled`                      | `tls.enabled`                                       |
| `metrics.enabled`                  | `metrics.enabled`                                   |
| `metrics.serviceMonitor.enabled`   | `metrics.serviceMonitor`                            |
| `sentinel.enabled`                 | `topology: Sentinel` + `sentinel.replicas`          |
| `sentinel.quorum`                  | `sentinel.quorum`                                   |
| `commonConfiguration` / extra conf | `spec.config` (key/value map)                       |
| `podAntiAffinityPreset`            | enabled by default; override via `spec.affinity`    |
| `pdb.create`                       | always for `replicas>1`; tune `podDisruptionBudget` |

## Common pitfalls

- **Persistence mode.** bitnami defaults to RDB on, AOF off. The
  valkey-operator Cache profile turns persistence off entirely
  (allkeys-lru); Durable turns both on. If you mistakenly pick Durable
  for a cache workload, you'll burn IOPS for nothing. Check the source's
  `maxmemory-policy` — it helps you choose correctly.
- **Cluster source on Redis 7 → target Valkey 9.** Module commands and a
  couple of command options have diverged. Run the application's reads
  against a staging target before the prod cutover.
- **Large keys / long BGSAVE.** On large datasets, `BGSAVE` can stall for
  many seconds because of the fork. The bitnami chart is often untuned;
  if `BGSAVE` triggers an OOM, lower `maxmemory` first (leave room for
  the fork's COW — the same `0.6 × limits.memory` rule the operator
  applies to new clusters).
- **AOF rewrite during cutover.** If the source has AOF on, an in-flight
  AOF rewrite will keep `BGSAVE` queued. Run `BGREWRITEAOF` before the
  freeze to drain the queue.
