# Topologies

`ValkeyCluster.spec.topology` selects one of four shapes. The field is immutable
after creation — changing the shape means a new resource.

## Standalone

A single Valkey pod. No replication, no failover, no HA. Useful only for
dev/test workloads where downtime is acceptable.

- StatefulSet replicas: 1
- Failover loop: skipped
- Bootstrap: none

```yaml
apiVersion: cache.wellcake.io/v1alpha1
kind: ValkeyCluster
metadata:
  name: dev-cache
spec:
  topology: Standalone
  profile: Cache
```

## Replication

One primary + N-1 replicas in a single StatefulSet.

- StatefulSet replicas: `spec.replicas` (default 3)
- pod-0 starts as the primary; replicas point at it via `replicaof`,
  written by an init container.
- **Operator-driven failover** every 15 seconds: a TCP survey of all pods,
  promoting the replica with the largest offset via `REPLICAOF NO ONE` if the
  primary is unreachable, then `REPLICAOF <new>` on the rest. Status.Primary
  reflects the surveyed primary, not "pod-0 forever".
- Anti-affinity by hostname, topology spread by zone, and a PDB
  (maxUnavailable=1) by default.

```yaml
apiVersion: cache.wellcake.io/v1alpha1
kind: ValkeyCluster
metadata:
  name: web-cache
spec:
  topology: Replication
  profile: Cache
  replicas: 3
```

### Failover behavior

| Scenario                              | Operator action                                 |
| ------------------------------------- | ----------------------------------------------- |
| All pods reachable, one master        | Adopt; status.Primary = master pod name         |
| Primary unreachable, replicas alive   | Promote the replica with the max offset; retarget the rest |
| All pods unreachable                  | status.Primary unchanged; reconcile retries     |
| pod-0 restarts with empty data        | The init script sets `replicaof pod-0` (on itself); the survey sees two masters and retargets pod-0 onto the real primary on the next pass |

There is no quorum here — the operator makes the decision itself. If you need
quorum-based failover, use **Sentinel** below.

## Sentinel

Replication plus a separate StatefulSet of sentinel pods that monitor the
primary and elect a new one on failure.

- Main StatefulSet: as in Replication (`spec.replicas`).
- Sentinel StatefulSet: `spec.sentinel.replicas` (minimum 3, odd
  recommended).
- Sentinel quorum: `spec.sentinel.quorum` or `replicas/2 + 1`.
- Operator-driven failover is **disabled** — the Sentinel quorum is
  authoritative.
- Clients must use a Sentinel-aware library
  (`SENTINEL get-master-addr-by-name mymaster`).
- Sentinel pods own a small (1Gi) PVC for state — Sentinel
  rewrites its config on failover and needs writable storage.

```yaml
apiVersion: cache.wellcake.io/v1alpha1
kind: ValkeyCluster
metadata:
  name: session-store
spec:
  topology: Sentinel
  profile: Durable
  replicas: 3
  sentinel:
    replicas: 3
    quorum: 2
```

Services:
- `<name>` — client Service on the Valkey data port.
- `<name>-headless` — headless Service for the Valkey pods.
- `<name>-sentinel` — headless Service for the Sentinel pods on 26379.

## Cluster

Sharded Valkey with native cluster mode (16384 slots, gossip).

- StatefulSet replicas: `shards * (1 + replicasPerShard)`.
- All pods in a single StatefulSet; gossip on the bus port 16379.
- Bootstrap: a one-shot Job `<name>-bootstrap` runs
  `valkey-cli --cluster create ... --cluster-replicas <perShard> --cluster-yes`
  once all pods are Ready. It sets `status.ClusterInitialized=true`
  on success.
- Scale-up: a one-shot Job `<name>-scaleup` runs `add-node` for each
  new ordinal when
  `totalReplicas(spec) > status.LastAppliedReplicas`. With `autoReshard: true`
  (the default) it then runs `--cluster rebalance --cluster-use-empty-masters`,
  so new shards immediately receive slots. With `autoReshard: false` the new
  masters stay with 0 slots until a manual reshard.
- Scale-down: a one-shot Job `<name>-scaledown` (when
  `totalReplicas(spec) < status.LastAppliedReplicas`) first reshards the slots
  of the departing masters onto the remaining ones, then runs `del-node`. The
  StatefulSet stays at the old size (`statefulSetReplicas()`) until the Job
  succeeds — otherwise a pod still owning slots would be deleted with data loss.
  With `autoReshard: true` it runs `--cluster rebalance` at the end so the
  remaining masters are balanced.
- `cluster-require-full-coverage` follows the Profile: `yes` for Durable
  (any unreachable slot blocks writes), `no` for Cache (degraded
  operation is acceptable).
- `cluster-announce-hostname` and `cluster-preferred-endpoint-type
  hostname` are set so gossip uses stable DNS rather than
  ephemeral pod IPs.

```yaml
apiVersion: cache.wellcake.io/v1alpha1
kind: ValkeyCluster
metadata:
  name: shard-cache
spec:
  topology: Cluster
  profile: Cache
  shards: 6
  replicasPerShard: 1
  image: valkey/valkey:8.0   # cluster mode is supported since 7.x; 8.x baseline
  resources:
    requests: { cpu: 1, memory: 8Gi }
    limits:   { cpu: 2, memory: 16Gi }
```

### Operational notes

- `kubectl get valkeycluster shard-cache -o yaml` shows
  `status.shards`, `status.readyShards`, `status.clusterInitialized`,
  `status.lastAppliedReplicas` and the `status.shardDetails[]` array per shard
  with `primary`, `primaryNodeID`, `replicas[]`, `slotRanges[]`,
  `slotCount` and `health` (Ready/Degraded/Down).
- Monitor `condition.ShardsHealthy` — it flips to False with the message
  `unhealthy shards: 2:Degraded, 3:Down` if any shard is not Ready.
- See [runbook.md → Working with Cluster shards](runbook.md#working-with-cluster-shards)
  for kubectl examples and targeted reshard commands.
- With `autoReshard: true` (the default) scale-up/scale-down converge to a
  balanced, covered cluster on their own — this is the mode for Crossplane/GitOps,
  where changing `shards` must work without manual steps.
- If `autoReshard: false` is set, after scale-up the new shard has 0
  slots until a human runs a reshard manually:
  ```
  kubectl exec -it shard-cache-0 -- valkey-cli --cluster reshard \
    shard-cache-0:6379 --cluster-from <source-id> \
    --cluster-to <new-id> --cluster-slots <count> --cluster-yes
  ```

## Choosing a topology

By typical production workloads:

| Topology     | When to use |
| ------------ | ----------- |
| Standalone   | Dev, small caches where downtime is acceptable |
| Replication  | HA needed, but the data fits on a single node |
| Sentinel     | Replication + quorum-based failover |
| Cluster      | Data > one node's memory, sharding needed |
