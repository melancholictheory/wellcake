# CRD Reference

Two CRDs in the `cache.wellcake.io` group. The hub and storage version is `v1beta1`
(`+kubebuilder:storageversion`); `v1alpha1` remains a served spike with round-trip
conversion (the schemas are identical). An upstream Crossplane composition / GitOps
layer may write `v1alpha1`, so new fields are mirrored there as well (otherwise
admission prunes them — a parity fix).

## ValkeyCluster

Short names: `vk`, `vkc`. Printer columns: `Topology`, `Replicas`,
`Ready`, `Phase`, `Age`.

### spec

| Field                         | Type                       | Default        | Notes |
| ----------------------------- | -------------------------- | -------------- | ------- |
| `topology`                    | `Standalone\|Replication\|Sentinel\|Cluster` | `Replication` | **Immutable** (CEL). |
| `profile`                     | `Cache\|Durable`           | `Cache`        | **Immutable** (CEL). Controls persistence/eviction/PVC-size defaults. |
| `image`                       | string                     | `valkey/valkey:8.0` | |
| `imagePullPolicy`             | `Always\|Never\|IfNotPresent` | `IfNotPresent` | |
| `replicas`                    | int32 ≥1                   | 3              | Total pods for Standalone/Replication/Sentinel; ignored for Cluster (computed from shards). |
| `shards`                      | int32 ≥3                   | —              | **Required** when `topology=Cluster`. Forbidden otherwise (CEL). |
| `replicasPerShard`            | int32 ≥0                   | —              | Cluster only. Replicas per primary. |
| `perShardWorkload`            | *bool                      | —              | Cluster only. **Immutable** (CEL). **Experimental, opt-in.** `true` → one StatefulSet + headless Service per shard (`<cluster>-sh<N>`) instead of a single STS. Enables shard-aware anti-affinity (a shard's primary and replicas land on different nodes, hard) and clean shard scale-down (decreasing `shards` deletes the shard's STS+Service). Absent → single STS. |
| `autoReshard`                 | bool                       | `true`         | Cluster only. Auto-rebalances slots on scale-up/scale-down so the cluster converges without a manual reshard. `false` → reshard manually. |
| `sentinel`                    | `SentinelSpec`             | —              | **Required** when `topology=Sentinel`. Forbidden otherwise (CEL). |
| `sentinel.replicas`           | int32 ≥3                   | 3              | Number of Sentinel pods. |
| `sentinel.quorum`             | int32                      | `replicas/2+1` | Quorum for failover. |
| `sentinel.image`              | string                     | inherited      | |
| `resources`                   | k8s ResourceRequirements   | —              | Applied to the Valkey container. `maxmemory` is automatically set to 60% of `limits.memory` unless overridden in `spec.config.maxmemory`. |
| `storage`                     | `StorageSpec`              | —              | |
| `storage.size`                | resource.Quantity          | `10Gi`         | **Expand-only** (CEL). The Cache profile defaults to 1Gi. |
| `storage.storageClassName`    | *string                    | —              | **Immutable** (CEL). |
| `storage.mode`                | `rdb\|aof\|both\|none`     | `rdb`          | **Immutable** (CEL). The kubebuilder default `rdb` is applied by admission; the profile-dependent default (Cache→`none`, Durable→`both`) only kicks in when `mode` is empty. |
| `storage.medium`              | `Disk\|Memory`            | `Disk`         | **Immutable** (CEL). `Memory` is a tmpfs/RAM emptyDir (PoC): no PVC/StorageClass, counts against the pod's memory limit (not ephemeral-storage → not subject to nodefs eviction). Cache only; forbidden with Durable (CEL); persistence is forced to `none`. |
| `auth`                        | `AuthSpec`                 | enabled        | |
| `auth.enabled`                | bool                       | `true`         | Enables `requirepass`. |
| `auth.existingSecret`         | string                     | —              | Secret with a `password` key. If empty, the operator generates one. |
| `tls`                         | `TLSSpec`                  | disabled       | |
| `tls.enabled`                 | bool                       | `false`        | |
| `tls.issuerRef`               | `CertIssuerRef`            | —              | Reference to a cert-manager Issuer/ClusterIssuer. |
| `tls.existingSecret`          | string                     | —              | Pre-created Secret with `tls.crt`, `tls.key`, `ca.crt`. |
| `tls.mutualTLS`               | bool                       | `false`        | `true` → `tls-auth-clients yes` (enforce mTLS); `false` → `optional`. Pod-to-pod always uses a certificate. |
| `metrics`                     | `MetricsSpec`              | disabled       | |
| `metrics.enabled`             | bool                       | `false`        | Adds a `redis_exporter` sidecar on port 9121. |
| `metrics.image`               | string                     | `oliver006/redis_exporter:v1.62.0` | |
| `metrics.serviceMonitor`      | bool                       | `false`        | Creates a ServiceMonitor (Prometheus Operator) via unstructured; if the CRDs are not installed it is simply skipped. |
| `config`                      | map[string]string          | —              | Arbitrary Valkey directives, appended to `valkey.conf`. |
| `nodeSelector` / `tolerations`| standard                   | —              | |
| `affinity`                    | *corev1.Affinity           | hostname anti-affinity | |
| `topologySpreadConstraints`   | []corev1.TopologySpreadConstraint | zone spread, ScheduleAnyway | |
| `podDisruptionBudget`         | `PDBSpec`                  | auto when replicas>1 | |
| `podDisruptionBudget.enabled` | bool                       | `true`         | |
| `podDisruptionBudget.maxUnavailable` / `minAvailable` | IntOrString | `1` / nil | |
| `networkPolicy`               | `NetworkPolicySpec`        | disabled       | |
| `networkPolicy.enabled`       | bool                       | `false`        | |
| `networkPolicy.allowFrom`     | []NetworkPolicyPeer        | same-namespace | Always plus Valkey-to-Valkey. |
| `backup`                      | `BackupSpec`               | disabled       | |
| `backup.enabled`              | bool                       | `false`        | Creates a CronJob that takes an RDB dump and uploads it to S3. |
| `backup.schedule`             | string (cron)              | `0 3 * * *`    | |
| `backup.retention`            | int32 ≥0                   | 7              | Keep N snapshots. For Cluster it is multiplied by `shards`. |
| `backup.image`                | string                     | from `spec.image`| Image for the dump step. |
| `backup.awsCLIImage`          | string                     | `amazon/aws-cli:2.15.0` | Image for the upload step. |
| `backup.s3.bucket`            | string                     | —              | Required when `backup.enabled=true`. |
| `backup.s3.endpoint`          | string                     | —              | For S3-compatible stores (MinIO, Ceph). |
| `backup.s3.region`            | string                     | `us-east-1`    | |
| `backup.s3.prefix`            | string                     | —              | |
| `backup.s3.credentialsSecret` | string                     | —              | Required. Secret with `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. |
| `backup.s3.encryption`        | `AES256\|KMS`              | —              | SSE-S3 or SSE-KMS (mapped internally to `aws:kms`). |
| `backup.s3.kmsKeyId`          | string                     | —              | For `encryption=KMS`. |
| `restoreFrom`                 | `RestoreSpec`              | —              | An init container downloads the RDB onto the PVC before Valkey starts. For Cluster, restore is per-shard: `sourceKey` MUST contain the `{shard}` placeholder (validated by the webhook), and each master ordinal pulls its own snapshot; an automatic `cluster create` is not run, and assembling the cluster is a guided procedure (see the runbook, ROADMAP C2). |
| `restoreFrom.s3.*`            | `S3Spec`                   | —              | Same fields as in backup; may use the same Secret. |
| `restoreFrom.sourceKey`       | string                     | —              | Exact S3 object key. |
| `restoreFrom.awsCLIImage`     | string                     | `amazon/aws-cli:2.15.0` | |
| `replicateFrom`               | `ReplicateFromSpec`        | —              | Makes the cluster an async replica of an external primary. Standalone/Replication only (CEL). |
| `replicateFrom.host`          | string                     | —              | FQDN or IP of the source primary. |
| `replicateFrom.port`          | int32                      | 6379           | |
| `replicateFrom.passwordSecret`| SecretKeyReference         | —              | Source password for `masterauth`. |
| `replicateFrom.tls`           | bool                       | `false`        | Enables `tls-replication yes`. |
| `replicateFrom.caSecret`      | SourceCASecretRef          | —              | CA bundle of the source cluster, used to verify its TLS cert (S4, multi-region). The operator merges it with the local CA into a single trust bundle. |
| `replicateFrom.caSecret.name` | string                     | —              | Name of the Secret holding the source cluster's CA (in the same namespace). |
| `replicateFrom.caSecret.key`  | string                     | `ca.crt`       | Key in the Secret holding the PEM CA bundle. |

#### Validation rules (CEL XValidation)

- `topology` immutable: `self == oldSelf`.
- `profile` immutable: `self == oldSelf`.
- `storage.storageClassName` immutable: `self == oldSelf`.
- `storage.size` expand-only: `quantity(self).compareTo(quantity(oldSelf)) >= 0` (requires k8s 1.29+).
- `storage.mode` immutable: `self == oldSelf`.
- `storage.medium` immutable: `self == oldSelf`; `Memory` is forbidden when `profile=Durable`.
- `perShardWorkload` is immutable and allowed only when `topology=Cluster`.
- `shards` is required iff `topology=Cluster`; it may only **grow**, except when `perShardWorkload=true` (the per-STS layout supports clean shard scale-down; a single STS would remove replicas rather than a shard).
- `sentinel` is required iff `topology=Sentinel`.
- `restoreFrom` is allowed for `topology=Cluster`, but `sourceKey` must contain `{shard}` (validating webhook, not CEL).
- `replicateFrom` is allowed only for `topology=Standalone` or `Replication`.
- `replicateFrom.caSecret` requires `replicateFrom.tls=true` AND `tls.enabled=true`
  (the local pods need their own cert/key to speak `tls-replication`
  outbound).

### S3 encryption

`backup.s3.encryption` accepts:
- `AES256` — SSE-S3 (AWS-managed keys). Reads are transparent.
- `KMS` — SSE-KMS with `kmsKeyId`. Passed internally as `aws:kms`;
  the reading principal needs `kms:Decrypt` for the same key if it is
  needed for a future restore.
- empty — SSE disabled.

### status

| Field                   | Type                         | Notes |
| ----------------------- | ---------------------------- | ------- |
| `phase`                 | `Pending\|Creating\|Ready\|Updating\|Failed\|Hibernated` | Short overview. |
| `readyReplicas`         | int32                        | From the corresponding StatefulSet. |
| `primary`               | string                       | The polled primary pod (Replication/Sentinel topologies). |
| `internalEndpoint`      | string                       | In-cluster endpoint of the client Service (`<name>.<ns>.svc:<port>`). |
| `shards`                | int32                        | Cluster topology. |
| `readyShards`           | int32                        | Cluster topology; computed from shardDetails. |
| `clusterInitialized`    | bool                         | Cluster topology: once true, bootstrap is not run again. |
| `lastAppliedReplicas`   | int32                        | Cluster topology: a gate for scale-up. |
| `primaryDownSince`      | metav1.Time                  | When the operator first observed the primary as unreachable; debounces reactive failover (threshold `failoverDownAfter`=20s). |
| `lastReshardToken`      | string                       | The value of the `valkey.wellcake.io/reshard` annotation processed most recently (Cluster) — a run-once gate. |
| `lastFailoverToken`     | string                       | The value of the `valkey.wellcake.io/failover` annotation processed most recently (Replication) — a run-once gate. |
| `lastPasswordRotationToken` | string                   | The value of the `valkey.wellcake.io/rotate-password` annotation processed most recently — in-place password rotation without a restart, run-once. |
| `lastTLSCertFingerprint`| string                       | SHA-256 of the leaf cert reloaded onto the pods; a mismatch on a cert-manager renewal triggers a live `CONFIG SET tls-cert-file` without a restart. |
| `shardDetails[]`        | []ShardStatus                | Cluster topology only. Refreshed every 30s from `CLUSTER NODES`. |
| `observedGeneration`    | int64                        | The last reconciled `metadata.generation`. |
| `conditions`            | []metav1.Condition           | See below. |

#### ShardStatus

| Field             | Type    | Notes |
| ----------------- | ------- | ------- |
| `index`           | int32   | Operator-assigned shard ordinal, stable across reconciles. |
| `primary`         | string  | Name of the pod that owns the shard's slots. |
| `primaryNodeID`   | string  | Valkey-internal cluster node ID. Use it for `valkey-cli --cluster reshard --cluster-from <id>`. |
| `replicas[]`      | []string| Names of the pods replicating this primary. |
| `slotRanges[]`    | []string| For example `["0-5460", "12000-12100"]`. Empty for a freshly added shard that has not been resharded. |
| `slotCount`       | int32   | Total number of slots. |
| `health`          | enum    | `Ready` / `Degraded` (primary up, replica down) / `Down` (primary unreachable) / `Unknown`. |
| `primaryOffset`   | int64   | `master_repl_offset` from the primary at poll time. |
| `replicaOffsets[]`| []ReplicaOffset | Per-replica `{pod, offset}` from INFO replication. |
| `maxLagBytes`     | int64   | The maximum `(primaryOffset - replicaOffset)` across replicas. 0 if there are no replicas or all have caught up. |

#### Conditions

| Type                 | Status meaning |
| -------------------- | -------------- |
| `Ready`              | kstatus-compatible readiness signal (mirrors `Available`). Consumed by Crossplane auto-ready, `kubectl wait --for=condition=Ready`, and kstatus. |
| `Available`          | Overall readiness. True when the STS (and the Sentinel STS, for that topology) is fully ready. |
| `Progressing`        | Reserved (not yet set). |
| `Degraded`           | Reserved (not yet set). |
| `ClusterInitialized` | True after `valkey-cli --cluster create` succeeds. |
| `ShardsHealthy`      | Cluster only. False with reason `ShardsNotHealthy` and a message like `unhealthy shards: 2:Degraded, 3:Down` if any shard's health is not `Ready`. |

## ValkeyACL

Short name: `vkacl`. Printer columns: `Cluster`, `Users`, `Age`.

### spec

| Field                           | Type                         | Notes |
| ------------------------------- | ---------------------------- | ------- |
| `clusterRef.name`               | string                       | The target ValkeyCluster in the same namespace. |
| `users[]`                       | []ValkeyACLUser              | |
| `users[].name`                  | string ≥1                    | ACL user name. |
| `users[].rules`                 | string                       | A Valkey ACL rules string, for example `"on ~* +@read -@dangerous"`. |
| `users[].passwordSecret.name`   | string                       | Reference to a Secret. |
| `users[].passwordSecret.key`    | string (defaults to `password`) | Key in that Secret. |

If `passwordSecret` is omitted, the user is created with `nopass`
(implying a read-only or restricted command set).

### status

| Field                | Notes |
| -------------------- | ------- |
| `appliedUsers[]`     | Names successfully applied on the last reconcile (excluding `default`). |
| `observedGeneration` | The last reconciled generation. |
| `conditions[]`       | `Available` with reason `Applied` / `ClusterNotReady` / `ClusterMissing` / `PrimaryUnreachable`. |

On each reconcile:

1. Resolves the target `ValkeyCluster`, waits for `status.Primary` and
   `status.ReadyReplicas > 0` (for Cluster, `ClusterInitialized=true`).
2. Connects to the primary (or to each shard for Cluster) over TCP
   (TLS, if enabled on the cluster).
3. For each `spec.users`:
   `ACL SETUSER <name> reset <rules> >password` (or `nopass`).
   The `reset` token is critical — without it ACL SETUSER is additive and accumulates
   drift.
4. For names that were in `status.appliedUsers` but are absent from spec:
   `ACL DELUSER <name>` (`default` is never deleted).
5. `ACL SAVE` (a no-op if aclfile is not configured; only logged).
6. Patches the status.

## Example: a complete Replication CR

```yaml
apiVersion: cache.wellcake.io/v1alpha1
kind: ValkeyCluster
metadata:
  name: api-cache
  namespace: web
spec:
  topology: Replication
  profile: Cache
  replicas: 3
  image: valkey/valkey:8.0
  resources:
    requests: { cpu: 200m, memory: 512Mi }
    limits:   { cpu: 1,    memory: 2Gi   }
  storage:
    size: 2Gi
  auth:
    enabled: true
  tls:
    enabled: true
    issuerRef:
      name: cluster-ca
      kind: ClusterIssuer
  metrics:
    enabled: true
  networkPolicy:
    enabled: true
    allowFrom:
      - podSelector:
          matchLabels:
            app.kubernetes.io/name: api
  config:
    timeout: "0"
    tcp-keepalive: "60"
---
apiVersion: cache.wellcake.io/v1alpha1
kind: ValkeyACL
metadata:
  name: api-cache-users
  namespace: web
spec:
  clusterRef:
    name: api-cache
  users:
    - name: readonly
      rules: "on ~* &* +@read -@dangerous"
      passwordSecret:
        name: api-cache-readonly
    - name: app
      rules: "on ~app:* +@all -flushdb -flushall -debug -shutdown"
      passwordSecret:
        name: api-cache-app
```
