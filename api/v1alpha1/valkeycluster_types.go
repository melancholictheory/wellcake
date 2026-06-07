/*
Copyright 2026 The Wellcake Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// Topology defines the deployment topology of a Valkey cluster.
// +kubebuilder:validation:Enum=Standalone;Replication;Sentinel;Cluster
type Topology string

const (
	TopologyStandalone  Topology = "Standalone"
	TopologyReplication Topology = "Replication"
	TopologySentinel    Topology = "Sentinel"
	TopologyCluster     Topology = "Cluster"
)

// Profile selects the workload profile.
// Cache implies evictable in-memory data backed by emptyDir (no persistence by default).
// Durable implies state-store semantics: PVC-backed, persistence enabled, noeviction.
// The two profiles reflect the roughly even cache/durable split seen in practice.
// +kubebuilder:validation:Enum=Cache;Durable
type Profile string

const (
	ProfileCache   Profile = "Cache"
	ProfileDurable Profile = "Durable"
)

// ValkeyClusterSpec defines the desired state of ValkeyCluster.
// +kubebuilder:validation:XValidation:rule="self.topology != 'Cluster' || has(self.shards)",message="shards is required when topology is Cluster"
// +kubebuilder:validation:XValidation:rule="self.topology == 'Cluster' || !has(self.shards)",message="shards is only valid when topology is Cluster"
// +kubebuilder:validation:XValidation:rule="self.topology != 'Sentinel' || has(self.sentinel)",message="sentinel is required when topology is Sentinel"
// +kubebuilder:validation:XValidation:rule="self.topology == 'Sentinel' || !has(self.sentinel)",message="sentinel is only valid when topology is Sentinel"
// +kubebuilder:validation:XValidation:rule="!has(self.replicateFrom) || self.topology == 'Standalone' || self.topology == 'Replication'",message="replicateFrom only supported for Standalone or Replication topology"
// +kubebuilder:validation:XValidation:rule="!has(self.replicateFrom) || !has(self.replicateFrom.caSecret) || (self.replicateFrom.tls && has(self.tls) && self.tls.enabled)",message="replicateFrom.caSecret requires replicateFrom.tls=true and spec.tls.enabled=true"
// +kubebuilder:validation:XValidation:rule="!has(self.perShardWorkload) || self.topology == 'Cluster'",message="perShardWorkload is only valid when topology is Cluster"
// +kubebuilder:validation:XValidation:rule="!has(self.shards) || !has(oldSelf.shards) || self.shards >= oldSelf.shards || (has(self.perShardWorkload) && self.perShardWorkload)",message="shards can only grow unless perShardWorkload=true (per-StatefulSet layout supports clean shard scale-down; single-STS removes replicas, not a shard — see ADR 0005 / C4)"
// +kubebuilder:validation:XValidation:rule="has(self.perShardWorkload) == has(oldSelf.perShardWorkload) && (!has(self.perShardWorkload) || self.perShardWorkload == oldSelf.perShardWorkload)",message="perShardWorkload is immutable"
type ValkeyClusterSpec struct {
	// Topology selects the deployment shape. Immutable after creation —
	// switching shape requires a new resource because PVCs, layout and
	// bootstrap differ fundamentally.
	// +kubebuilder:default=Replication
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="topology is immutable"
	// +optional
	Topology Topology `json:"topology,omitempty"`

	// Profile selects the workload profile (Cache or Durable). Immutable —
	// changing profile would require re-sizing PVCs and flipping persistence,
	// which is not safe in-place. Drives defaults for persistence, eviction
	// policy and maxmemory headroom.
	// +kubebuilder:default=Cache
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="profile is immutable"
	// +optional
	Profile Profile `json:"profile,omitempty"`

	// Image is the Valkey container image. Defaults to a known stable tag.
	// +kubebuilder:default="valkey/valkey:8.0"
	// +optional
	Image string `json:"image,omitempty"`

	// ImagePullPolicy for the Valkey container.
	// +kubebuilder:default=IfNotPresent
	// +optional
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// Replicas is the number of Valkey instances.
	// For Replication: 1 primary + (Replicas-1) replicas. For Cluster: total nodes (must be >=6 with Shards*ReplicasPerShard).
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// Shards is only used for Cluster topology. Number of shards (primaries).
	// +kubebuilder:validation:Minimum=3
	// +optional
	Shards *int32 `json:"shards,omitempty"`

	// ReplicasPerShard for Cluster topology — replicas per primary.
	// +kubebuilder:validation:Minimum=0
	// +optional
	ReplicasPerShard *int32 `json:"replicasPerShard,omitempty"`

	// PerShardWorkload, when true (Cluster topology only), renders ONE
	// StatefulSet per shard instead of a single StatefulSet for the whole
	// cluster (ADR 0005). Unlocks shard-aware anti-affinity (a shard's
	// primary+replicas on different nodes — not expressible from one shared pod
	// template) and clean shard scale-down (remove a shard = delete its STS).
	// Immutable (see the spec-level rule). EXPERIMENTAL; default (absent →
	// false) keeps the single-StatefulSet path. Mirrors v1beta1 so the field
	// survives v1alpha1↔v1beta1 conversion (the public Crossplane composition
	// writes v1alpha1).
	// +optional
	PerShardWorkload *bool `json:"perShardWorkload,omitempty"`

	// AutoReshard, when true and topology=Cluster, makes scale-up runs also
	// invoke `valkey-cli --cluster rebalance --cluster-use-empty-masters` so
	// new shards receive their share of slots automatically, and makes
	// scale-down rebalance the surviving masters after slots are migrated off
	// the leaving nodes. Defaults to true so a Crossplane/GitOps-driven change
	// to `shards` converges to a balanced, fully-covered cluster with no manual
	// `valkey-cli --cluster reshard`. Set false to keep slot (re)distribution a
	// deliberate manual step.
	// +kubebuilder:default=true
	// +optional
	AutoReshard bool `json:"autoReshard,omitempty"`

	// Sentinel-specific settings (only used when Topology=Sentinel).
	// +optional
	Sentinel *SentinelSpec `json:"sentinel,omitempty"`

	// Resources for the Valkey container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Storage configures persistence backed by PVCs.
	// +optional
	Storage *StorageSpec `json:"storage,omitempty"`

	// Auth configures password authentication.
	// +optional
	Auth *AuthSpec `json:"auth,omitempty"`

	// TLS configures TLS via cert-manager.
	// +optional
	TLS *TLSSpec `json:"tls,omitempty"`

	// Metrics enables the Prometheus exporter sidecar.
	// +optional
	Metrics *MetricsSpec `json:"metrics,omitempty"`

	// Config is a free-form map of Valkey config directives appended to valkey.conf.
	// +optional
	Config map[string]string `json:"config,omitempty"`

	// NodeSelector for Valkey pods.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Tolerations for Valkey pods.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// Affinity for Valkey pods. If nil the operator applies pod anti-affinity by default.
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// TopologySpreadConstraints to spread pods across zones/DCs.
	// If nil the operator applies a default hostname spread across availability zones.
	// +optional
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`

	// PodDisruptionBudget controls voluntary disruptions for the Valkey StatefulSet.
	// Defaults to MaxUnavailable=1 when nil.
	// +optional
	PodDisruptionBudget *PDBSpec `json:"podDisruptionBudget,omitempty"`

	// NetworkPolicy generates a default-deny + allow-from policy if Enabled.
	// +optional
	NetworkPolicy *NetworkPolicySpec `json:"networkPolicy,omitempty"`

	// Backup configures periodic RDB snapshot upload to S3-compatible storage.
	// +optional
	Backup *BackupSpec `json:"backup,omitempty"`

	// RestoreFrom downloads an RDB from S3 into /data/dump.rdb on each pod
	// before Valkey starts, but ONLY when /data/dump.rdb does not already
	// exist. This makes restore a no-op once a cluster has been bootstrapped:
	// to re-restore, delete /data/dump.rdb via a debug pod first.
	//
	// Standalone/Replication/Sentinel: sourceKey is used verbatim on every pod;
	// replicas resync from the primary after it loads the RDB.
	//
	// Cluster: restore is per-shard — sourceKey must contain a "{shard}"
	// placeholder; each master ordinal pulls its own snapshot. The operator does
	// NOT auto-run `valkey-cli --cluster create` in this case (it aborts on
	// non-empty nodes); cluster assembly is a guided manual procedure
	// (docs/runbook.md#cluster-restore), after which the operator detects the
	// formed cluster and marks it initialized.
	// +optional
	RestoreFrom *RestoreSpec `json:"restoreFrom,omitempty"`

	// ReplicateFrom makes this cluster a pull-based asynchronous replica of
	// an external Valkey/Redis primary, usually in another region or another
	// Kubernetes cluster — a DR / read-scaling pattern.
	//
	// When set, every pod (including pod-0) runs `replicaof <host> <port>`
	// at startup, and operator-driven local failover is disabled (the local
	// pods are all replicas of an external source). Promotion on DR is a
	// manual step: clear spec.replicateFrom, the operator then lets pod-0
	// resume primary role and replicas attach to it.
	//
	// Supported topologies: Standalone, Replication.
	// Sentinel and Cluster are unsupported: Sentinel would race the local
	// quorum against the external primary, and OSS Valkey Cluster has no
	// cross-cluster replication semantics.
	// +optional
	ReplicateFrom *ReplicateFromSpec `json:"replicateFrom,omitempty"`
}

// ReplicateFromSpec points at an external primary to replicate from.
type ReplicateFromSpec struct {
	// Host is the external primary FQDN or IP — typically a LoadBalancer
	// service, a Sentinel-discovered address, or a peered ClusterIP.
	// +kubebuilder:validation:MinLength=1
	Host string `json:"host"`

	// Port of the external primary. Defaults to 6379.
	// +kubebuilder:default=6379
	// +optional
	Port int32 `json:"port,omitempty"`

	// PasswordSecret references the source cluster's auth password,
	// used as masterauth on this cluster. If unset, replication is
	// attempted without auth.
	// +optional
	PasswordSecret *SecretKeyReference `json:"passwordSecret,omitempty"`

	// TLS indicates the upstream primary accepts TLS connections. When true,
	// replication frames are encrypted (`tls-replication yes`). The upstream
	// certificate must chain to a CA this cluster trusts. In a multi-region
	// setup where the source has its own CA (different from this cluster's),
	// set caSecret below — the operator then trusts both. If source and target
	// share a CA, leave caSecret empty.
	// +optional
	TLS bool `json:"tls,omitempty"`

	// CASecret references a Secret holding the source cluster's CA bundle (PEM)
	// to verify the upstream primary's TLS cert when it is signed by a separate
	// CA — the multi-region case (S4). The operator merges it with this
	// cluster's own CA into one trust bundle. Key defaults to "ca.crt".
	// +optional
	CASecret *SourceCASecretRef `json:"caSecret,omitempty"`
}

// SourceCASecretRef picks the CA bundle key from a Secret. Mirrors
// SecretKeyReference but defaults Key to "ca.crt".
type SourceCASecretRef struct {
	// Name of the Secret holding the source cluster's CA bundle.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Key in the Secret whose value is the PEM CA bundle. Defaults to "ca.crt".
	// +kubebuilder:default=ca.crt
	// +optional
	Key string `json:"key,omitempty"`
}

// RestoreSpec points the init container at a specific RDB object in S3.
type RestoreSpec struct {
	// S3 destination details. Reuses the same shape as backup.s3, including
	// the credentials Secret. The credentialsSecret may be the same Secret
	// used by Backup, or a separate read-only one.
	// +required
	S3 *S3Spec `json:"s3,omitempty"`

	// SourceKey is the S3 key to download.
	//
	// Single-shard topologies (Standalone/Replication/Sentinel) use it verbatim,
	// e.g. "backups/web-cache-20260528-030000.rdb".
	//
	// For Cluster topology the restore is per-shard: SourceKey must contain a
	// "{shard}" placeholder which each master ordinal (0..shards-1) substitutes
	// with its index, e.g. "backups/web-20260528-030000-shard-{shard}.rdb".
	// Cluster restore requires the same shard count as the snapshot and is a
	// guided manual procedure — see docs/runbook.md#cluster-restore.
	// +kubebuilder:validation:MinLength=1
	SourceKey string `json:"sourceKey"`

	// AWSCLIImage override for the download init container.
	// +kubebuilder:default="amazon/aws-cli:2.15.0"
	// +optional
	AWSCLIImage string `json:"awsCLIImage,omitempty"`
}

// BackupSpec configures a CronJob that runs `valkey-cli --rdb` against the
// primary and uploads the resulting file to S3-compatible storage.
type BackupSpec struct {
	// Enabled toggles the CronJob.
	Enabled bool `json:"enabled,omitempty"`

	// Schedule is a standard cron expression, e.g. "0 3 * * *".
	// +kubebuilder:default="0 3 * * *"
	Schedule string `json:"schedule,omitempty"`

	// S3 holds destination details.
	// +required
	S3 *S3Spec `json:"s3,omitempty"`

	// Image override for the dump step. Defaults to spec.image (Valkey image
	// includes valkey-cli).
	// +optional
	Image string `json:"image,omitempty"`

	// AWSCLIImage override for the upload step.
	// +kubebuilder:default="amazon/aws-cli:2.15.0"
	// +optional
	AWSCLIImage string `json:"awsCLIImage,omitempty"`

	// Retention is the number of most-recent snapshots to keep under the S3
	// prefix. Older snapshots matching `<prefix><cluster>-*.rdb` are deleted
	// after each successful upload. 0 disables retention (keep forever).
	// +kubebuilder:default=7
	// +kubebuilder:validation:Minimum=0
	// +optional
	Retention int32 `json:"retention,omitempty"`
}

// S3Spec points at an S3 (or S3-compatible, e.g. MinIO/Ceph) bucket.
type S3Spec struct {
	// Bucket name.
	// +kubebuilder:validation:MinLength=1
	Bucket string `json:"bucket"`

	// Endpoint URL for S3-compatible services. Leave empty for AWS S3.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// Region, defaults to "us-east-1" for AWS / empty for many MinIO setups.
	// +kubebuilder:default="us-east-1"
	// +optional
	Region string `json:"region,omitempty"`

	// Prefix is prepended to the object key. The final key is
	//   <prefix>/<cluster-name>-YYYYMMDD-HHMMSS.rdb
	// +optional
	Prefix string `json:"prefix,omitempty"`

	// CredentialsSecret references a Secret with keys
	// AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
	// +kubebuilder:validation:MinLength=1
	CredentialsSecret string `json:"credentialsSecret"`

	// Encryption selects server-side encryption for uploads.
	//   AES256 — SSE-S3, AWS-managed keys (no extra setup, no extra cost)
	//   KMS    — SSE-KMS, customer-controlled key in KMSKeyID (translated to
	//            "aws:kms" when invoking the AWS CLI).
	// Empty disables SSE on upload. Reading SSE-S3 objects is transparent;
	// reading SSE-KMS objects requires the same KMS permissions on the
	// reading principal (so the restore Secret must have kms:Decrypt on the
	// key).
	// +kubebuilder:validation:Enum=AES256;KMS
	// +optional
	Encryption string `json:"encryption,omitempty"`

	// KMSKeyID is the KMS key ARN/ID/alias when Encryption=aws:kms.
	// +optional
	KMSKeyID string `json:"kmsKeyId,omitempty"`
}

// PDBSpec configures a PodDisruptionBudget for the cluster.
type PDBSpec struct {
	// Enabled toggles the PDB. Defaults to true.
	// +kubebuilder:default=true
	Enabled bool `json:"enabled,omitempty"`

	// MaxUnavailable, mutually exclusive with MinAvailable. Default "1".
	// +optional
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`

	// MinAvailable, mutually exclusive with MaxUnavailable.
	// +optional
	MinAvailable *intstr.IntOrString `json:"minAvailable,omitempty"`
}

// NetworkPolicySpec configures network access to Valkey pods.
type NetworkPolicySpec struct {
	// Enabled toggles NetworkPolicy generation. Defaults to false (opt-in for now).
	Enabled bool `json:"enabled,omitempty"`

	// AllowFrom is a list of pod/namespace selectors allowed to reach Valkey ports.
	// If empty, only pods in the same namespace are allowed.
	// +optional
	AllowFrom []networkingv1.NetworkPolicyPeer `json:"allowFrom,omitempty"`
}

// StorageSpec configures PVC-backed persistence.
type StorageSpec struct {
	// Size of each PVC, e.g. "10Gi". Expand-only: the value may grow but never
	// shrink (PVC expansion in k8s only works upwards anyway, and only if the
	// StorageClass has AllowVolumeExpansion=true).
	// +kubebuilder:default="10Gi"
	// +kubebuilder:validation:XValidation:rule="quantity(self).compareTo(quantity(oldSelf)) >= 0",message="storage size cannot shrink"
	Size resource.Quantity `json:"size,omitempty"`

	// StorageClassName for the PVC. Immutable: changing the StorageClass on an
	// existing StatefulSet has no effect on already-bound PVCs and can lead to
	// inconsistent storage between pod ordinals.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="storageClassName is immutable"
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`

	// Persistence mode: rdb, aof, both, none.
	// +kubebuilder:validation:Enum=rdb;aof;both;none
	// +kubebuilder:default=rdb
	// +optional
	Mode string `json:"mode,omitempty"`
}

// AuthSpec configures password authentication via Secret.
type AuthSpec struct {
	// Enabled toggles requirepass.
	// +kubebuilder:default=true
	Enabled bool `json:"enabled,omitempty"`

	// ExistingSecret holds the password. If empty, the operator generates one.
	// The secret must contain key "password".
	// +optional
	ExistingSecret string `json:"existingSecret,omitempty"`
}

// TLSSpec wires cert-manager certificates into Valkey.
type TLSSpec struct {
	// Enabled toggles TLS for client and replication traffic.
	Enabled bool `json:"enabled,omitempty"`

	// IssuerRef points at a cert-manager Issuer or ClusterIssuer.
	// +optional
	IssuerRef *CertIssuerRef `json:"issuerRef,omitempty"`

	// ExistingSecret is a pre-created secret containing tls.crt, tls.key, ca.crt.
	// Either IssuerRef or ExistingSecret must be set when Enabled=true.
	// +optional
	ExistingSecret string `json:"existingSecret,omitempty"`

	// MutualTLS, when true, requires clients to present a certificate signed by
	// the cluster CA (`tls-auth-clients yes`). Default false keeps client certs
	// optional (`tls-auth-clients optional`) — pod-to-pod replication always
	// uses the mounted cert regardless. Set true to enforce mTLS, as the
	// platform DBaaS contract does.
	// +optional
	MutualTLS bool `json:"mutualTLS,omitempty"`
}

// CertIssuerRef references a cert-manager issuer.
type CertIssuerRef struct {
	Name string `json:"name"`
	// +kubebuilder:default=Issuer
	// +kubebuilder:validation:Enum=Issuer;ClusterIssuer
	Kind string `json:"kind,omitempty"`
	// +optional
	Group string `json:"group,omitempty"`
}

// MetricsSpec configures the Prometheus exporter sidecar.
type MetricsSpec struct {
	// Enabled toggles the exporter sidecar.
	Enabled bool `json:"enabled,omitempty"`

	// Image of the exporter.
	// +kubebuilder:default="oliver006/redis_exporter:v1.62.0"
	// +optional
	Image string `json:"image,omitempty"`

	// ServiceMonitor creates a Prometheus Operator ServiceMonitor.
	// +optional
	ServiceMonitor bool `json:"serviceMonitor,omitempty"`
}

// SentinelSpec configures Sentinel topology specifics.
type SentinelSpec struct {
	// Replicas is the number of Sentinel pods. Quorum is Replicas/2 + 1.
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=3
	Replicas int32 `json:"replicas,omitempty"`

	// Quorum override. If zero, computed as Replicas/2 + 1.
	// +optional
	Quorum int32 `json:"quorum,omitempty"`

	// Image of the Sentinel container. Defaults to the same image as Valkey.
	// +optional
	Image string `json:"image,omitempty"`
}

// ValkeyClusterPhase reflects high-level lifecycle state.
// +kubebuilder:validation:Enum=Pending;Creating;Ready;Updating;Failed;Hibernated
type ValkeyClusterPhase string

const (
	PhasePending    ValkeyClusterPhase = "Pending"
	PhaseCreating   ValkeyClusterPhase = "Creating"
	PhaseReady      ValkeyClusterPhase = "Ready"
	PhaseUpdating   ValkeyClusterPhase = "Updating"
	PhaseFailed     ValkeyClusterPhase = "Failed"
	PhaseHibernated ValkeyClusterPhase = "Hibernated"
)

// ValkeyClusterStatus defines the observed state of ValkeyCluster.
type ValkeyClusterStatus struct {
	// Phase is a quick high-level summary.
	// +optional
	Phase ValkeyClusterPhase `json:"phase,omitempty"`

	// ReadyReplicas is the count of ready Valkey pods.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// Primary holds the pod currently acting as primary (Replication/Sentinel topologies).
	// +optional
	Primary string `json:"primary,omitempty"`

	// InternalEndpoint is the in-cluster client endpoint (host:port) of the
	// client Service, for consumers to connect to. Surfaced here so wrappers
	// (e.g. a Crossplane Composition) can project it without recomputing names.
	// +optional
	InternalEndpoint string `json:"internalEndpoint,omitempty"`

	// Shards reports the configured number of shards (Cluster topology only).
	// +optional
	Shards int32 `json:"shards,omitempty"`

	// ReadyShards reports how many shards have a Ready primary (Cluster topology only).
	// +optional
	ReadyShards int32 `json:"readyShards,omitempty"`

	// ClusterInitialized is true once `valkey-cli --cluster create` succeeded
	// for Cluster topology. The bootstrap is then never repeated.
	// +optional
	ClusterInitialized bool `json:"clusterInitialized,omitempty"`

	// LastAppliedReplicas is the total replica count last successfully
	// reflected in the running cluster (Cluster topology). Drives scale-up
	// detection: when totalReplicas(spec) > LastAppliedReplicas the operator
	// runs a one-shot add-node Job, then advances this counter.
	// +optional
	LastAppliedReplicas int32 `json:"lastAppliedReplicas,omitempty"`

	// ShardDetails reports per-shard topology, slot ownership and health
	// (Cluster topology only). Refreshed by the reconciler on every pass
	// via `CLUSTER NODES`. Empty when topology != Cluster or the cluster
	// has not bootstrapped yet.
	// +optional
	// +listType=map
	// +listMapKey=index
	ShardDetails []ShardStatus `json:"shardDetails,omitempty"`

	// ObservedGeneration is the .metadata.generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// LastReshardToken is the value of the valkey.wellcake.io/reshard annotation the
	// operator last acted on (Cluster topology). Used to run a manual reshard
	// exactly once per request.
	// +optional
	LastReshardToken string `json:"lastReshardToken,omitempty"`

	// LastFailoverToken is the value of the valkey.wellcake.io/failover annotation
	// the operator last acted on (Replication topology). Used to run a manual
	// failover exactly once per request.
	// +optional
	LastFailoverToken string `json:"lastFailoverToken,omitempty"`

	// Conditions represent the current state of the resource.
	// Well-known types: Available, Progressing, Degraded, ClusterInitialized.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// ShardStatus describes a single shard (one primary plus its replicas) in a
// Cluster-topology ValkeyCluster.
type ShardStatus struct {
	// Index is the operator-assigned shard ordinal, stable across reconciles.
	// It is the order in which we encountered the master in `CLUSTER NODES`,
	// not a Valkey-internal identifier.
	Index int32 `json:"index"`

	// Primary is the pod name (e.g. "demo-3") that owns this shard's slots.
	Primary string `json:"primary"`

	// PrimaryNodeID is the Valkey-internal cluster node ID of the primary.
	// Useful for `valkey-cli --cluster reshard --cluster-from <id>` operations.
	// +optional
	PrimaryNodeID string `json:"primaryNodeID,omitempty"`

	// Replicas lists pod names that replicate this primary.
	// +optional
	Replicas []string `json:"replicas,omitempty"`

	// SlotRanges enumerates the slot ranges owned by the primary, e.g.
	// ["0-5460", "12000-12100"]. A shard with zero slots (freshly added by
	// scale-up but not yet reshard'ed) shows an empty list.
	// +optional
	SlotRanges []string `json:"slotRanges,omitempty"`

	// SlotCount is the total slot count owned by this shard.
	// +optional
	SlotCount int32 `json:"slotCount,omitempty"`

	// Health summarises the shard state: Ready, Degraded (primary up, some
	// replica down), Down (primary unreachable). Unknown when the survey
	// couldn't talk to any pod.
	// +kubebuilder:validation:Enum=Unknown;Ready;Degraded;Down
	// +optional
	Health string `json:"health,omitempty"`

	// PrimaryOffset is the primary's master_repl_offset at the last survey,
	// reported by INFO replication. Useful as a freshness signal and as the
	// baseline for replica lag below.
	// +optional
	PrimaryOffset int64 `json:"primaryOffset,omitempty"`

	// ReplicaOffsets enumerates each replica's repl offset alongside the
	// primary's, so lag can be calculated per replica.
	// +optional
	ReplicaOffsets []ReplicaOffset `json:"replicaOffsets,omitempty"`

	// MaxLagBytes is the largest (primaryOffset - replicaOffset) across the
	// shard's replicas. Equals 0 when there are no replicas or every replica
	// matches the primary exactly.
	// +optional
	MaxLagBytes int64 `json:"maxLagBytes,omitempty"`
}

// ReplicaOffset records one replica's repl offset alongside its pod name.
type ReplicaOffset struct {
	Pod    string `json:"pod"`
	Offset int64  `json:"offset"`
}

// Condition type constants.
const (
	// ConditionReady is the kstatus-compatible readiness signal. It is True iff
	// the cluster is fully serving (phase Ready). Tooling that derives readiness
	// generically — Crossplane's function-auto-ready, `kubectl wait
	// --for=condition=Ready`, kstatus — keys off this type, so it mirrors the
	// Available status while keeping Available for the human-facing message.
	ConditionReady              = "Ready"
	ConditionAvailable          = "Available"
	ConditionProgressing        = "Progressing"
	ConditionDegraded           = "Degraded"
	ConditionClusterInitialized = "ClusterInitialized"
	ConditionShardsHealthy      = "ShardsHealthy"
)

// Shard health constants.
const (
	ShardHealthReady    = "Ready"
	ShardHealthDegraded = "Degraded"
	ShardHealthDown     = "Down"
	ShardHealthUnknown  = "Unknown"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=vk;vkc
// +kubebuilder:printcolumn:name="Topology",type=string,JSONPath=`.spec.topology`
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ValkeyCluster is the Schema for the valkeyclusters API.
type ValkeyCluster struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec ValkeyClusterSpec `json:"spec"`

	// +optional
	Status ValkeyClusterStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ValkeyClusterList contains a list of ValkeyCluster.
type ValkeyClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []ValkeyCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ValkeyCluster{}, &ValkeyClusterList{})
}
