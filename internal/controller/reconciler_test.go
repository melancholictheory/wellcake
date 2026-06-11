/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"context"
	"slices"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// newTestScheme registers every API the reconciler touches so the fake
// client can serve typed Get/List/Create/Update calls for them.
func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		cachev1beta1.AddToScheme,
		corev1.AddToScheme,
		appsv1.AddToScheme,
		batchv1.AddToScheme,
		policyv1.AddToScheme,
		networkingv1.AddToScheme,
	} {
		if err := add(s); err != nil {
			t.Fatalf("scheme add: %v", err)
		}
	}
	return s
}

func TestEnsurePVCSizeGrowsLivePVCs(t *testing.T) {
	scheme := newTestScheme(t)
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology: cachev1beta1.TopologyReplication, Replicas: 2,
			Storage: &cachev1beta1.StorageSpec{Size: resource.MustParse("20Gi")},
		},
	}
	// Two live PVCs at the old size, one already-large PVC that must NOT shrink.
	mkPVC := func(name, size string) *corev1.PersistentVolumeClaim {
		return &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", Labels: map[string]string{instanceLabel: "c"}},
			Spec: corev1.PersistentVolumeClaimSpec{Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(size)},
			}},
		}
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(vc, mkPVC("data-c-0", "10Gi"), mkPVC("data-c-1", "10Gi"), mkPVC("data-c-2", "50Gi")).Build()
	r := &ValkeyClusterReconciler{Client: c, Scheme: scheme}

	if err := r.ensurePVCSize(context.Background(), vc); err != nil {
		t.Fatalf("ensurePVCSize: %v", err)
	}
	want := resource.MustParse("20Gi")
	for _, n := range []string{"data-c-0", "data-c-1"} {
		var p corev1.PersistentVolumeClaim
		if err := c.Get(context.Background(), types.NamespacedName{Name: n, Namespace: "ns"}, &p); err != nil {
			t.Fatalf("get %s: %v", n, err)
		}
		if got := p.Spec.Resources.Requests[corev1.ResourceStorage]; got.Cmp(want) != 0 {
			t.Errorf("%s size = %s, want 20Gi", n, got.String())
		}
	}
	// The 50Gi PVC must be left untouched (never shrink).
	var big corev1.PersistentVolumeClaim
	_ = c.Get(context.Background(), types.NamespacedName{Name: "data-c-2", Namespace: "ns"}, &big)
	if got := big.Spec.Resources.Requests[corev1.ResourceStorage]; got.Cmp(resource.MustParse("50Gi")) != 0 {
		t.Errorf("data-c-2 shrunk to %s, must stay 50Gi", got.String())
	}
}

func TestAdoptFormedClusterFlipsInitialized(t *testing.T) {
	// AR2: when a cluster is already formed but the status flag was lost, the
	// operator adopts it (flips ClusterInitialized) instead of re-bootstrapping.
	scheme := newTestScheme(t)
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology: cachev1beta1.TopologyCluster, Shards: ptr.To[int32](3),
		},
		// status lost: ClusterInitialized=false despite a live cluster
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(vc).
		WithStatusSubresource(&cachev1beta1.ValkeyCluster{}).Build()
	r := &ValkeyClusterReconciler{Client: c, Scheme: scheme}

	if _, err := r.adoptFormedCluster(context.Background(), vc, "AdoptedExisting", "adopted"); err != nil {
		t.Fatalf("adoptFormedCluster: %v", err)
	}
	var got cachev1beta1.ValkeyCluster
	if err := c.Get(context.Background(), types.NamespacedName{Name: "c", Namespace: "ns"}, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if !got.Status.ClusterInitialized {
		t.Error("ClusterInitialized not flipped to true on adopt")
	}
	if got.Status.LastAppliedReplicas != 3 {
		t.Errorf("LastAppliedReplicas = %d, want 3 (shards)", got.Status.LastAppliedReplicas)
	}
}

// reconcileUntilStable invokes Reconcile up to N times. The first pass
// usually only attaches the finalizer and returns Requeue=true; the next
// pass performs the actual work. We stop as soon as the result is
// Requeue=false AND RequeueAfter=0, or after the safety cap.
func reconcileUntilStable(t *testing.T, r *ValkeyClusterReconciler, key types.NamespacedName) {
	t.Helper()
	ctx := context.Background()
	const maxPasses = 8 // safety cap; the reconciler converges in 1–2 passes under fake client
	for i := range maxPasses {
		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		if err != nil {
			t.Fatalf("reconcile #%d: %v", i+1, err)
		}
		if res.RequeueAfter == 0 {
			return
		}
	}
}

func TestReconcileReplicationCreatesOwnedObjects(t *testing.T) {
	scheme := newTestScheme(t)
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "rcache", Namespace: "ns"},
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology: cachev1beta1.TopologyReplication,
			Profile:  cachev1beta1.ProfileCache,
			Replicas: 3,
			Auth:     &cachev1beta1.AuthSpec{Enabled: true},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(vc).
		WithStatusSubresource(&cachev1beta1.ValkeyCluster{}).
		Build()
	r := &ValkeyClusterReconciler{Client: c, Scheme: scheme}
	key := types.NamespacedName{Name: vc.Name, Namespace: vc.Namespace}

	reconcileUntilStable(t, r, key)

	// Headless + client Service.
	var headless corev1.Service
	if err := c.Get(context.Background(), types.NamespacedName{Name: "rcache-headless", Namespace: "ns"}, &headless); err != nil {
		t.Fatalf("headless service: %v", err)
	}
	if headless.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Errorf("headless service should be ClusterIP=None, got %q", headless.Spec.ClusterIP)
	}
	var clientSvc corev1.Service
	if err := c.Get(context.Background(), types.NamespacedName{Name: "rcache", Namespace: "ns"}, &clientSvc); err != nil {
		t.Fatalf("client service: %v", err)
	}

	// Generated password Secret.
	var sec corev1.Secret
	if err := c.Get(context.Background(), types.NamespacedName{Name: "rcache-auth", Namespace: "ns"}, &sec); err != nil {
		t.Fatalf("auth secret: %v", err)
	}
	if len(sec.Data["password"]) == 0 {
		t.Errorf("auth secret should have a non-empty password")
	}

	// ConfigMap.
	var cm corev1.ConfigMap
	if err := c.Get(context.Background(), types.NamespacedName{Name: "rcache-config", Namespace: "ns"}, &cm); err != nil {
		t.Fatalf("configmap: %v", err)
	}
	if _, ok := cm.Data["valkey.conf"]; !ok {
		t.Errorf("configmap should contain valkey.conf")
	}

	// StatefulSet, with the config-hash annotation propagated.
	var sts appsv1.StatefulSet
	if err := c.Get(context.Background(), types.NamespacedName{Name: "rcache", Namespace: "ns"}, &sts); err != nil {
		t.Fatalf("statefulset: %v", err)
	}
	if *sts.Spec.Replicas != 3 {
		t.Errorf("STS replicas = %d, want 3", *sts.Spec.Replicas)
	}
	if sts.Spec.Template.Annotations[configHashAnnotation] == "" {
		t.Errorf("STS pod template missing config-hash annotation")
	}

	// PDB (replicas>1).
	var pdb policyv1.PodDisruptionBudget
	if err := c.Get(context.Background(), types.NamespacedName{Name: "rcache-pdb", Namespace: "ns"}, &pdb); err != nil {
		t.Fatalf("pdb: %v", err)
	}

	// No bootstrap Job for non-Cluster topology.
	var jobs batchv1.JobList
	if err := c.List(context.Background(), &jobs, client.InNamespace("ns")); err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs.Items) != 0 {
		t.Errorf("unexpected Jobs in non-Cluster topology: %d", len(jobs.Items))
	}
}

func TestReconcileFinalizerLifecycle(t *testing.T) {
	scheme := newTestScheme(t)
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "f", Namespace: "ns"},
		Spec:       cachev1beta1.ValkeyClusterSpec{Topology: cachev1beta1.TopologyReplication, Replicas: 1},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(vc).
		WithStatusSubresource(&cachev1beta1.ValkeyCluster{}).Build()
	r := &ValkeyClusterReconciler{Client: c, Scheme: scheme}
	key := types.NamespacedName{Name: vc.Name, Namespace: vc.Namespace}

	// First reconcile attaches the finalizer.
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var got cachev1beta1.ValkeyCluster
	if err := c.Get(context.Background(), key, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	found := slices.Contains(got.Finalizers, finalizerName)
	if !found {
		t.Fatalf("finalizer %q not added on first reconcile (got %v)", finalizerName, got.Finalizers)
	}
}

func TestReconcileClusterRequiresShards(t *testing.T) {
	scheme := newTestScheme(t)
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology: cachev1beta1.TopologyCluster,
			// Shards intentionally nil — CEL would reject this in a real
			// cluster, but the reconciler should still fail safely if the
			// admission layer is bypassed (e.g. operator-installed CRDs
			// without webhooks).
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(vc).
		WithStatusSubresource(&cachev1beta1.ValkeyCluster{}).Build()
	r := &ValkeyClusterReconciler{Client: c, Scheme: scheme}
	key := types.NamespacedName{Name: vc.Name, Namespace: vc.Namespace}

	reconcileUntilStable(t, r, key)

	var got cachev1beta1.ValkeyCluster
	if err := c.Get(context.Background(), key, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status.Phase != cachev1beta1.PhaseFailed {
		t.Errorf("Phase = %q, want %q", got.Status.Phase, cachev1beta1.PhaseFailed)
	}
}

func TestReconcileSentinelRequiresSentinelSpec(t *testing.T) {
	scheme := newTestScheme(t)
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "ns"},
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology: cachev1beta1.TopologySentinel,
			// Sentinel field nil — reconciler should refuse with PhaseFailed.
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(vc).
		WithStatusSubresource(&cachev1beta1.ValkeyCluster{}).Build()
	r := &ValkeyClusterReconciler{Client: c, Scheme: scheme}
	key := types.NamespacedName{Name: vc.Name, Namespace: vc.Namespace}

	reconcileUntilStable(t, r, key)

	var got cachev1beta1.ValkeyCluster
	if err := c.Get(context.Background(), key, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status.Phase != cachev1beta1.PhaseFailed {
		t.Errorf("Phase = %q, want %q", got.Status.Phase, cachev1beta1.PhaseFailed)
	}
}

func TestReconcileBackupCreatesCronJob(t *testing.T) {
	scheme := newTestScheme(t)
	// Pre-seed the credentials Secret so the reconciler doesn't reject the
	// backup spec for a missing reference (webhook does that, but in a
	// no-webhook fake-client run we still want the CronJob to materialize).
	creds := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "s3creds", Namespace: "ns"},
		Data: map[string][]byte{
			"AWS_ACCESS_KEY_ID":     []byte("k"),
			"AWS_SECRET_ACCESS_KEY": []byte("s"),
		},
	}
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "ns"},
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology: cachev1beta1.TopologyReplication,
			Replicas: 3,
			Auth:     &cachev1beta1.AuthSpec{Enabled: true},
			Backup: &cachev1beta1.BackupSpec{
				Enabled:  true,
				Schedule: "0 3 * * *",
				S3: &cachev1beta1.S3Spec{
					Bucket:            "my-bucket",
					Region:            "us-east-1",
					CredentialsSecret: "s3creds",
				},
				Retention: 7,
			},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(vc, creds).
		WithStatusSubresource(&cachev1beta1.ValkeyCluster{}).Build()
	r := &ValkeyClusterReconciler{Client: c, Scheme: scheme}
	key := types.NamespacedName{Name: vc.Name, Namespace: vc.Namespace}

	reconcileUntilStable(t, r, key)

	var cron batchv1.CronJob
	if err := c.Get(context.Background(), types.NamespacedName{Name: "b-backup", Namespace: "ns"}, &cron); err != nil {
		t.Fatalf("backup cronjob: %v", err)
	}
	if cron.Spec.Schedule != "0 3 * * *" {
		t.Errorf("CronJob schedule = %q, want %q", cron.Spec.Schedule, "0 3 * * *")
	}

	// The dump init-container must verify the RDB it just wrote, so a corrupt
	// snapshot fails the job before the upload container runs (B1/AR3).
	inits := cron.Spec.JobTemplate.Spec.Template.Spec.InitContainers
	if len(inits) == 0 {
		t.Fatalf("backup CronJob has no init (dump) container")
	}
	dumpScript := inits[0].Command[len(inits[0].Command)-1]
	if !strings.Contains(dumpScript, "valkey-check-rdb /backup/dump.rdb") {
		t.Errorf("dump script does not verify dump.rdb with valkey-check-rdb:\n%s", dumpScript)
	}
}

// TestReconcileClusterBackupCreatesCronJob guards the regression where
// reconcileCluster never called ensureBackupCronJob: a Cluster with
// backup.enabled must materialize a `<name>-backup` CronJob just like the other
// topologies (the Cluster dump script fans out per shard).
func TestReconcileClusterBackupCreatesCronJob(t *testing.T) {
	scheme := newTestScheme(t)
	creds := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "s3creds", Namespace: "ns"},
		Data:       map[string][]byte{"AWS_ACCESS_KEY_ID": []byte("k"), "AWS_SECRET_ACCESS_KEY": []byte("s")},
	}
	shards := int32(3)
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cbk", Namespace: "ns"},
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology: cachev1beta1.TopologyCluster,
			Shards:   &shards,
			Backup: &cachev1beta1.BackupSpec{
				Enabled:  true,
				Schedule: "0 3 * * *",
				S3:       &cachev1beta1.S3Spec{Bucket: "b", Region: "r", CredentialsSecret: "s3creds"},
			},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(vc, creds).
		WithStatusSubresource(&cachev1beta1.ValkeyCluster{}).Build()
	r := &ValkeyClusterReconciler{Client: c, Scheme: scheme}
	reconcileUntilStable(t, r, types.NamespacedName{Name: vc.Name, Namespace: vc.Namespace})

	var cron batchv1.CronJob
	if err := c.Get(context.Background(), types.NamespacedName{Name: "cbk-backup", Namespace: "ns"}, &cron); err != nil {
		t.Fatalf("Cluster backup cronjob not created: %v", err)
	}
}

// TestClusterBackupWritesSlotManifest asserts a Cluster-topology backup records
// each shard's owned slot ranges into a manifest and uploads it alongside the
// per-shard RDBs — the groundwork for automated cluster re-assembly on restore
// (ROADMAP C2): slots live in cluster state, not the RDB, so a restore needs
// this manifest to know which slots each restored shard owns.
func TestClusterBackupWritesSlotManifest(t *testing.T) {
	shards := int32(3)
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cb", Namespace: "ns"},
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology: cachev1beta1.TopologyCluster,
			Shards:   &shards,
			Backup: &cachev1beta1.BackupSpec{
				Enabled:   true,
				Schedule:  "0 3 * * *",
				Retention: 7,
				S3:        &cachev1beta1.S3Spec{Bucket: "bkt", Region: "r", CredentialsSecret: "c"},
			},
		},
	}
	pod := buildBackupCronJob(vc, "cb-backup").Spec.JobTemplate.Spec.Template.Spec
	dump := pod.InitContainers[0].Command[len(pod.InitContainers[0].Command)-1]
	if !strings.Contains(dump, "cluster nodes") || !strings.Contains(dump, "manifest.txt") {
		t.Errorf("cluster dump script must record a slot manifest from cluster nodes:\n%s", dump)
	}
	upload := pod.Containers[0].Command[len(pod.Containers[0].Command)-1]
	if !strings.Contains(upload, "manifest.txt") {
		t.Errorf("upload script must upload the manifest:\n%s", upload)
	}
}

func TestClusterBackupPerShardHosts(t *testing.T) {
	// Per-shard Cluster (ADR 0005): the dump must fan out over the per-shard pod
	// FQDNs (<cluster>-sh<i>-<o>.<cluster>-sh<i>...), not the single-STS ordinals.
	shards := int32(3)
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cb", Namespace: "ns"},
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology:         cachev1beta1.TopologyCluster,
			Shards:           &shards,
			PerShardWorkload: ptr.To(true),
			Backup: &cachev1beta1.BackupSpec{
				Enabled: true, Schedule: "0 3 * * *", Retention: 7,
				S3: &cachev1beta1.S3Spec{Bucket: "bkt", Region: "r", CredentialsSecret: "c"},
			},
		},
	}
	pod := buildBackupCronJob(vc, "cb-backup").Spec.JobTemplate.Spec.Template.Spec
	dump := pod.InitContainers[0].Command[len(pod.InitContainers[0].Command)-1]
	for _, want := range []string{
		"cb-sh0-0.cb-sh0.ns.svc.cluster.local",
		"cb-sh2-0.cb-sh2.ns.svc.cluster.local",
		"for FQDN in $HOSTS",
	} {
		if !strings.Contains(dump, want) {
			t.Errorf("per-shard dump must iterate per-shard FQDNs (%q):\n%s", want, dump)
		}
	}
	if strings.Contains(dump, "cb-0.cb-headless") {
		t.Errorf("per-shard dump must NOT use single-STS FQDNs:\n%s", dump)
	}
}

func TestReconcileDisablingBackupDeletesCronJob(t *testing.T) {
	scheme := newTestScheme(t)
	cron := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{Name: "d-backup", Namespace: "ns"},
		Spec:       batchv1.CronJobSpec{Schedule: "* * * * *"},
	}
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "d", Namespace: "ns"},
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology: cachev1beta1.TopologyReplication,
			Replicas: 1,
			Auth:     &cachev1beta1.AuthSpec{Enabled: true},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(vc, cron).
		WithStatusSubresource(&cachev1beta1.ValkeyCluster{}).Build()
	r := &ValkeyClusterReconciler{Client: c, Scheme: scheme}
	reconcileUntilStable(t, r, types.NamespacedName{Name: "d", Namespace: "ns"})

	var existing batchv1.CronJob
	err := c.Get(context.Background(), types.NamespacedName{Name: "d-backup", Namespace: "ns"}, &existing)
	if !apierrors.IsNotFound(err) {
		t.Errorf("stale backup CronJob should have been deleted, got err=%v", err)
	}
}

func TestReconcileClusterScaleUpHoldThenAdvance(t *testing.T) {
	scheme := newTestScheme(t)
	// LastAppliedReplicas=6 but spec.shards=3 → totalReplicas=3 < 6.
	// The StatefulSet must stay at 6 until the scale-down Job advances
	// LastAppliedReplicas back down. This test only checks the hold side.
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cl", Namespace: "ns"},
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology: cachev1beta1.TopologyCluster,
			Shards:   ptr.To[int32](3),
		},
		Status: cachev1beta1.ValkeyClusterStatus{
			LastAppliedReplicas: 6,
			ClusterInitialized:  true,
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(vc).
		WithStatusSubresource(&cachev1beta1.ValkeyCluster{}).Build()
	r := &ValkeyClusterReconciler{Client: c, Scheme: scheme}

	reconcileUntilStable(t, r, types.NamespacedName{Name: "cl", Namespace: "ns"})

	var sts appsv1.StatefulSet
	if err := c.Get(context.Background(), types.NamespacedName{Name: "cl", Namespace: "ns"}, &sts); err != nil {
		t.Fatalf("statefulset: %v", err)
	}
	if *sts.Spec.Replicas != 6 {
		t.Errorf("STS replicas = %d, want 6 (held during scale-down)", *sts.Spec.Replicas)
	}

	// The reconciler must have launched the scale-down Job that reshards slots
	// away from the leaving masters before the StatefulSet may shrink.
	var scaledown batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{Name: "cl-scaledown", Namespace: "ns"}, &scaledown); err != nil {
		t.Fatalf("scale-down job not created: %v", err)
	}
}

func TestReconcileHibernateScalesToZero(t *testing.T) {
	scheme := newTestScheme(t)
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "h", Namespace: "ns",
			Annotations: map[string]string{"valkey.wellcake.io/hibernate": "true"},
		},
		Spec: cachev1beta1.ValkeyClusterSpec{Topology: cachev1beta1.TopologyReplication, Replicas: 3},
	}
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "h", Namespace: "ns"},
		Spec:       appsv1.StatefulSetSpec{Replicas: ptr.To[int32](3)},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(vc, sts).
		WithStatusSubresource(&cachev1beta1.ValkeyCluster{}).Build()
	r := &ValkeyClusterReconciler{Client: c, Scheme: scheme}
	key := types.NamespacedName{Name: "h", Namespace: "ns"}

	reconcileUntilStable(t, r, key)

	var got appsv1.StatefulSet
	if err := c.Get(context.Background(), key, &got); err != nil {
		t.Fatalf("statefulset: %v", err)
	}
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 0 {
		t.Errorf("hibernated STS replicas = %v, want 0", got.Spec.Replicas)
	}
	var vcGot cachev1beta1.ValkeyCluster
	if err := c.Get(context.Background(), key, &vcGot); err != nil {
		t.Fatal(err)
	}
	if vcGot.Status.Phase != cachev1beta1.PhaseHibernated {
		t.Errorf("phase = %q, want Hibernated", vcGot.Status.Phase)
	}
}

func TestReconcileClusterReshardOnAnnotation(t *testing.T) {
	scheme := newTestScheme(t)
	// A converged 3-shard cluster (initialized, all 3 pods ready) with a
	// manual reshard request whose token hasn't been handled yet.
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "cl", Namespace: "ns",
			Annotations: map[string]string{"valkey.wellcake.io/reshard": "tok-1"},
		},
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology: cachev1beta1.TopologyCluster,
			Shards:   ptr.To[int32](3),
		},
		Status: cachev1beta1.ValkeyClusterStatus{
			ClusterInitialized:  true,
			LastAppliedReplicas: 3,
		},
	}
	// Pre-create the StatefulSet as fully Ready so allReady is true (no kubelet
	// in a fake client). ensureStatefulSet updates spec but preserves status.
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "cl", Namespace: "ns"},
		Spec:       appsv1.StatefulSetSpec{Replicas: ptr.To[int32](3)},
		Status:     appsv1.StatefulSetStatus{ReadyReplicas: 3},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(vc, sts).
		WithStatusSubresource(&cachev1beta1.ValkeyCluster{}).Build()
	r := &ValkeyClusterReconciler{Client: c, Scheme: scheme}
	key := types.NamespacedName{Name: "cl", Namespace: "ns"}

	reconcileUntilStable(t, r, key)

	var reshard batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{Name: "cl-reshard", Namespace: "ns"}, &reshard); err != nil {
		t.Fatalf("reshard job not created on annotation: %v", err)
	}
}

func TestMapSecretToCluster(t *testing.T) {
	scheme := newTestScheme(t)
	const ns = "ns"
	mkVC := func(name string, auth *cachev1beta1.AuthSpec, tls *cachev1beta1.TLSSpec) *cachev1beta1.ValkeyCluster {
		return &cachev1beta1.ValkeyCluster{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec:       cachev1beta1.ValkeyClusterSpec{Auth: auth, TLS: tls},
		}
	}
	authExt := func() *cachev1beta1.AuthSpec {
		return &cachev1beta1.AuthSpec{Enabled: true, ExistingSecret: "shared-auth"}
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		mkVC("c1", authExt(), nil),                                                       // references shared-auth
		mkVC("c2", authExt(), nil),                                                       // also references shared-auth
		mkVC("c3", &cachev1beta1.AuthSpec{Enabled: true}, nil),                           // generated secret, no existingSecret
		mkVC("c4", nil, &cachev1beta1.TLSSpec{Enabled: true, ExistingSecret: "tls-ext"}), // TLS existingSecret
	).Build()
	r := &ValkeyClusterReconciler{Client: c, Scheme: scheme}

	names := func(reqs []ctrl.Request) []string {
		out := make([]string, 0, len(reqs))
		for _, req := range reqs {
			out = append(out, req.Name)
		}
		slices.Sort(out)
		return out
	}
	mapped := func(secretName string) []string {
		s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: ns}}
		return names(r.mapSecretToCluster(context.Background(), s))
	}

	// A user-managed auth.existingSecret update enqueues every cluster referencing it.
	if got := mapped("shared-auth"); !slices.Equal(got, []string{"c1", "c2"}) {
		t.Errorf("auth existingSecret shared-auth → %v, want [c1 c2]", got)
	}
	// The TLS cert Secret still enqueues its cluster (no regression).
	if got := mapped("tls-ext"); !slices.Equal(got, []string{"c4"}) {
		t.Errorf("tls existingSecret tls-ext → %v, want [c4]", got)
	}
	// An unreferenced Secret enqueues nothing.
	if got := mapped("unrelated"); len(got) != 0 {
		t.Errorf("unreferenced secret → %v, want none", got)
	}
	// The operator-managed generated auth Secret for c3 is "c3-auth"; it must NOT be
	// enqueued through this path — Owns() already covers operator-owned Secrets.
	if got := mapped("c3-auth"); len(got) != 0 {
		t.Errorf("generated auth secret c3-auth → %v, want none (handled by Owns)", got)
	}
}
