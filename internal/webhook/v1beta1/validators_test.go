/*
Copyright 2026 The Wellcake Authors.
*/

package v1beta1

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// newFakeClient builds a fake client seeded with the given objects, with both
// the core and cache schemes registered. Used to unit-test the admission
// validators (which read referenced Secrets / ValkeyClusters) without envtest.
func newFakeClient(objs ...client.Object) client.Client {
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		panic(err)
	}
	if err := cachev1beta1.AddToScheme(s); err != nil {
		panic(err)
	}
	return fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()
}

func secret(ns, name string) *corev1.Secret {
	return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
}

func TestValkeyClusterValidator(t *testing.T) {
	const ns = "ns"
	// A Secret that exists in the namespace, so "reference present" cases pass.
	present := secret(ns, "present")

	base := func() *cachev1beta1.ValkeyCluster {
		return &cachev1beta1.ValkeyCluster{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "c"},
			Spec: cachev1beta1.ValkeyClusterSpec{
				Topology: cachev1beta1.TopologyStandalone,
				Profile:  cachev1beta1.ProfileCache,
			},
		}
	}

	tests := []struct {
		name      string
		mutate    func(*cachev1beta1.ValkeyCluster)
		objs      []client.Object
		wantErr   bool
		wantWarns bool
	}{
		{
			name:   "no references is valid",
			mutate: func(_ *cachev1beta1.ValkeyCluster) {},
		},
		{
			name: "auth existingSecret missing -> error",
			mutate: func(vc *cachev1beta1.ValkeyCluster) {
				vc.Spec.Auth = &cachev1beta1.AuthSpec{Enabled: true, ExistingSecret: "absent"}
			},
			wantErr: true,
		},
		{
			name: "auth existingSecret present -> ok",
			mutate: func(vc *cachev1beta1.ValkeyCluster) {
				vc.Spec.Auth = &cachev1beta1.AuthSpec{Enabled: true, ExistingSecret: "present"}
			},
			objs: []client.Object{present},
		},
		{
			name: "auth disabled does not require the Secret",
			mutate: func(vc *cachev1beta1.ValkeyCluster) {
				vc.Spec.Auth = &cachev1beta1.AuthSpec{Enabled: false, ExistingSecret: "absent"}
			},
		},
		{
			name: "tls existingSecret missing -> error",
			mutate: func(vc *cachev1beta1.ValkeyCluster) {
				vc.Spec.TLS = &cachev1beta1.TLSSpec{Enabled: true, ExistingSecret: "absent"}
			},
			wantErr: true,
		},
		{
			name: "backup s3 credentials missing -> error",
			mutate: func(vc *cachev1beta1.ValkeyCluster) {
				vc.Spec.Backup = &cachev1beta1.BackupSpec{Enabled: true, S3: &cachev1beta1.S3Spec{CredentialsSecret: "absent"}}
			},
			wantErr: true,
		},
		{
			name: "restoreFrom s3 credentials missing -> error",
			mutate: func(vc *cachev1beta1.ValkeyCluster) {
				vc.Spec.RestoreFrom = &cachev1beta1.RestoreSpec{S3: &cachev1beta1.S3Spec{CredentialsSecret: "absent"}}
			},
			wantErr: true,
		},
		{
			name: "replicateFrom passwordSecret missing -> error",
			mutate: func(vc *cachev1beta1.ValkeyCluster) {
				vc.Spec.ReplicateFrom = &cachev1beta1.ReplicateFromSpec{PasswordSecret: &cachev1beta1.SecretKeyReference{Name: "absent"}}
			},
			wantErr: true,
		},
		{
			name: "Cluster topology without shards -> error",
			mutate: func(vc *cachev1beta1.ValkeyCluster) {
				vc.Spec.Topology = cachev1beta1.TopologyCluster
			},
			wantErr: true,
		},
		{
			name: "Cluster topology with shards -> ok",
			mutate: func(vc *cachev1beta1.ValkeyCluster) {
				vc.Spec.Topology = cachev1beta1.TopologyCluster
				vc.Spec.Shards = ptr.To[int32](3)
			},
		},
		{
			name: "Sentinel topology without sentinel block -> error",
			mutate: func(vc *cachev1beta1.ValkeyCluster) {
				vc.Spec.Topology = cachev1beta1.TopologySentinel
			},
			wantErr: true,
		},
		{
			name: "Durable + Replication -> warn (operator-arbitrated failover)",
			mutate: func(vc *cachev1beta1.ValkeyCluster) {
				vc.Spec.Profile = cachev1beta1.ProfileDurable
				vc.Spec.Topology = cachev1beta1.TopologyReplication
			},
			wantWarns: true,
		},
		{
			name: "Durable + empty topology (defaults to Replication) -> warn",
			mutate: func(vc *cachev1beta1.ValkeyCluster) {
				vc.Spec.Profile = cachev1beta1.ProfileDurable
				vc.Spec.Topology = ""
			},
			wantWarns: true,
		},
		{
			name: "Durable + Cluster -> no warn",
			mutate: func(vc *cachev1beta1.ValkeyCluster) {
				vc.Spec.Profile = cachev1beta1.ProfileDurable
				vc.Spec.Topology = cachev1beta1.TopologyCluster
				vc.Spec.Shards = ptr.To[int32](3)
			},
		},
		{
			name: "Cache + Replication -> no warn",
			mutate: func(vc *cachev1beta1.ValkeyCluster) {
				vc.Spec.Profile = cachev1beta1.ProfileCache
				vc.Spec.Topology = cachev1beta1.TopologyReplication
			},
		},
		{
			name: "Cluster restore without {shard} placeholder -> error",
			mutate: func(vc *cachev1beta1.ValkeyCluster) {
				vc.Spec.Topology = cachev1beta1.TopologyCluster
				vc.Spec.Shards = ptr.To[int32](3)
				vc.Spec.RestoreFrom = &cachev1beta1.RestoreSpec{
					S3:        &cachev1beta1.S3Spec{Bucket: "b"},
					SourceKey: "backups/web.rdb",
				}
			},
			wantErr: true,
		},
		{
			name: "Cluster restore with {shard} placeholder -> warn",
			mutate: func(vc *cachev1beta1.ValkeyCluster) {
				vc.Spec.Topology = cachev1beta1.TopologyCluster
				vc.Spec.Shards = ptr.To[int32](3)
				vc.Spec.RestoreFrom = &cachev1beta1.RestoreSpec{
					S3:        &cachev1beta1.S3Spec{Bucket: "b"},
					SourceKey: "backups/web-shard-{shard}.rdb",
				}
			},
			wantWarns: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			vc := base()
			tc.mutate(vc)
			v := &ValkeyClusterCustomValidator{Client: newFakeClient(tc.objs...)}
			warns, err := v.ValidateCreate(context.Background(), vc)
			if tc.wantErr != (err != nil) {
				t.Fatalf("ValidateCreate err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantWarns != (len(warns) > 0) {
				t.Fatalf("ValidateCreate warnings = %v, wantWarns = %v", warns, tc.wantWarns)
			}
		})
	}
}

func TestValkeyClusterDefaulter(t *testing.T) {
	d := &ValkeyClusterCustomDefaulter{}

	// Bare spec → conventional defaults filled at admission.
	vc := &cachev1beta1.ValkeyCluster{ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"}}
	if err := d.Default(context.Background(), vc); err != nil {
		t.Fatalf("Default: %v", err)
	}
	if vc.Spec.Topology != cachev1beta1.TopologyReplication {
		t.Errorf("Topology = %q, want Replication", vc.Spec.Topology)
	}
	if vc.Spec.Profile != cachev1beta1.ProfileCache {
		t.Errorf("Profile = %q, want Cache", vc.Spec.Profile)
	}
	if vc.Spec.Image == "" || vc.Spec.ImagePullPolicy == "" {
		t.Errorf("Image/ImagePullPolicy not defaulted: %q / %q", vc.Spec.Image, vc.Spec.ImagePullPolicy)
	}
	if vc.Spec.Replicas != 3 {
		t.Errorf("Replicas = %d, want 3", vc.Spec.Replicas)
	}

	// Durable profile → PVC-backed storage defaulted.
	dur := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "d", Namespace: "ns"},
		Spec:       cachev1beta1.ValkeyClusterSpec{Profile: cachev1beta1.ProfileDurable},
	}
	if err := d.Default(context.Background(), dur); err != nil {
		t.Fatalf("Default durable: %v", err)
	}
	if dur.Spec.Storage == nil || dur.Spec.Storage.Mode != "both" {
		t.Errorf("durable storage not defaulted: %+v", dur.Spec.Storage)
	}

	// Explicit values are preserved (idempotent / non-clobbering).
	std := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "ns"},
		Spec:       cachev1beta1.ValkeyClusterSpec{Topology: cachev1beta1.TopologyStandalone},
	}
	if err := d.Default(context.Background(), std); err != nil {
		t.Fatalf("Default standalone: %v", err)
	}
	if std.Spec.Replicas != 1 {
		t.Errorf("Standalone Replicas = %d, want 1", std.Spec.Replicas)
	}
}

func TestValkeyACLValidator(t *testing.T) {
	const ns = "ns"
	cluster := &cachev1beta1.ValkeyCluster{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "c"}}
	userSecret := secret(ns, "user-pw")

	base := func() *cachev1beta1.ValkeyACL {
		return &cachev1beta1.ValkeyACL{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "acl"},
			Spec: cachev1beta1.ValkeyACLSpec{
				ClusterRef: corev1.LocalObjectReference{Name: "c"},
			},
		}
	}

	tests := []struct {
		name      string
		mutate    func(*cachev1beta1.ValkeyACL)
		objs      []client.Object
		wantErr   bool
		wantWarns bool
	}{
		{
			name:   "cluster present, no users -> ok no warnings",
			mutate: func(_ *cachev1beta1.ValkeyACL) {},
			objs:   []client.Object{cluster},
		},
		{
			name:      "missing target cluster -> warning, not error",
			mutate:    func(_ *cachev1beta1.ValkeyACL) {},
			objs:      nil, // cluster absent
			wantWarns: true,
		},
		{
			name: "duplicate user -> error",
			mutate: func(a *cachev1beta1.ValkeyACL) {
				a.Spec.Users = []cachev1beta1.ValkeyACLUser{{Name: "alice"}, {Name: "alice"}}
			},
			objs:    []client.Object{cluster},
			wantErr: true,
		},
		{
			name: "reserved default user -> error",
			mutate: func(a *cachev1beta1.ValkeyACL) {
				a.Spec.Users = []cachev1beta1.ValkeyACLUser{{Name: "default"}}
			},
			objs:    []client.Object{cluster},
			wantErr: true,
		},
		{
			name: "user passwordSecret missing -> error",
			mutate: func(a *cachev1beta1.ValkeyACL) {
				a.Spec.Users = []cachev1beta1.ValkeyACLUser{
					{Name: "alice", PasswordSecret: &cachev1beta1.SecretKeyReference{Name: "absent"}},
				}
			},
			objs:    []client.Object{cluster},
			wantErr: true,
		},
		{
			name: "user passwordSecret present -> ok",
			mutate: func(a *cachev1beta1.ValkeyACL) {
				a.Spec.Users = []cachev1beta1.ValkeyACLUser{
					{Name: "alice", PasswordSecret: &cachev1beta1.SecretKeyReference{Name: "user-pw"}},
				}
			},
			objs: []client.Object{cluster, userSecret},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			acl := base()
			tc.mutate(acl)
			v := &ValkeyACLCustomValidator{Client: newFakeClient(tc.objs...)}
			warns, err := v.ValidateCreate(context.Background(), acl)
			if tc.wantErr != (err != nil) {
				t.Fatalf("ValidateCreate err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantWarns != (len(warns) > 0) {
				t.Fatalf("warnings = %v, wantWarns = %v", warns, tc.wantWarns)
			}
		})
	}
}
