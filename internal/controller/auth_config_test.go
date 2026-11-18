/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

func TestRefreshRuntimeAuthAfterContainerRestart(t *testing.T) {
	for _, tc := range []struct {
		name, file, original, preserved string
		sentinel                        bool
	}{
		{"data", "runtime.conf", "replicaof old 6379\nrequirepass old\nmasterauth old\n", "replicaof old 6379", false},
		{"sentinel", "runtime-sentinel.conf", "sentinel myid stable-id\nrequirepass old\nsentinel sentinel-pass old\nsentinel auth-pass mymaster old\n", "sentinel myid stable-id", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, tc.file)
			if err := os.WriteFile(path, []byte(tc.original), 0o600); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				cmd := exec.Command("sh", "-c", strings.ReplaceAll(refreshRuntimeAuth(tc.sentinel), dataMountPath, dir))
				cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "VALKEY_PASSWORD=new", "SOURCE_PASSWORD=remote", "WELLCAKE_INTERNAL_PASSWORD=metrics"}
				if out, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("refresh runtime auth: %v\n%s", err, out)
				}
			}
			config, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			for _, line := range []string{tc.preserved, `requirepass "\x6e\x65\x77"`} {
				if strings.Count(string(config), line) != 1 {
					t.Fatalf("expected one copy of %q in %q", line, config)
				}
			}
			if tc.sentinel && !strings.Contains(string(config), `sentinel sentinel-pass "\x6e\x65\x77"`) {
				t.Fatalf("Sentinel still has old peer credentials: %q", config)
			}
			if tc.sentinel && strings.Count(string(config), "user "+exporterACLUser+" on #") != 1 {
				t.Fatalf("exporter user was not restored exactly once: %q", config)
			}
			if !tc.sentinel && !strings.Contains(string(config), `masterauth "\x72\x65\x6d\x6f\x74\x65"`) {
				t.Fatalf("lost upstream credentials: %q", config)
			}
		})
	}
}

func TestAuthConfigHashKeepsExporterPasswordAcrossRotation(t *testing.T) {
	ctx := context.Background()
	vc := minimalCR()
	vc.UID = types.UID("test")
	vc.Spec.Auth = &cachev1beta1.AuthSpec{Enabled: new(true)}
	vc.Spec.Metrics = &cachev1beta1.MetricsSpec{Enabled: true}
	scheme := newTestScheme(t)
	r := &ValkeyClusterReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).Build(), Scheme: scheme}
	original, err := r.ensureConfigMap(ctx, vc, "old-cluster-password")
	if err != nil {
		t.Fatal(err)
	}
	rotated, err := r.ensureConfigMap(ctx, vc, "new-cluster-password")
	if err != nil || original != rotated {
		t.Fatalf("managed rotation changed the pod hash: %q -> %q (%v)", original, rotated, err)
	}
	var cm corev1.ConfigMap
	if err := r.Get(ctx, client.ObjectKey{Namespace: vc.Namespace, Name: configMapName(vc)}, &cm); err != nil {
		t.Fatal(err)
	}
	if conf := cm.Data[valkeyConfFile]; strings.Contains(conf, "requirepass") || strings.Contains(conf, "masterauth") {
		t.Fatalf("generated ConfigMap contains credential directives: %s", conf)
	}
	vc.Spec.Auth.ExistingSecret = "external"
	external, err := r.ensureConfigMap(ctx, vc, "external-old")
	if err != nil {
		t.Fatal(err)
	}
	replaced, err := r.ensureConfigMap(ctx, vc, "external-new")
	if err != nil || external == replaced {
		t.Fatalf("external password change must change the pod hash: %q -> %q (%v)", external, replaced, err)
	}
}

func TestCleanLegacyAuthConfigWaitsForOldPods(t *testing.T) {
	ctx := context.Background()
	vc := minimalCR()
	vc.UID = types.UID("test")
	scheme := newTestScheme(t)
	cm := buildConfigMap(vc)
	cm.Name = vc.Name + "-config"
	cm.Data[valkeyConfFile] = "requirepass old"
	if err := controllerutil.SetControllerReference(vc, cm, scheme); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{}
	pod.Name, pod.Namespace, pod.Labels = vc.Name+"-0", vc.Namespace, labelsFor(vc)
	pod.Spec.Volumes = []corev1.Volume{{VolumeSource: corev1.VolumeSource{
		ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: cm.Name}},
	}}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm, pod).Build()
	r := &ValkeyClusterReconciler{Client: c, Scheme: scheme}
	if err := r.cleanLegacyAuthConfig(ctx, vc); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(cm), &corev1.ConfigMap{}); err != nil {
		t.Fatalf("legacy ConfigMap deleted while an old pod uses it: %v", err)
	}
	if err := c.Delete(ctx, pod); err != nil {
		t.Fatal(err)
	}
	if err := r.cleanLegacyAuthConfig(ctx, vc); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(cm), &corev1.ConfigMap{}); !apierrors.IsNotFound(err) {
		t.Fatalf("legacy ConfigMap still exists: %v", err)
	}
}
