/*
Copyright 2026 The Wellcake Authors.
*/

package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

func TestRunCertificateExportsFiles(t *testing.T) {
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "ns"},
		Spec: cachev1beta1.ValkeyClusterSpec{
			Topology: cachev1beta1.TopologyReplication,
			TLS:      &cachev1beta1.TLSSpec{Enabled: true, ExistingSecret: "web-tls"},
		},
	}
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "web-tls", Namespace: "ns"},
		Data:       map[string][]byte{"ca.crt": []byte("CA"), "tls.crt": []byte("CRT"), "tls.key": []byte("KEY")},
	}
	c := testClient(t, vc, sec)
	dir := t.TempDir()
	var out bytes.Buffer

	if err := runCertificate(context.Background(), c, &out, "ns", "web", dir); err != nil {
		t.Fatalf("runCertificate: %v", err)
	}
	for f, want := range map[string]string{"ca.crt": "CA", "tls.crt": "CRT", "tls.key": "KEY"} {
		b, err := os.ReadFile(filepath.Join(dir, f))
		if err != nil || string(b) != want {
			t.Errorf("%s = %q (err %v), want %q", f, string(b), err, want)
		}
	}
}

func TestRunCertificateStdoutCA(t *testing.T) {
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "ns"},
		Spec:       cachev1beta1.ValkeyClusterSpec{Topology: cachev1beta1.TopologyReplication, TLS: &cachev1beta1.TLSSpec{Enabled: true}},
	}
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "web-tls", Namespace: "ns"},
		Data:       map[string][]byte{"ca.crt": []byte("THE-CA")},
	}
	c := testClient(t, vc, sec)
	var out bytes.Buffer
	if err := runCertificate(context.Background(), c, &out, "ns", "web", ""); err != nil {
		t.Fatalf("runCertificate: %v", err)
	}
	if out.String() != "THE-CA" {
		t.Errorf("stdout = %q, want THE-CA", out.String())
	}
}

func TestRunCertificateNoTLS(t *testing.T) {
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "ns"},
		Spec:       cachev1beta1.ValkeyClusterSpec{Topology: cachev1beta1.TopologyReplication},
	}
	c := testClient(t, vc)
	var out bytes.Buffer
	if err := runCertificate(context.Background(), c, &out, "ns", "web", ""); err == nil {
		t.Fatalf("expected an error when TLS is disabled")
	}
}

func TestRunReportWritesFiles(t *testing.T) {
	vc := &cachev1beta1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "ns"},
		Spec:       cachev1beta1.ValkeyClusterSpec{Topology: cachev1beta1.TopologyReplication, Replicas: 3},
	}
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "ns", Labels: map[string]string{instanceLabel: "web"}},
	}
	c := testClient(t, vc, sts)
	dir := t.TempDir()
	var out bytes.Buffer

	if err := runReport(context.Background(), c, &out, "ns", "web", dir); err != nil {
		t.Fatalf("runReport: %v", err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "cluster.yaml"))
	if err != nil || !strings.Contains(string(b), "name: web") {
		t.Errorf("cluster.yaml missing or wrong: %q (err %v)", string(b), err)
	}
	if _, err := os.Stat(filepath.Join(dir, "statefulsets.yaml")); err != nil {
		t.Errorf("statefulsets.yaml not written: %v", err)
	}
}
