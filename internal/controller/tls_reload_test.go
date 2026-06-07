/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// makeCertPEM returns a self-signed leaf cert PEM and its expected fingerprint.
func makeCertPEM(t *testing.T, cn string, serial int64) ([]byte, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("genkey: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Unix(0, 0),
		NotAfter:     time.Unix(1<<31, 0),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("createcert: %v", err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return pemBytes, fingerprintDER(der)
}

func TestCertFingerprintFromPEM(t *testing.T) {
	pem1, fp1 := makeCertPEM(t, "valkey-1", 1)
	pem2, fp2 := makeCertPEM(t, "valkey-2", 2)

	got1, err := certFingerprintFromPEM(pem1)
	if err != nil {
		t.Fatalf("fp1: %v", err)
	}
	if got1 != fp1 {
		t.Errorf("fingerprint mismatch: got %s want %s", got1, fp1)
	}
	if fp1 == fp2 {
		t.Errorf("distinct certs must have distinct fingerprints")
	}

	// Leaf-picking: a bundle (leaf first, then a second cert) returns the leaf's fp.
	bundle := append(append([]byte{}, pem1...), pem2...)
	gotBundle, err := certFingerprintFromPEM(bundle)
	if err != nil {
		t.Fatalf("bundle: %v", err)
	}
	if gotBundle != fp1 {
		t.Errorf("bundle fingerprint = %s, want leaf %s", gotBundle, fp1)
	}

	// Junk PEM → error.
	if _, err := certFingerprintFromPEM([]byte("not a pem")); err == nil {
		t.Errorf("expected error on non-PEM input")
	}
}

// TestTLSReloadGuards covers the short-circuits that need no pod dial.
func TestTLSReloadGuards(t *testing.T) {
	scheme := newTestScheme(t)
	certPEM, fp := makeCertPEM(t, "valkey-1", 1)

	t.Run("tls disabled", func(t *testing.T) {
		vc := &cachev1beta1.ValkeyCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
			Spec:       cachev1beta1.ValkeyClusterSpec{Topology: cachev1beta1.TopologyReplication, Replicas: 3},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(vc).Build()
		r := &ValkeyClusterReconciler{Client: c, Scheme: scheme}
		reloaded, after, err := r.reconcileTLSReload(context.Background(), vc)
		if err != nil || reloaded || after != 0 {
			t.Fatalf("tls disabled must no-op, got reloaded=%v after=%v err=%v", reloaded, after, err)
		}
	})

	t.Run("cert secret missing", func(t *testing.T) {
		vc := &cachev1beta1.ValkeyCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
			Spec: cachev1beta1.ValkeyClusterSpec{
				Topology: cachev1beta1.TopologyReplication, Replicas: 3,
				TLS: &cachev1beta1.TLSSpec{Enabled: true},
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(vc).Build()
		r := &ValkeyClusterReconciler{Client: c, Scheme: scheme}
		reloaded, after, err := r.reconcileTLSReload(context.Background(), vc)
		if err != nil || reloaded || after != 0 {
			t.Fatalf("missing secret must no-op, got reloaded=%v after=%v err=%v", reloaded, after, err)
		}
	})

	t.Run("fingerprint already applied", func(t *testing.T) {
		vc := &cachev1beta1.ValkeyCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
			Spec: cachev1beta1.ValkeyClusterSpec{
				Topology: cachev1beta1.TopologyReplication, Replicas: 3,
				TLS: &cachev1beta1.TLSSpec{Enabled: true},
			},
			Status: cachev1beta1.ValkeyClusterStatus{LastTLSCertFingerprint: fp},
		}
		sec := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "c-tls", Namespace: "ns"},
			Data:       map[string][]byte{secretKeyTLSCert: certPEM},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(vc, sec).Build()
		r := &ValkeyClusterReconciler{Client: c, Scheme: scheme}
		reloaded, after, err := r.reconcileTLSReload(context.Background(), vc)
		if err != nil || reloaded || after != 0 {
			t.Fatalf("matching fingerprint must no-op, got reloaded=%v after=%v err=%v", reloaded, after, err)
		}
	})
}
