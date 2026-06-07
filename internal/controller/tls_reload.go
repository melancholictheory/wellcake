/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"net"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// loadMTLSClientCert returns the client certificate the operator must present
// when dialing pods under mutual TLS (tls.mutualTLS=true → tls-auth-clients yes
// on the server, which rejects a handshake without a client cert). Returns nil
// when TLS is off or mutualTLS is not set, or when the cert Secret can't be read
// yet — the dial then fails and retries, which is better than wedging the
// reconcile. The cert lives in the same TLS Secret as the server cert.
func loadMTLSClientCert(ctx context.Context, c client.Reader, vc *cachev1beta1.ValkeyCluster) *tls.Certificate {
	if !tlsEnabled(vc) || vc.Spec.TLS == nil || !vc.Spec.TLS.MutualTLS {
		return nil
	}
	var sec corev1.Secret
	if err := c.Get(ctx, types.NamespacedName{Namespace: vc.Namespace, Name: tlsSecretName(vc)}, &sec); err != nil {
		return nil
	}
	kp, err := tls.X509KeyPair(sec.Data[secretKeyTLSCert], sec.Data[secretKeyTLSKey])
	if err != nil {
		return nil
	}
	return &kp
}

const (
	secretKeyTLSCert   = "tls.crt"
	secretKeyTLSKey    = "tls.key"
	secretKeyTLSCACert = "ca.crt"
)

// reconcileTLSReload reloads the TLS certificate on every live pod with no
// restart when the cert Secret changes (typically a cert-manager renewal).
//
// Valkey reads the cert/key files only at startup and on an explicit
// `CONFIG SET tls-cert-file <path>` (re-reading the file at that path); the
// mounted Secret files update on renewal but the in-memory cert stays stale, and
// the cert is deliberately excluded from the config-hash so the pods are NOT
// rolled. So the operator drives a live reload, verifying success over the TLS
// handshake itself (the served leaf cert), which sidesteps the unknowable
// kubelet volume-sync delay: it only records success once a pod actually serves
// the Secret's current cert.
//
// Returns (reloaded, requeueAfter, error). reloaded=true means it just brought
// every pod onto the new cert (caller requeues). A non-zero requeueAfter means
// the mounted files have not synced on some pod yet — try again shortly.
func (r *ValkeyClusterReconciler) reconcileTLSReload(ctx context.Context, vc *cachev1beta1.ValkeyCluster) (bool, time.Duration, error) {
	if !tlsEnabled(vc) {
		return false, 0, nil
	}

	var sec corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: vc.Namespace, Name: tlsSecretName(vc)}, &sec); err != nil {
		if apierrors.IsNotFound(err) {
			// The cert Secret is the user's / cert-manager's to provide; nothing
			// to reload until it exists.
			return false, 0, nil
		}
		return false, 0, err
	}
	certPEM := sec.Data[secretKeyTLSCert]
	if len(certPEM) == 0 {
		return false, 0, nil
	}
	desiredFP, err := certFingerprintFromPEM(certPEM)
	if err != nil {
		// A malformed cert Secret is the user's / cert-manager's problem; don't
		// wedge the reconcile over it — log and skip the reload.
		logf.FromContext(ctx).Info("tls reload: cannot parse cert from Secret, skipping", "secret", tlsSecretName(vc), "err", err.Error())
		return false, 0, nil
	}

	// Fast-path gate: the live pods already serve this cert (recorded last time).
	if desiredFP == vc.Status.LastTLSCertFingerprint {
		return false, 0, nil
	}
	// Authoritative (uncached) confirmation — the cached object can lag a prior
	// reconcile's status write (same cache-lag race as the password rotation).
	if r.APIReader != nil {
		var fresh cachev1beta1.ValkeyCluster
		if err := r.APIReader.Get(ctx, types.NamespacedName{Namespace: vc.Namespace, Name: vc.Name}, &fresh); err != nil {
			return false, 0, err
		}
		if desiredFP == fresh.Status.LastTLSCertFingerprint {
			return false, 0, nil
		}
	}

	// Under mutual TLS the server demands a client cert for the handshake; present
	// the cluster cert (same Secret) for both the redis dial and the verify dial.
	var clientCert *tls.Certificate
	if kp, err := tls.X509KeyPair(certPEM, sec.Data[secretKeyTLSKey]); err == nil {
		clientCert = &kp
	}

	password, err := r.ensurePasswordSecret(ctx, vc)
	if err != nil {
		return false, 0, err
	}

	allServeNew, err := r.reloadTLSOnPods(ctx, vc, password, clientCert, desiredFP)
	if err != nil {
		return false, 0, err
	}
	if !allServeNew {
		// Some pod still serves the old cert: its mounted Secret volume has not
		// been synced by the kubelet yet. Re-check shortly; do NOT record success.
		return false, 20 * time.Second, nil
	}

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var latest cachev1beta1.ValkeyCluster
		if err := r.Get(ctx, types.NamespacedName{Namespace: vc.Namespace, Name: vc.Name}, &latest); err != nil {
			return err
		}
		latest.Status.LastTLSCertFingerprint = desiredFP
		return r.Status().Update(ctx, &latest)
	}); err != nil {
		return false, 0, err
	}

	logf.FromContext(ctx).Info("tls reload: complete", "fingerprint", desiredFP)
	return true, 0, nil
}

// reloadTLSOnPods drives the live reload on each pod and reports whether ALL of
// them now serve the desired cert. A pod already serving it is skipped; one that
// does not gets a CONFIG SET tls-* (re-read from the mounted path) and is then
// re-checked. A pod that still serves the old cert after the reload (kubelet
// volume sync not done) leaves allServeNew=false so the caller requeues.
func (r *ValkeyClusterReconciler) reloadTLSOnPods(ctx context.Context, vc *cachev1beta1.ValkeyCluster, password string, clientCert *tls.Certificate, desiredFP string) (bool, error) {
	port := valkeyTLSPort
	log := logf.FromContext(ctx)

	allServeNew := true
	// clusterDataPods enumerates the right FQDNs for every topology, including the
	// per-shard Cluster layout (ADR 0005).
	for _, p := range clusterDataPods(vc) {
		host := p.host

		served, err := servedCertFingerprint(ctx, host, port, clientCert)
		if err != nil {
			// Unreachable (pod down / rolling). Skip WITHOUT forcing a requeue: a
			// pod that is down must not stall the reconcile (failover may need to
			// run), and a restarted pod loads the renewed cert from its mounted
			// file at startup anyway. It'll be re-checked on the next reconcile.
			log.Info("tls reload: pod unreachable, skipping", "host", host, "err", err.Error())
			continue
		}
		if served == desiredFP {
			continue
		}

		c := dialReplClient(ctx, host, port, password, true, clientCert, 5*time.Second)
		if c == nil {
			log.Info("tls reload: cannot dial pod, will retry", "host", host)
			allServeNew = false
			continue
		}
		log.Info("tls reload: reloading cert on pod", "host", host)
		// tls-ca-cert-file points at caCertPath, which under S4 source-CA merge is
		// the combined bundle on the data PVC — NOT tlsMountPath/ca.crt. Resetting
		// it to the local-only CA here would silently drop trust in the source CA
		// (replication link breaks) until the next pod restart rebuilds the bundle.
		for _, p := range []struct{ param, path string }{
			{"tls-cert-file", fmt.Sprintf("%s/%s", tlsMountPath, secretKeyTLSCert)},
			{"tls-key-file", fmt.Sprintf("%s/%s", tlsMountPath, secretKeyTLSKey)},
			{"tls-ca-cert-file", caCertPath(vc)},
		} {
			if err := c.configSet(ctx, p.param, p.path); err != nil {
				c.close()
				return false, fmt.Errorf("tls reload %s: set %s: %w", host, p.param, err)
			}
		}
		c.close()

		// Re-check: did the reload pick up the new (synced) file?
		served, err = servedCertFingerprint(ctx, host, port, clientCert)
		if err != nil || served != desiredFP {
			allServeNew = false
		}
	}
	return allServeNew, nil
}

// servedCertFingerprint opens a TLS connection and returns the SHA-256
// fingerprint of the leaf certificate the server presents. InsecureSkipVerify is
// fine here: we are not authenticating the peer, only reading which cert it
// serves to compare against the desired one.
func servedCertFingerprint(ctx context.Context, host string, port int32, clientCert *tls.Certificate) (string, error) {
	conf := &tls.Config{InsecureSkipVerify: true} // #nosec G402 — reading served cert, not authenticating
	if clientCert != nil {
		conf.Certificates = []tls.Certificate{*clientCert}
	}
	d := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 5 * time.Second}, Config: conf}
	conn, err := d.DialContext(ctx, "tcp", net.JoinHostPort(host, strconv.Itoa(int(port))))
	if err != nil {
		return "", err
	}
	defer func() { _ = conn.Close() }()
	state := conn.(*tls.Conn).ConnectionState()
	if len(state.PeerCertificates) == 0 {
		return "", fmt.Errorf("no peer certificate from %s", host)
	}
	return fingerprintDER(state.PeerCertificates[0].Raw), nil
}

// certFingerprintFromPEM parses the first certificate in a PEM bundle (the leaf)
// and returns its SHA-256 fingerprint.
func certFingerprintFromPEM(pemBytes []byte) (string, error) {
	for {
		var block *pem.Block
		block, pemBytes = pem.Decode(pemBytes)
		if block == nil {
			return "", fmt.Errorf("no CERTIFICATE block in tls.crt")
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return "", err
		}
		return fingerprintDER(cert.Raw), nil
	}
}

func fingerprintDER(der []byte) string {
	sum := sha256.Sum256(der)
	return hex.EncodeToString(sum[:])
}
