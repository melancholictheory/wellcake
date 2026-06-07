/*
Copyright 2026 The Wellcake Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"crypto/tls"
	"flag"
	"os"
	"strings"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	cachev1alpha1 "github.com/melancholictheory/wellcake/api/v1alpha1"
	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
	"github.com/melancholictheory/wellcake/internal/controller"
	webhookv1beta1 "github.com/melancholictheory/wellcake/internal/webhook/v1beta1"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	// Both API versions are registered: v1beta1 is the storage/hub version the
	// controller reconciles; v1alpha1 is the served spoke, present so the
	// conversion webhook can convert between them.
	utilruntime.Must(cachev1alpha1.AddToScheme(scheme))
	utilruntime.Must(cachev1beta1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

// watchNamespaces parses a WATCH_NAMESPACE value (comma-separated list) into
// the cache.Options.DefaultNamespaces map that scopes the manager cache. An
// empty or whitespace-only value yields nil, which leaves the manager
// cluster-scoped (watching all namespaces) — the default.
func watchNamespaces(raw string) map[string]cache.Config {
	out := map[string]cache.Config{}
	for ns := range strings.SplitSeq(raw, ",") {
		if ns = strings.TrimSpace(ns); ns != "" {
			out[ns] = cache.Config{}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// nolint:gocyclo
func main() {
	var metricsAddr string
	var metricsCertPath, metricsCertName, metricsCertKey string
	var webhookCertPath, webhookCertName, webhookCertKey string
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var tlsOpts []func(*tls.Config)
	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.StringVar(&webhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
	flag.StringVar(&webhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
	flag.StringVar(&webhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	flag.StringVar(&metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	flag.StringVar(&metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(&metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	var maxConcurrentReconciles int
	flag.IntVar(&maxConcurrentReconciles, "max-concurrent-reconciles", 1,
		"Max ValkeyClusters reconciled in parallel (different objects only; the same "+
			"object is never reconciled concurrently). Raise for large fleets — the "+
			"default 1 serialises all CRs (see design-review Q3/SC2).")
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("Disabling HTTP/2")
		c.NextProtos = []string{"http/1.1"}
	}

	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	// Initial webhook TLS options
	webhookTLSOpts := tlsOpts
	webhookServerOptions := webhook.Options{
		TLSOpts: webhookTLSOpts,
	}

	if len(webhookCertPath) > 0 {
		setupLog.Info("Initializing webhook certificate watcher using provided certificates",
			"webhook-cert-path", webhookCertPath, "webhook-cert-name", webhookCertName, "webhook-cert-key", webhookCertKey)

		webhookServerOptions.CertDir = webhookCertPath
		webhookServerOptions.CertName = webhookCertName
		webhookServerOptions.KeyName = webhookCertKey
	}

	webhookServer := webhook.NewServer(webhookServerOptions)

	// Metrics endpoint is enabled in 'config/default/kustomization.yaml'. The Metrics options configure the server.
	// More info:
	// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.3/pkg/metrics/server
	// - https://book.kubebuilder.io/reference/metrics.html
	metricsServerOptions := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: secureMetrics,
		TLSOpts:       tlsOpts,
	}

	if secureMetrics {
		// FilterProvider is used to protect the metrics endpoint with authn/authz.
		// These configurations ensure that only authorized users and service accounts
		// can access the metrics endpoint. The RBAC are configured in 'config/rbac/kustomization.yaml'. More info:
		// https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.3/pkg/metrics/filters#WithAuthenticationAndAuthorization
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	// If the certificate is not specified, controller-runtime will automatically
	// generate self-signed certificates for the metrics server. While convenient for development and testing,
	// this setup is not recommended for production.
	//
	// TODO(user): If you enable certManager, uncomment the following lines:
	// - [METRICS-WITH-CERTS] at config/default/kustomization.yaml to generate and use certificates
	// managed by cert-manager for the metrics server.
	// - [PROMETHEUS-WITH-CERTS] at config/prometheus/kustomization.yaml for TLS certification.
	if len(metricsCertPath) > 0 {
		setupLog.Info("Initializing metrics certificate watcher using provided certificates",
			"metrics-cert-path", metricsCertPath, "metrics-cert-name", metricsCertName, "metrics-cert-key", metricsCertKey)

		metricsServerOptions.CertDir = metricsCertPath
		metricsServerOptions.CertName = metricsCertName
		metricsServerOptions.KeyName = metricsCertKey
	}

	// WATCH_NAMESPACE (comma-separated) scopes the manager cache — and thus
	// every controller's watch — to the listed namespaces. Empty/unset leaves
	// the operator cluster-scoped (all namespaces). Pair with rbac.scope in the
	// chart so the ServiceAccount only gets namespaced RBAC.
	watchNS := watchNamespaces(os.Getenv("WATCH_NAMESPACE"))
	if len(watchNS) > 0 {
		names := make([]string, 0, len(watchNS))
		for ns := range watchNS {
			names = append(names, ns)
		}
		setupLog.Info("restricting watch to namespaces", "namespaces", names)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsServerOptions,
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: probeAddr,
		Cache: cache.Options{
			DefaultNamespaces: watchNS,
			// Scope the Pod cache to our own data-plane pods. The controller
			// watches Pods only to react fast to a primary loss (see the Pod
			// Watches in SetupWithManager) and never reads foreign pods. Without
			// this selector the manager would cache EVERY pod in the watched
			// namespaces — a real memory + event cost at 500+ instances
			// (design-review Q3/SC1). labelsFor() stamps this label on every pod.
			ByObject: map[client.Object]cache.ByObject{
				&corev1.Pod{}: {
					Label: labels.SelectorFromSet(labels.Set{"app.kubernetes.io/managed-by": "valkey-operator"}),
				},
			},
		},
		LeaderElection:   enableLeaderElection,
		LeaderElectionID: "1e0a58e3.wellcake.io",
		// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
		// when the Manager ends. This requires the binary to immediately end when the
		// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
		// speeds up voluntary leader transitions as the new leader don't have to wait
		// LeaseDuration time first.
		//
		// In the default scaffold provided, the program ends immediately after
		// the manager stops, so would be fine to enable this option. However,
		// if you are doing or is intended to do any operation such as perform cleanups
		// after the manager stops then its usage might be unsafe.
		// LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "Failed to start manager")
		os.Exit(1)
	}

	if err := (&controller.ValkeyClusterReconciler{
		Client:                  mgr.GetClient(),
		APIReader:               mgr.GetAPIReader(),
		Scheme:                  mgr.GetScheme(),
		MaxConcurrentReconciles: maxConcurrentReconciles,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "valkeycluster")
		os.Exit(1)
	}
	if err := (&controller.ValkeyACLReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "valkeyacl")
		os.Exit(1)
	}
	// nolint:goconst
	if os.Getenv("ENABLE_WEBHOOKS") != "false" {
		if err := webhookv1beta1.SetupValkeyClusterWebhookWithManager(mgr); err != nil {
			setupLog.Error(err, "Failed to create webhook", "webhook", "ValkeyCluster")
			os.Exit(1)
		}
	}
	// nolint:goconst
	if os.Getenv("ENABLE_WEBHOOKS") != "false" {
		if err := webhookv1beta1.SetupValkeyACLWebhookWithManager(mgr); err != nil {
			setupLog.Error(err, "Failed to create webhook", "webhook", "ValkeyACL")
			os.Exit(1)
		}
	}
	// +kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("Starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "Failed to run manager")
		os.Exit(1)
	}
}
