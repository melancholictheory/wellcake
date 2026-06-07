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
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/client/config"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// Ports and mount paths the operator hard-codes; duplicated here because the
// plugin is a standalone binary and must not import the controller's internal
// package. Keep in sync with internal/controller/resources.go.
const (
	valkeyPort    = 6379
	valkeyTLSPort = 6380
	tlsMountPath  = "/etc/valkey/tls"
	valkeyCtr     = "valkey"
	caCrtFile     = "ca.crt"
	tlsCrtFile    = "tls.crt"
	tlsKeyFile    = "tls.key"
	valueTrue     = "true"
)

// newClient builds a controller-runtime client from the ambient kubeconfig,
// with the ValkeyCluster API and core/batch types registered.
func newClient() (client.Client, error) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, err
	}
	if err := cachev1beta1.AddToScheme(scheme); err != nil {
		return nil, err
	}
	cfg, err := ctrlconfig.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig: %w", err)
	}
	return client.New(cfg, client.Options{Scheme: scheme})
}

// resolveNamespace returns the explicit flag value, or the active
// kube-context namespace, matching kubectl's default behavior.
func resolveNamespace(flag string) string {
	if flag != "" {
		return flag
	}
	cc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{})
	ns, _, err := cc.Namespace()
	if err != nil || ns == "" {
		return "default"
	}
	return ns
}

// passwordSecretName returns the Secret holding the cluster password.
func passwordSecretName(vc *cachev1beta1.ValkeyCluster) string {
	if vc.Spec.Auth != nil && vc.Spec.Auth.ExistingSecret != "" {
		return vc.Spec.Auth.ExistingSecret
	}
	return vc.Name + "-auth"
}

// dataPort returns the client-facing port for the cluster (TLS-aware).
func dataPort(vc *cachev1beta1.ValkeyCluster) int {
	if vc.Spec.TLS != nil && vc.Spec.TLS.Enabled {
		return valkeyTLSPort
	}
	return valkeyPort
}

func tlsOn(vc *cachev1beta1.ValkeyCluster) bool {
	return vc.Spec.TLS != nil && vc.Spec.TLS.Enabled
}

func authOn(vc *cachev1beta1.ValkeyCluster) bool {
	return vc.Spec.Auth != nil && vc.Spec.Auth.Enabled
}

// tlsSecretNameFor returns the Secret holding the cluster's TLS material.
func tlsSecretNameFor(vc *cachev1beta1.ValkeyCluster) string {
	if vc.Spec.TLS != nil && vc.Spec.TLS.ExistingSecret != "" {
		return vc.Spec.TLS.ExistingSecret
	}
	return vc.Name + "-tls"
}

// instanceLabel selects the resources the operator owns for a cluster.
const instanceLabel = "app.kubernetes.io/instance"
