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
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

func newCertificateCmd() *cobra.Command {
	var namespace, outDir string
	cmd := &cobra.Command{
		Use:   "certificate <cluster>",
		Short: "Show or export the cluster's TLS material",
		Long: "Read the cluster's TLS Secret (cert-manager-issued or spec.tls.existingSecret). " +
			"By default prints ca.crt to stdout; with -o writes ca.crt/tls.crt/tls.key to a directory " +
			"so a client can connect with TLS.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := newClient()
			if err != nil {
				return err
			}
			return runCertificate(cmd.Context(), c, cmd.OutOrStdout(), namespace, args[0], outDir)
		},
	}
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace (defaults to current kube-context)")
	cmd.Flags().StringVarP(&outDir, "output-dir", "o", "",
		"Write ca.crt/tls.crt/tls.key here (default: print ca.crt to stdout)")
	return cmd
}

func runCertificate(ctx context.Context, c client.Client, out io.Writer, namespace, name, outDir string) error {
	namespace = resolveNamespace(namespace)

	var vc cachev1beta1.ValkeyCluster
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &vc); err != nil {
		return fmt.Errorf("get ValkeyCluster %s/%s: %w", namespace, name, err)
	}
	if !tlsOn(&vc) {
		return fmt.Errorf("TLS is not enabled for %s/%s (spec.tls.enabled is false)", namespace, name)
	}

	secName := tlsSecretNameFor(&vc)
	var sec corev1.Secret
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: secName}, &sec); err != nil {
		return fmt.Errorf("get TLS secret %q: %w", secName, err)
	}

	if outDir == "" {
		ca := sec.Data[caCrtFile]
		if len(ca) == 0 {
			return fmt.Errorf("TLS secret %q has no ca.crt", secName)
		}
		_, err := out.Write(ca)
		return err
	}

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	wrote := 0
	for _, f := range []string{caCrtFile, tlsCrtFile, tlsKeyFile} {
		data, ok := sec.Data[f]
		if !ok || len(data) == 0 {
			continue
		}
		// #nosec G306 -- tls.key needs to be readable by the local client tooling.
		if err := os.WriteFile(filepath.Join(outDir, f), data, 0o600); err != nil {
			return fmt.Errorf("write %s: %w", f, err)
		}
		wrote++
	}
	if wrote == 0 {
		return fmt.Errorf("TLS secret %q contained none of ca.crt/tls.crt/tls.key", secName)
	}
	_, _ = fmt.Fprintf(out, "wrote %d TLS file(s) from secret %q to %s\n", wrote, secName, outDir)
	return nil
}
