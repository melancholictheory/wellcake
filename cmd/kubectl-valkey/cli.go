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
	"os"
	"os/exec"
	"strconv"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

func newCliCmd() *cobra.Command {
	var namespace, podOverride string
	cmd := &cobra.Command{
		Use:     "cli <cluster> [-- valkey-cli args...]",
		Aliases: []string{"exec"},
		Short:   "Open valkey-cli against a cluster pod",
		Long: "Exec valkey-cli inside a cluster pod — the primary by default " +
			"(pod-0 for Cluster topology, with -c). Auth is taken from the cluster " +
			"Secret. Anything after -- is passed through to valkey-cli.\n\n" +
			"Examples:\n" +
			"  kubectl valkey cli web-cache\n" +
			"  kubectl valkey cli web-cache -- INFO replication\n" +
			"  kubectl valkey cli demo --pod demo-2 -- CLUSTER NODES",
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCli(cmd.Context(), namespace, args[0], podOverride, args[1:])
		},
	}
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace (defaults to current kube-context)")
	cmd.Flags().StringVar(&podOverride, "pod", "", "Target a specific pod instead of the primary/pod-0")
	return cmd
}

func runCli(ctx context.Context, namespace, name, podOverride string, extra []string) error {
	c, err := newClient()
	if err != nil {
		return err
	}
	namespace = resolveNamespace(namespace)

	var vc cachev1beta1.ValkeyCluster
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &vc); err != nil {
		return fmt.Errorf("get ValkeyCluster %s/%s: %w", namespace, name, err)
	}

	pod := podOverride
	if pod == "" {
		pod = targetPod(&vc)
	}

	password := ""
	if authOn(&vc) {
		var sec corev1.Secret
		if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: passwordSecretName(&vc)}, &sec); err != nil {
			return fmt.Errorf("read auth secret %q: %w", passwordSecretName(&vc), err)
		}
		password = string(sec.Data["password"])
	}

	kubectl, err := exec.LookPath("kubectl")
	if err != nil {
		return fmt.Errorf("kubectl not found on PATH: %w", err)
	}

	argv := buildCliExecArgs(&vc, namespace, pod, password, extra)
	// #nosec G204 -- argv is assembled from cluster metadata + user-supplied
	// valkey-cli args, executed via the trusted kubectl binary.
	command := exec.CommandContext(ctx, kubectl, argv...)
	command.Stdin, command.Stdout, command.Stderr = os.Stdin, os.Stdout, os.Stderr
	return command.Run()
}

// targetPod picks which pod to exec into: the observed primary for
// non-Cluster topologies, otherwise pod-0.
func targetPod(vc *cachev1beta1.ValkeyCluster) string {
	if vc.Spec.Topology != cachev1beta1.TopologyCluster && vc.Status.Primary != "" {
		return vc.Status.Primary
	}
	return vc.Name + "-0"
}

// buildCliExecArgs assembles the `kubectl exec` argv. Pure function so the
// argv shape is unit-testable without a cluster. The password (when present)
// is delivered as REDISCLI_AUTH to valkey-cli inside the pod.
func buildCliExecArgs(vc *cachev1beta1.ValkeyCluster, namespace, pod, password string, extra []string) []string {
	argv := []string{"exec", "-it", "-n", namespace, pod, "-c", valkeyCtr, "--"}
	if password != "" {
		argv = append(argv, "env", "REDISCLI_AUTH="+password)
	}
	argv = append(argv, "valkey-cli", "-p", strconv.Itoa(dataPort(vc)))
	if tlsOn(vc) {
		argv = append(argv, "--tls",
			"--cert", tlsMountPath+"/tls.crt",
			"--key", tlsMountPath+"/tls.key",
			"--cacert", tlsMountPath+"/ca.crt")
	}
	if vc.Spec.Topology == cachev1beta1.TopologyCluster {
		argv = append(argv, "-c")
	}
	return append(argv, extra...)
}
