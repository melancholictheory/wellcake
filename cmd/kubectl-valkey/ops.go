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
	"maps"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// Request annotations honored by the operator. Keep in sync with
// internal/controller/resources.go.
const (
	restartAnnotation        = "valkey.wellcake.io/restart"
	reshardAnnotation        = "valkey.wellcake.io/reshard"
	failoverAnnotation       = "valkey.wellcake.io/failover"
	failoverTargetAnnotation = "valkey.wellcake.io/failover-target"
	hibernateAnnotation      = "valkey.wellcake.io/hibernate"
)

// nowToken returns a fresh request token. A new value makes the operator act
// once; the same value is a no-op.
func nowToken() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func newRestartCmd() *cobra.Command {
	var namespace string
	cmd := &cobra.Command{
		Use:   "restart <cluster>",
		Short: "Trigger a rolling restart of the cluster pods",
		Long:  "Stamp a restart token on the cluster; the operator rolls the StatefulSet by updating the pod template.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := newClient()
			if err != nil {
				return err
			}
			return patchClusterAnnotations(cmd.Context(), c, cmd.OutOrStdout(),
				resolveNamespace(namespace), args[0],
				map[string]string{restartAnnotation: nowToken()}, nil, "rolling restart requested")
		},
	}
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace (defaults to current kube-context)")
	return cmd
}

func newReshardCmd() *cobra.Command {
	var namespace string
	cmd := &cobra.Command{
		Use:   "reshard <cluster>",
		Short: "Trigger a slot rebalance (Cluster topology)",
		Long:  "Stamp a reshard token; the operator runs a one-off valkey-cli --cluster rebalance.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := newClient()
			if err != nil {
				return err
			}
			return patchClusterAnnotations(cmd.Context(), c, cmd.OutOrStdout(),
				resolveNamespace(namespace), args[0],
				map[string]string{reshardAnnotation: nowToken()}, requireCluster, "reshard requested")
		},
	}
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace (defaults to current kube-context)")
	return cmd
}

func newFailoverCmd() *cobra.Command {
	var namespace, to string
	cmd := &cobra.Command{
		Use:   "failover <cluster>",
		Short: "Promote a replica to primary (Replication/Sentinel topology)",
		Long: "Stamp a failover token; the operator promotes a replica even if the current primary is healthy. " +
			"With --to, the named replica is promoted; otherwise the most up-to-date replica wins.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := newClient()
			if err != nil {
				return err
			}
			kv := map[string]string{failoverAnnotation: nowToken()}
			if to != "" {
				kv[failoverTargetAnnotation] = to
			}
			return patchClusterAnnotations(cmd.Context(), c, cmd.OutOrStdout(),
				resolveNamespace(namespace), args[0], kv, requireNotCluster, "failover requested")
		},
	}
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace (defaults to current kube-context)")
	cmd.Flags().StringVar(&to, "to", "", "Promote this specific replica pod (default: most up-to-date replica)")
	return cmd
}

func newHibernateCmd() *cobra.Command {
	var namespace string
	cmd := &cobra.Command{
		Use:   "hibernate <on|off> <cluster>",
		Short: "Hibernate (scale to zero, keep PVCs) or wake a cluster",
		Long: "`hibernate on` scales the cluster to zero pods while keeping its PVCs; " +
			"`hibernate off` wakes it back to its configured size.",
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			var value, msg string
			switch args[0] {
			case "on":
				value, msg = valueTrue, "hibernation requested"
			case "off":
				value, msg = "false", "wake requested"
			default:
				return fmt.Errorf("first argument must be 'on' or 'off', got %q", args[0])
			}
			c, err := newClient()
			if err != nil {
				return err
			}
			return patchClusterAnnotations(cmd.Context(), c, cmd.OutOrStdout(),
				resolveNamespace(namespace), args[1],
				map[string]string{hibernateAnnotation: value}, nil, msg)
		},
	}
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace (defaults to current kube-context)")
	return cmd
}

// patchClusterAnnotations sets request annotations on a ValkeyCluster via a
// merge patch, after an optional topology guard.
func patchClusterAnnotations(ctx context.Context, c client.Client, out io.Writer, namespace, name string,
	kv map[string]string, guard func(cachev1beta1.Topology) error, msg string) error {
	var vc cachev1beta1.ValkeyCluster
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &vc); err != nil {
		return fmt.Errorf("get ValkeyCluster %s/%s: %w", namespace, name, err)
	}
	if guard != nil {
		if err := guard(vc.Spec.Topology); err != nil {
			return err
		}
	}
	patch := client.MergeFrom(vc.DeepCopy())
	if vc.Annotations == nil {
		vc.Annotations = map[string]string{}
	}
	maps.Copy(vc.Annotations, kv)
	if err := c.Patch(ctx, &vc, patch); err != nil {
		return fmt.Errorf("patch ValkeyCluster %s/%s: %w", namespace, name, err)
	}
	_, _ = fmt.Fprintf(out, "%s for %s/%s\n", msg, namespace, name)
	return nil
}

func requireCluster(t cachev1beta1.Topology) error {
	if t != cachev1beta1.TopologyCluster {
		return fmt.Errorf("reshard is only valid for Cluster topology (cluster is %q)", t)
	}
	return nil
}

func requireNotCluster(t cachev1beta1.Topology) error {
	if t == cachev1beta1.TopologyCluster {
		return fmt.Errorf("failover is for Replication/Sentinel; Cluster topology fails over via native gossip")
	}
	return nil
}
