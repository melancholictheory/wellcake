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
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/types"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

func newStatusCmd() *cobra.Command {
	var namespace string
	cmd := &cobra.Command{
		Use:   "status <cluster>",
		Short: "Show the status of a ValkeyCluster",
		Long: "Print a human-readable summary of a ValkeyCluster: topology, profile, " +
			"primary/shards, per-shard health and replica lag, and conditions.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runStatus(cmd.Context(), cmd.OutOrStdout(), namespace, args[0])
		},
	}
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "",
		"Namespace of the cluster (defaults to the current kube-context namespace)")
	return cmd
}

func runStatus(ctx context.Context, out io.Writer, namespace, name string) error {
	c, err := newClient()
	if err != nil {
		return err
	}
	namespace = resolveNamespace(namespace)

	var vc cachev1beta1.ValkeyCluster
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &vc); err != nil {
		return fmt.Errorf("get ValkeyCluster %s/%s: %w", namespace, name, err)
	}
	_, _ = fmt.Fprint(out, formatStatus(&vc))
	return nil
}

// formatStatus renders the CR into a plain-text report. Pure function — no
// cluster access — so it is unit-testable.
func formatStatus(vc *cachev1beta1.ValkeyCluster) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Cluster:   %s/%s\n", vc.Namespace, vc.Name)
	fmt.Fprintf(&b, "Topology:  %s\n", orDash(string(vc.Spec.Topology)))
	fmt.Fprintf(&b, "Profile:   %s\n", orDash(string(vc.Spec.Profile)))
	fmt.Fprintf(&b, "Image:     %s\n", orDash(vc.Spec.Image))
	fmt.Fprintf(&b, "Phase:     %s\n", orDash(string(vc.Status.Phase)))

	if vc.Spec.Topology == cachev1beta1.TopologyCluster {
		fmt.Fprintf(&b, "Shards:    %d/%d ready\n", vc.Status.ReadyShards, vc.Status.Shards)
		fmt.Fprintf(&b, "Bootstrapped: %t\n", vc.Status.ClusterInitialized)
	} else {
		fmt.Fprintf(&b, "Primary:   %s\n", orDash(vc.Status.Primary))
		fmt.Fprintf(&b, "Ready:     %d/%d\n", vc.Status.ReadyReplicas, vc.Spec.Replicas)
	}

	if len(vc.Status.Conditions) > 0 {
		b.WriteString("\nConditions:\n")
		w := tabwriter.NewWriter(&b, 0, 2, 2, ' ', 0)
		_, _ = fmt.Fprintln(w, "  TYPE\tSTATUS\tREASON\tMESSAGE")
		for _, c := range vc.Status.Conditions {
			_, _ = fmt.Fprintf(w, "  %s\t%s\t%s\t%s\n", c.Type, c.Status, c.Reason, c.Message)
		}
		_ = w.Flush()
	}

	if len(vc.Status.ShardDetails) > 0 {
		b.WriteString("\nShards:\n")
		w := tabwriter.NewWriter(&b, 0, 2, 2, ' ', 0)
		_, _ = fmt.Fprintln(w, "  #\tPRIMARY\tREPLICAS\tSLOTS\tHEALTH\tLAG(bytes)")
		for _, s := range vc.Status.ShardDetails {
			_, _ = fmt.Fprintf(w, "  %d\t%s\t%s\t%d\t%s\t%d\n",
				s.Index, orDash(s.Primary), joinOrDash(s.Replicas), s.SlotCount, orDash(s.Health), s.MaxLagBytes)
		}
		_ = w.Flush()
	}

	return b.String()
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func joinOrDash(items []string) string {
	if len(items) == 0 {
		return "-"
	}
	return strings.Join(items, ",")
}
