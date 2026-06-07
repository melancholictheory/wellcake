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
	"strings"

	"github.com/spf13/cobra"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

func newReportCmd() *cobra.Command {
	var namespace, outDir string
	cmd := &cobra.Command{
		Use:   "report <cluster>",
		Short: "Collect a diagnostic dump of the cluster and its owned resources",
		Long: "Write the ValkeyCluster CR plus its owned StatefulSet, Services, ConfigMaps, " +
			"PDBs, Pods and related Events as YAML files into a directory, for support/debugging.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := newClient()
			if err != nil {
				return err
			}
			return runReport(cmd.Context(), c, cmd.OutOrStdout(), namespace, args[0], outDir)
		},
	}
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace (defaults to current kube-context)")
	cmd.Flags().StringVarP(&outDir, "output-dir", "o", "",
		"Directory to write the report into (default: ./valkey-report-<cluster>)")
	return cmd
}

func runReport(ctx context.Context, c client.Client, out io.Writer, namespace, name, outDir string) error {
	namespace = resolveNamespace(namespace)
	if outDir == "" {
		outDir = "valkey-report-" + name
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}

	var vc cachev1beta1.ValkeyCluster
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &vc); err != nil {
		return fmt.Errorf("get ValkeyCluster %s/%s: %w", namespace, name, err)
	}
	if err := writeYAML(outDir, "cluster.yaml", &vc); err != nil {
		return err
	}

	inst := client.MatchingLabels{instanceLabel: name}
	files := 1

	var stsList appsv1.StatefulSetList
	if c.List(ctx, &stsList, client.InNamespace(namespace), inst) == nil && len(stsList.Items) > 0 {
		_ = writeYAML(outDir, "statefulsets.yaml", &stsList)
		files++
	}
	var svcList corev1.ServiceList
	if c.List(ctx, &svcList, client.InNamespace(namespace), inst) == nil && len(svcList.Items) > 0 {
		_ = writeYAML(outDir, "services.yaml", &svcList)
		files++
	}
	var cmList corev1.ConfigMapList
	if c.List(ctx, &cmList, client.InNamespace(namespace), inst) == nil && len(cmList.Items) > 0 {
		_ = writeYAML(outDir, "configmaps.yaml", &cmList)
		files++
	}
	var pdbList policyv1.PodDisruptionBudgetList
	if c.List(ctx, &pdbList, client.InNamespace(namespace), inst) == nil && len(pdbList.Items) > 0 {
		_ = writeYAML(outDir, "poddisruptionbudgets.yaml", &pdbList)
		files++
	}
	var podList corev1.PodList
	if c.List(ctx, &podList, client.InNamespace(namespace), inst) == nil && len(podList.Items) > 0 {
		_ = writeYAML(outDir, "pods.yaml", &podList)
		files++
	}

	// Events involving this cluster's objects (name prefix match — owned
	// objects are all named after the cluster).
	var evList corev1.EventList
	if c.List(ctx, &evList, client.InNamespace(namespace)) == nil {
		var mine corev1.EventList
		for i := range evList.Items {
			if strings.HasPrefix(evList.Items[i].InvolvedObject.Name, name) {
				mine.Items = append(mine.Items, evList.Items[i])
			}
		}
		if len(mine.Items) > 0 {
			_ = writeYAML(outDir, "events.yaml", &mine)
			files++
		}
	}

	_, _ = fmt.Fprintf(out, "wrote %d file(s) to %s\n", files, outDir)
	return nil
}

func writeYAML(dir, file string, obj any) error {
	b, err := yaml.Marshal(obj)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", file, err)
	}
	if err := os.WriteFile(filepath.Join(dir, file), b, 0o644); err != nil { // #nosec G306 -- diagnostic dump
		return fmt.Errorf("write %s: %w", file, err)
	}
	return nil
}
