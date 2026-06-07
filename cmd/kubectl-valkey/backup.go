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

	"github.com/spf13/cobra"
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func newBackupCmd() *cobra.Command {
	var namespace string
	cmd := &cobra.Command{
		Use:   "backup <cluster>",
		Short: "Trigger an on-demand backup",
		Long: "Instantiate a one-off backup Job from the cluster's backup CronJob " +
			"(<cluster>-backup), the same way `kubectl create job --from=cronjob/...` does. " +
			"Requires spec.backup.enabled on the ValkeyCluster.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runBackup(cmd.Context(), cmd.OutOrStdout(), namespace, args[0])
		},
	}
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace (defaults to current kube-context)")
	return cmd
}

func runBackup(ctx context.Context, out io.Writer, namespace, name string) error {
	c, err := newClient()
	if err != nil {
		return err
	}
	namespace = resolveNamespace(namespace)

	cronName := name + "-backup"
	var cj batchv1.CronJob
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: cronName}, &cj); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf(
				"no backup CronJob %q in namespace %s — set spec.backup.enabled on the ValkeyCluster",
				cronName, namespace)
		}
		return fmt.Errorf("get CronJob %q: %w", cronName, err)
	}

	job := jobFromCronJob(&cj)
	if err := c.Create(ctx, job); err != nil {
		return fmt.Errorf("create backup job: %w", err)
	}
	_, _ = fmt.Fprintf(out, "created backup job %s/%s\n", namespace, job.Name)
	return nil
}

// jobFromCronJob materializes a one-off Job from a CronJob's JobTemplate,
// mirroring `kubectl create job --from=cronjob/<name>`. GenerateName lets the
// API server assign a unique suffix.
func jobFromCronJob(cj *batchv1.CronJob) *batchv1.Job {
	labels := map[string]string{}
	maps.Copy(labels, cj.Spec.JobTemplate.Labels)
	labels["valkey.wellcake.io/manual-backup"] = valueTrue

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: cj.Name + "-manual-",
			Namespace:    cj.Namespace,
			Labels:       labels,
			Annotations:  map[string]string{"cronjob.kubernetes.io/instantiate": "manual"},
		},
		Spec: cj.Spec.JobTemplate.Spec,
	}
}
