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

// kubectl-valkey is a kubectl plugin for the valkey-operator. Built as a
// binary named `kubectl-valkey`, kubectl invokes it as `kubectl valkey ...`.
package main

import (
	"os"

	"github.com/spf13/cobra"
)

func main() {
	root := &cobra.Command{
		Use:           valkeyCtr,
		Short:         "kubectl plugin for the valkey-operator",
		Long:          "Operational commands for ValkeyCluster resources managed by the valkey-operator.",
		SilenceUsage:  true,
		SilenceErrors: false,
	}
	root.AddCommand(newStatusCmd())
	root.AddCommand(newCliCmd())
	root.AddCommand(newBackupCmd())
	root.AddCommand(newRestartCmd())
	root.AddCommand(newReshardCmd())
	root.AddCommand(newFailoverCmd())
	root.AddCommand(newCertificateCmd())
	root.AddCommand(newHibernateCmd())
	root.AddCommand(newReportCmd())

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}
