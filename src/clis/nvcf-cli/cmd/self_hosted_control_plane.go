/*
SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0

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

package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"nvcf-cli/internal/selfhosted/controlplaneprofile"
)

var (
	controlPlaneProfileValidateFile    string
	controlPlaneProfileValidateRequire string
)

var selfHostedControlPlaneCmd = &cobra.Command{
	Use:   "control-plane",
	Short: "Manage self-hosted control-plane artifacts",
}

var selfHostedControlPlaneProfileCmd = &cobra.Command{
	Use:   "profile",
	Short: "Manage self-hosted control-plane profile files",
}

var selfHostedControlPlaneProfileValidateCmd = &cobra.Command{
	Use:          "validate",
	Short:        "Validate a self-hosted control-plane profile file",
	SilenceUsage: true,
	RunE:         runControlPlaneProfileValidate,
}

func init() {
	selfHostedCmd.AddCommand(selfHostedControlPlaneCmd)
	selfHostedControlPlaneCmd.AddCommand(selfHostedControlPlaneProfileCmd)
	selfHostedControlPlaneProfileCmd.AddCommand(selfHostedControlPlaneProfileValidateCmd)

	selfHostedControlPlaneProfileValidateCmd.Flags().StringVar(&controlPlaneProfileValidateFile, "file", "", "Path to control-plane profile YAML")
	_ = selfHostedControlPlaneProfileValidateCmd.MarkFlagRequired("file")
	selfHostedControlPlaneProfileValidateCmd.Flags().StringVar(&controlPlaneProfileValidateRequire, "require", string(controlplaneprofile.RequireAny),
		"Endpoint scope to require: any, in-cluster, compute-reachable, or both")
}

func runControlPlaneProfileValidate(c *cobra.Command, _ []string) error {
	require, err := parseControlPlaneProfileRequireMode(controlPlaneProfileValidateRequire)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(controlPlaneProfileValidateFile)
	if err != nil {
		return fmt.Errorf("read control-plane profile %q: %w", controlPlaneProfileValidateFile, err)
	}
	result, err := controlplaneprofile.ParseAndValidate(data, controlplaneprofile.ValidateOptions{Require: require})
	if err != nil {
		return err
	}
	fmt.Fprintln(c.OutOrStdout(), "control-plane profile is valid")
	fmt.Fprintln(c.OutOrStdout(), result.Summary())
	return nil
}

func parseControlPlaneProfileRequireMode(value string) (controlplaneprofile.RequireMode, error) {
	if value == "" {
		return controlplaneprofile.RequireAny, nil
	}
	switch controlplaneprofile.RequireMode(value) {
	case controlplaneprofile.RequireAny, controlplaneprofile.RequireInCluster, controlplaneprofile.RequireComputeReachable, controlplaneprofile.RequireBoth:
		return controlplaneprofile.RequireMode(value), nil
	default:
		return "", fmt.Errorf("--require must be one of any, in-cluster, compute-reachable, or both")
	}
}
