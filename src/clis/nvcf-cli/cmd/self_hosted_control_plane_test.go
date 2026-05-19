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
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestControlPlaneProfileValidateCommandSucceeds(t *testing.T) {
	path := writeControlPlaneProfileFixture(t, validControlPlaneProfileYAML())
	resetControlPlaneProfileValidateCommand(t)

	var stdout bytes.Buffer
	rootCmd.SetOut(&stdout)
	rootCmd.SetErr(&bytes.Buffer{})
	rootCmd.SetArgs([]string{
		"self-hosted", "control-plane", "profile", "validate",
		"--file", path,
		"--require", "both",
	})

	err := rootCmd.Execute()
	require.NoError(t, err)

	assert.Contains(t, stdout.String(), "control-plane profile is valid")
	assert.Contains(t, stdout.String(), "in-cluster: usable")
	assert.Contains(t, stdout.String(), "compute-reachable: usable")
}

func TestControlPlaneProfileValidateCommandFailsWithFieldErrors(t *testing.T) {
	doc := removeLine(validControlPlaneProfileYAML(), "      natsURL: tls://nats.nvcf-cp.internal:4222")
	path := writeControlPlaneProfileFixture(t, doc)
	resetControlPlaneProfileValidateCommand(t)

	rootCmd.SetOut(&bytes.Buffer{})
	rootCmd.SetErr(&bytes.Buffer{})
	rootCmd.SetArgs([]string{
		"self-hosted", "control-plane", "profile", "validate",
		"--file", path,
		"--require", "compute-reachable",
	})

	err := rootCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "controlPlane.endpoints.computeReachable.natsURL")
}

func TestControlPlaneProfileValidateCommandHelpShowsAnyRequireMode(t *testing.T) {
	resetControlPlaneProfileValidateCommand(t)

	var stdout bytes.Buffer
	rootCmd.SetOut(&stdout)
	rootCmd.SetErr(&bytes.Buffer{})
	rootCmd.SetArgs([]string{
		"self-hosted", "control-plane", "profile", "validate",
		"--help",
	})

	err := rootCmd.Execute()
	require.NoError(t, err)
	assert.Contains(t, stdout.String(), "any")
	assert.Contains(t, stdout.String(), "Endpoint scope to require")
}

func TestParseControlPlaneProfileRequireModeAcceptsAny(t *testing.T) {
	requireMode, err := parseControlPlaneProfileRequireMode("any")
	require.NoError(t, err)
	assert.Equal(t, "any", string(requireMode))
}

func resetControlPlaneProfileValidateCommand(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		rootCmd.SetOut(os.Stdout)
		rootCmd.SetErr(os.Stderr)
		rootCmd.SetArgs(nil)
		controlPlaneProfileValidateFile = ""
		controlPlaneProfileValidateRequire = ""
	})
}

func writeControlPlaneProfileFixture(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "control-plane-profile.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func removeLine(content, needle string) string {
	lines := bytes.Split([]byte(content), []byte("\n"))
	out := lines[:0]
	for _, line := range lines {
		if string(line) == needle {
			continue
		}
		out = append(out, line)
	}
	return string(bytes.Join(out, []byte("\n")))
}

func validControlPlaneProfileYAML() string {
	return `apiVersion: nvcf.nvidia.com/v1alpha1
kind: ControlPlaneProfile

controlPlane:
  clusterName: nvcf-cp-euw1
  ncaID: nvcf-default
  region: eu-west-1

  endpoints:
    inCluster:
      icmsURL: http://api.sis.svc.cluster.local:8080
      revalURL: http://reval.nvcf.svc.cluster.local:8080
      natsURL: nats://nats.nats-system.svc.cluster.local:4222

    computeReachable:
      icmsURL: https://sis.nvcf-cp.internal
      revalURL: https://reval.nvcf-cp.internal
      natsURL: tls://nats.nvcf-cp.internal:4222

  gateway:
    httpURL: https://api.nvcf-cp.internal
    grpcURL: api.nvcf-cp.internal:10081

  hosts:
    api: api.nvcf-cp.internal
    apiKeys: api-keys.nvcf-cp.internal
    sis: sis.nvcf-cp.internal
    reval: reval.nvcf-cp.internal
    nats: nats.nvcf-cp.internal
    invocation: invocation.nvcf-cp.internal
`
}
