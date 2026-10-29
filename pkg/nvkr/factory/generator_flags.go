// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package factory

// UnstampedGeneratorVersion and UnstampedGeneratorCommit are the generator
// label parts of a build without -X main.version and -X main.commit stamps.
const (
	UnstampedGeneratorVersion = "dev"
	UnstampedGeneratorCommit  = "unknown"
)

// GeneratorVersion returns the generator label that Plans record and that
// generated bootstraps stamp into the runner. Linker stamps win. A `go install
// …/cmd/nvkr@vX.Y.Z` build cannot carry stamps, so an unstamped build whose
// build information records an exact release is labeled with that release;
// module-mode builds record no VCS revision, so its commit stays unknown. Every
// other unstamped build keeps the development label, so checkout builds
// regenerate committed bundles independently of the checked-out commit.
func GeneratorVersion(version, commit string) string {
	if version == UnstampedGeneratorVersion && commit == UnstampedGeneratorCommit {
		if info, ok := readGitHubGeneratorBuildInfo(); releasedGeneratorBuild(info, ok) {
			version = info.Main.Version
		}
	}
	return version + " (" + commit + ")"
}

// GeneratorLinkerFlags returns the validated Go linker flags that reproduce a
// Plan's generator version label. Adapters use the same parser as Run rendering;
// this descriptive label grants no build or release authority.
func GeneratorLinkerFlags(version string) (string, error) {
	return generatorLinkerFlags(version)
}
