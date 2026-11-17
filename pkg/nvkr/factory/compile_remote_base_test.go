// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package factory

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/dsx-ai-factory/nvkr/pkg/nvkr/catalog"
	e "github.com/dsx-ai-factory/nvkr/pkg/nvkr/errors"
	"sigs.k8s.io/yaml"
)

const remoteBaseRoot = "https://raw.githubusercontent.com/acme/catalogs/refs/heads/main/catalog/"

func TestCompileSnapshotCapturesRemoteBase(t *testing.T) {
	root, _, _ := snapshotFixture(t)
	before, _, err := CaptureCompileFiles(t.Context(), root, snapshotDefinition, snapshotEnvironment, "snapshot-test")
	if err != nil {
		t.Fatal(err)
	}
	remote := map[string][]byte{}
	for _, name := range []string{catalog.RegistryFile, "patches/version.patch"} {
		raw, err := os.ReadFile(filepath.Join(root, "catalog", filepath.FromSlash(name)))
		if err != nil {
			t.Fatal(err)
		}
		remote[remoteBaseRoot+name] = raw
	}
	fetches := 0
	fetch := func(_ context.Context, location string) ([]byte, error) {
		fetches++
		raw, ok := remote[location]
		if !ok {
			return nil, e.New(e.CodeFetchFailed, "GET %s returned HTTP 404 Not Found", location)
		}
		return raw, nil
	}
	previous := captureSource
	captureSource = func(root string) inputSource { return catalog.DirWithRemote(root, fetch).(inputSource) }
	t.Cleanup(func() { captureSource = previous })

	var base map[string]any
	if err := yaml.Unmarshal(remote[remoteBaseRoot+catalog.RegistryFile], &base); err != nil {
		t.Fatal(err)
	}
	child := map[string]any{"apiVersion": base["apiVersion"], "kind": base["kind"], "metadata": base["metadata"],
		"extends": map[string]any{"source": map[string]any{"url": remoteBaseRoot + catalog.RegistryFile}}}
	writeSnapshotFixtureDocument(t, root, "catalog/registry.yaml", child)
	if err := os.RemoveAll(filepath.Join(root, "catalog", "patches")); err != nil {
		t.Fatal(err)
	}

	after, snapshot, err := CaptureCompileFiles(t.Context(), root, snapshotDefinition, snapshotEnvironment, "snapshot-test")
	if err != nil {
		t.Fatal(err)
	}
	want, err := before.Digest()
	if err != nil {
		t.Fatal(err)
	}
	got, err := after.Digest()
	if err != nil || got != want {
		t.Fatalf("remote base changed Plan identity: %s / %s: %v", got, want, err)
	}
	if len(snapshot.Remote) != len(remote) {
		t.Fatalf("snapshot captured %d remote inputs, want %d", len(snapshot.Remote), len(remote))
	}
	for _, input := range snapshot.Remote {
		if !strings.HasPrefix(input.URL, remoteBaseRoot) || string(input.Data) != string(remote[input.URL]) {
			t.Fatalf("snapshot captured unexpected remote input %s", input.URL)
		}
	}

	fetches = 0
	replay, err := CompileSnapshot(t.Context(), snapshot, "snapshot-test")
	if err != nil {
		t.Fatal(err)
	}
	if got, err = replay.Digest(); err != nil || got != want || fetches != 0 {
		t.Fatalf("replay digest %s fetches %d: %v", got, fetches, err)
	}
	for _, input := range snapshot.Remote {
		incomplete := snapshot
		incomplete.Remote = slices.DeleteFunc(slices.Clone(snapshot.Remote), func(r InputRemote) bool { return r.URL == input.URL })
		_, err := CompileSnapshot(t.Context(), incomplete, "snapshot-test")
		assertFactoryErrorCode(t, err, e.CodeInvalidCatalog)
	}
	tampered := snapshot
	tampered.Remote = slices.Clone(snapshot.Remote)
	tampered.Remote[0].Data = append([]byte("# tampered\n"), tampered.Remote[0].Data...)
	if _, err := CompileSnapshot(t.Context(), tampered, "snapshot-test"); err == nil {
		t.Fatal("replay admitted tampered remote bytes")
	}
	unsorted := snapshot
	unsorted.Remote = []InputRemote{snapshot.Remote[1], snapshot.Remote[0]}
	_, err = CompileSnapshot(t.Context(), unsorted, "snapshot-test")
	assertFactoryErrorCode(t, err, e.CodeInvalidCatalog)
}
