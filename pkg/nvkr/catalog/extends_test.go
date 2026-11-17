// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package catalog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	e "github.com/dsx-ai-factory/nvkr/pkg/nvkr/errors"
	"sigs.k8s.io/yaml"
)

const (
	extendsRoot  = "https://raw.githubusercontent.com/acme/catalogs/refs/tags/v0.1.0/"
	extendsBase  = extendsRoot + "catalogs/base/registry.yaml"
	extendsPatch = "diff --git a/README b/README\n"
)

// extendsFixture is a remote base exercising every relative input kind: a
// sibling fragment, a patch file and a file-backed command operand.
type extendsFixture struct {
	files map[string][]byte // repository-relative path -> bytes
	base  *registryDoc      // the base declarations before operand lowering

	mu      sync.Mutex
	fetched []string
}

func newExtendsFixture(t *testing.T) *extendsFixture {
	t.Helper()
	provider, err := BindContext(t.Context(), Dir("../../../testdata/catalogs/kubernetes-layered"))
	if err != nil {
		t.Fatal(err)
	}
	reg, err := readRegistry(provider)
	if err != nil {
		t.Fatal(err)
	}
	fragment, err := yaml.Marshal(registryFragment{APIVersion: "nvkr.dev/catalog-fragment/v1", Kind: "CatalogFragment", Sources: reg.Sources})
	if err != nil {
		t.Fatal(err)
	}
	imp := fragmentImport{Path: "../shared/sources.yaml", SHA256: fmt.Sprintf("%x", sha256.Sum256(fragment)), Sources: map[string]string{}}
	for name := range reg.Sources {
		imp.Sources[name] = name
	}
	reg.Sources = nil
	reg.Imports = []fragmentImport{imp}
	containerd := reg.Components["containerd"]
	containerd.Patches = []patchDeclaration{{Name: "patches/fix.patch", SHA256: digestOf([]byte(extendsPatch))}}
	reg.Components["containerd"] = containerd
	raw, err := yaml.Marshal(reg)
	if err != nil {
		t.Fatal(err)
	}
	operand := "    - ./build_linux.sh\n"
	if strings.Count(string(raw), operand) != 1 {
		t.Fatal("fixture operand is not unique")
	}
	raw = []byte(strings.Replace(string(raw), operand, "    - file: scripts/build.sh\n", 1))
	return &extendsFixture{base: reg, files: map[string][]byte{
		"catalogs/base/registry.yaml":       raw,
		"catalogs/shared/sources.yaml":      fragment,
		"catalogs/base/patches/fix.patch":   []byte(extendsPatch),
		"catalogs/base/scripts/build.sh":    []byte("./build_linux.sh"),
		"catalogs/base/" + LockFile:         lockFixture(t),
		"catalogs/unrelated/registry.yaml":  []byte("unused"),
		"catalogs/base/patches/other.patch": []byte("other"),
	}}
}

func lockFixture(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile("../../../testdata/catalogs/kubernetes-layered/" + LockFile)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func digestOf(raw []byte) string {
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func (f *extendsFixture) fetch(_ context.Context, location string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fetched = append(f.fetched, location)
	raw, ok := f.files[strings.TrimPrefix(location, extendsRoot)]
	if !strings.HasPrefix(location, extendsRoot) || !ok {
		return nil, e.New(e.CodeFetchFailed, "GET %s returned HTTP 404 Not Found", location)
	}
	return append([]byte(nil), raw...), nil
}

// local writes the base inputs to disk so the remote load can be compared with
// an ordinary directory load of identical bytes.
func (f *extendsFixture) local(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for name, raw := range f.files {
		target := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
			t.Fatal(err)
		}
		writeFragmentFixture(t, target, raw)
	}
	return filepath.Join(root, "catalogs", "base")
}

type extendsChild struct {
	APIVersion string                  `json:"apiVersion"`
	Kind       string                  `json:"kind"`
	Metadata   Metadata                `json:"metadata"`
	Extends    *registryExtends        `json:"extends,omitempty"`
	Sources    map[string]Source       `json:"sources,omitempty"`
	Components map[string]rawComponent `json:"components,omitempty"`
}

func (f *extendsFixture) child(t *testing.T) extendsChild {
	t.Helper()
	return extendsChild{
		APIVersion: catalogAPIVersion, Kind: catalogKind,
		Extends: &registryExtends{Source: remoteBaseSource{URL: extendsBase}},
	}
}

func writeExtendsChild(t *testing.T, dir string, child extendsChild, lock []byte) {
	t.Helper()
	raw, err := yaml.Marshal(child)
	if err != nil {
		t.Fatal(err)
	}
	writeFragmentFixture(t, filepath.Join(dir, RegistryFile), raw)
	writeFragmentFixture(t, filepath.Join(dir, LockFile), lock)
}

func patchOperations(t *testing.T, operations ...string) []json.RawMessage {
	t.Helper()
	result := make([]json.RawMessage, 0, len(operations))
	for _, operation := range operations {
		if !json.Valid([]byte(operation)) {
			t.Fatalf("invalid operation %s", operation)
		}
		result = append(result, json.RawMessage(operation))
	}
	return result
}

func TestExtendsMatchesLocalCatalog(t *testing.T) {
	fixture := newExtendsFixture(t)
	want, err := LoadContext(t.Context(), Dir(fixture.local(t)))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	writeExtendsChild(t, dir, fixture.child(t), lockFixture(t))
	got, err := LoadContext(t.Context(), DirWithRemote(dir, fixture.fetch))
	if err != nil {
		t.Fatal(err)
	}
	wantDigest, err := want.SnapshotDigest()
	if err != nil {
		t.Fatal(err)
	}
	gotDigest, err := got.SnapshotDigest()
	if err != nil || gotDigest != wantDigest {
		t.Fatalf("resolved catalog differs: %s / %s: %v", gotDigest, wantDigest, err)
	}
	if got.Components["containerd"].Patches[0].Content != extendsPatch {
		t.Fatal("base patch bytes were not served")
	}
	if got.Components["cni-plugins"].BuildCommand[1] != "./build_linux.sh" {
		t.Fatal("base command file was not expanded")
	}
	for _, location := range fixture.fetched {
		if !strings.HasPrefix(location, extendsRoot) {
			t.Fatalf("fetched %s outside the pinned commit", location)
		}
		if path.Base(location) == LockFile {
			t.Fatal("the base lock must not be consulted")
		}
	}
}

func TestExtendsAppliesPatchesAndUnion(t *testing.T) {
	fixture := newExtendsFixture(t)
	child := fixture.child(t)
	child.Extends.Patches = patchOperations(t,
		`{"op":"test","path":"/components/crictl/source","value":"crictl"}`,
		`{"op":"replace","path":"/components/crictl/platforms","value":["linux/arm64"]}`,
		`{"op":"remove","path":"/components/containerd/patches"}`,
	)
	child.Components = map[string]rawComponent{"local-crictl": fixture.component(t, "crictl")}
	dir := t.TempDir()
	writeExtendsChild(t, dir, child, lockFixture(t))
	got, err := LoadContext(t.Context(), DirWithRemote(dir, fixture.fetch))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Components["crictl"].Platforms, []string{"linux/arm64"}) {
		t.Fatalf("platforms = %v", got.Components["crictl"].Platforms)
	}
	if len(got.Components["containerd"].Patches) != 0 {
		t.Fatal("patch-file removal was not applied")
	}
	if _, ok := got.Components["local-crictl"]; !ok {
		t.Fatal("child component missing from union")
	}
	if slices.Contains(fixture.fetched, extendsRoot+"catalogs/base/patches/fix.patch") {
		t.Fatal("dropped base patch file was fetched")
	}
}

func (f *extendsFixture) component(t *testing.T, name string) rawComponent {
	t.Helper()
	return f.base.Components[name]
}

func TestExtendsFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*extendsFixture, *extendsChild)
		code   e.Code
	}{
		{"missing source", func(_ *extendsFixture, c *extendsChild) { c.Extends.Source = remoteBaseSource{} }, e.CodeInvalidCatalog},
		{"http", func(_ *extendsFixture, c *extendsChild) {
			c.Extends.Source.URL = strings.Replace(c.Extends.Source.URL, "https", "http", 1)
		}, e.CodeInvalidCatalog},
		{"port", func(_ *extendsFixture, c *extendsChild) {
			c.Extends.Source.URL = strings.Replace(c.Extends.Source.URL, "githubusercontent.com", "githubusercontent.com:443", 1)
		}, e.CodeInvalidCatalog},
		{"query", func(_ *extendsFixture, c *extendsChild) { c.Extends.Source.URL += "?token=x" }, e.CodeInvalidCatalog},
		{"fragment", func(_ *extendsFixture, c *extendsChild) { c.Extends.Source.URL += "#x" }, e.CodeInvalidCatalog},
		{"escape", func(_ *extendsFixture, c *extendsChild) {
			c.Extends.Source.URL = strings.Replace(c.Extends.Source.URL, "base/", "base%2F", 1)
		}, e.CodeInvalidCatalog},
		{"dot segment", func(_ *extendsFixture, c *extendsChild) {
			c.Extends.Source.URL = strings.Replace(c.Extends.Source.URL, "base/", "unrelated/../base/", 1)
		}, e.CodeInvalidCatalog},
		{"not a registry", func(_ *extendsFixture, c *extendsChild) {
			c.Extends.Source.URL = strings.Replace(c.Extends.Source.URL, RegistryFile, "other.yaml", 1)
		}, e.CodeInvalidCatalog},
		{"missing base", func(_ *extendsFixture, c *extendsChild) {
			c.Extends.Source.URL = strings.Replace(c.Extends.Source.URL, "base/", "missing/", 1)
		}, e.CodeFetchFailed},
		{"nested extends", func(f *extendsFixture, _ *extendsChild) {
			f.files["catalogs/base/registry.yaml"] = append(f.files["catalogs/base/registry.yaml"], []byte("extends:\n  source:\n    url: "+extendsBase+"\n")...)
		}, e.CodeInvalidCatalog},
		{"unknown base field", func(f *extendsFixture, _ *extendsChild) {
			f.files["catalogs/base/registry.yaml"] = append(f.files["catalogs/base/registry.yaml"], []byte("recipes: {}\n")...)
		}, e.CodeInvalidCatalog},
		{"changed base fragment", func(f *extendsFixture, _ *extendsChild) {
			f.files["catalogs/shared/sources.yaml"] = append(f.files["catalogs/shared/sources.yaml"], '\n')
		}, e.CodeInvalidCatalog},
		{"changed base patch file", func(f *extendsFixture, _ *extendsChild) {
			f.files["catalogs/base/patches/fix.patch"] = []byte("tampered")
		}, e.CodeUnpinnedSource},
		{"missing base command file", func(f *extendsFixture, _ *extendsChild) {
			delete(f.files, "catalogs/base/scripts/build.sh")
		}, e.CodeInvalidCatalog},
		{"source collision", func(_ *extendsFixture, c *extendsChild) { c.Sources = map[string]Source{"containerd": {}} }, e.CodeInvalidCatalog},
		{"component collision", func(f *extendsFixture, c *extendsChild) {
			c.Components = map[string]rawComponent{"crictl": f.component(t, "crictl")}
		}, e.CodeInvalidCatalog},
		{"patch outside declarations", func(_ *extendsFixture, c *extendsChild) {
			c.Extends.Patches = patchOperations(t, `{"op":"replace","path":"/metadata/name","value":"x"}`)
		}, e.CodeInvalidCatalog},
		{"patch move from outside", func(_ *extendsFixture, c *extendsChild) {
			c.Extends.Patches = patchOperations(t, `{"op":"copy","from":"/imports","path":"/components/x"}`)
		}, e.CodeInvalidCatalog},
		{"failed test", func(_ *extendsFixture, c *extendsChild) {
			c.Extends.Patches = patchOperations(t, `{"op":"test","path":"/components/crictl/source","value":"other"}`)
		}, e.CodeInvalidCatalog},
		{"remove missing", func(_ *extendsFixture, c *extendsChild) {
			c.Extends.Patches = patchOperations(t, `{"op":"remove","path":"/components/missing"}`)
		}, e.CodeInvalidCatalog},
		{"unknown op", func(_ *extendsFixture, c *extendsChild) {
			c.Extends.Patches = patchOperations(t, `{"op":"merge","path":"/components/crictl","value":{}}`)
		}, e.CodeInvalidCatalog},
		{"patch introduces unknown field", func(_ *extendsFixture, c *extendsChild) {
			c.Extends.Patches = patchOperations(t, `{"op":"add","path":"/components/crictl/recipe","value":"x"}`)
		}, e.CodeInvalidCatalog},
		{"patch adds patch file", func(_ *extendsFixture, c *extendsChild) {
			c.Extends.Patches = patchOperations(t, fmt.Sprintf(`{"op":"add","path":"/components/crictl/patches","value":[{"name":"patches/other.patch","sha256":%q}]}`, digestOf([]byte("other"))))
		}, e.CodeInvalidCatalog},
		{"patch alters patch digest", func(_ *extendsFixture, c *extendsChild) {
			c.Extends.Patches = patchOperations(t, fmt.Sprintf(`{"op":"replace","path":"/components/containerd/patches/0/sha256","value":%q}`, digestOf([]byte("tampered"))))
		}, e.CodeInvalidCatalog},
		{"too many operations", func(_ *extendsFixture, c *extendsChild) {
			for range MaxBasePatchOperations + 1 {
				c.Extends.Patches = append(c.Extends.Patches, patchOperations(t, `{"op":"test","path":"/components/crictl/source","value":"crictl"}`)...)
			}
		}, e.CodeInvalidCatalog},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newExtendsFixture(t)
			child := fixture.child(t)
			tc.mutate(fixture, &child)
			dir := t.TempDir()
			writeExtendsChild(t, dir, child, lockFixture(t))
			_, err := LoadContext(t.Context(), DirWithRemote(dir, fixture.fetch))
			if got := catalogErrorCode(err); got != tc.code {
				t.Fatalf("code = %s, want %s: %v", got, tc.code, err)
			}
		})
	}
}

// remoteLessProvider is a context provider without the remote capability.
type remoteLessProvider struct{ root string }

func (p remoteLessProvider) ReadFile(name string) ([]byte, error) { return nil, fs.ErrInvalid }
func (p remoteLessProvider) ReadDir(string) ([]string, error)     { return nil, nil }
func (p remoteLessProvider) Name() string                         { return "remote-less" }
func (p remoteLessProvider) ReadFileContext(ctx context.Context, name string) ([]byte, error) {
	return Dir(p.root).(dirProvider).ReadFileContext(ctx, name)
}
func (p remoteLessProvider) ReadDirContext(context.Context, string) ([]string, error) {
	return nil, nil
}

func TestExtendsProviderAdmissionAndBudget(t *testing.T) {
	fixture := newExtendsFixture(t)
	dir := t.TempDir()
	writeExtendsChild(t, dir, fixture.child(t), lockFixture(t))
	if _, err := LoadContext(t.Context(), remoteLessProvider{root: dir}); catalogErrorCode(err) != e.CodeEngineCapability {
		t.Fatalf("remote-less provider: %v", err)
	}
	provider, err := BindContext(t.Context(), DirWithRemote(dir, fixture.fetch))
	if err != nil {
		t.Fatal(err)
	}
	provider.(*boundContextProvider).totalBytes = MaxCatalogTotalBytes - 1
	if _, err := provider.(*boundContextProvider).readRemote(extendsBase); catalogErrorCode(err) != e.CodeInvalidCatalog {
		t.Fatalf("aggregate bound: %v", err)
	}
	oversized := func(context.Context, string) ([]byte, error) {
		return make([]byte, MaxCatalogFileBytes+1), nil
	}
	if _, err := LoadContext(t.Context(), DirWithRemote(dir, oversized)); catalogErrorCode(err) != e.CodeInvalidCatalog {
		t.Fatalf("file bound: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := LoadContext(ctx, DirWithRemote(dir, fixture.fetch)); catalogErrorCode(err) != e.CodeTimeout {
		t.Fatalf("canceled: %v", err)
	}
}

func TestExtendsLockMatchesMergedKeySetAndWritesUnchanged(t *testing.T) {
	fixture := newExtendsFixture(t)
	previous := defaultRemoteFetch
	defaultRemoteFetch = fixture.fetch
	t.Cleanup(func() { defaultRemoteFetch = previous })

	dir := t.TempDir()
	writeExtendsChild(t, dir, fixture.child(t), lockFixture(t))
	lock, err := LoadLockContext(t.Context(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteLockContext(t.Context(), dir, lock); err != nil {
		t.Fatal(err)
	}
	written, err := os.ReadFile(filepath.Join(dir, LockFile))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(written), "extends") || strings.Contains(string(written), "v0.1.0") {
		t.Fatal("lock must not record inheritance")
	}

	child := fixture.child(t)
	child.Extends.Patches = patchOperations(t, `{"op":"remove","path":"/sources/containerd"}`, `{"op":"remove","path":"/components/containerd"}`)
	writeExtendsChild(t, dir, child, lockFixture(t))
	if _, err := LoadContext(t.Context(), Dir(dir)); catalogErrorCode(err) != e.CodeInvalidCatalog {
		t.Fatalf("orphan pin for a removed base source: %v", err)
	}
}

// TestExtendsNVKEReleaseExample replays the NVKE release example: a registry that
// holds only a tag-addressed base URL and constraint patches, with the complete
// merged lock. The base bytes are inputs/catalogs/nvke/release at nvkr v0.1.0.
// Pin-set rederivation needs the NVKE upstream and is outside this offline test.
func TestExtendsNVKEReleaseExample(t *testing.T) {
	const (
		root    = "testdata/extends-nvke-v0.1.0/"
		baseURL = "https://raw.githubusercontent.com/dsx-ai-factory/nvkr/refs/tags/v0.1.0/inputs/catalogs/nvke/release/"
	)
	fetched := map[string]int{}
	fetch := func(_ context.Context, location string) ([]byte, error) {
		fetched[location]++
		name, ok := strings.CutPrefix(location, baseURL)
		if !ok {
			return nil, e.New(e.CodeFetchFailed, "GET %s returned HTTP 404 Not Found", location)
		}
		return os.ReadFile(filepath.Join(root, "base", filepath.FromSlash(name)))
	}
	provider, err := BindContext(t.Context(), DirWithRemote(root+"child", fetch))
	if err != nil {
		t.Fatal(err)
	}
	reg, err := readRegistry(provider)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"nvke", "nvke-cli", "nvke-helm"} {
		if got := reg.Sources[name].Constraint; got != "v0.23.1" {
			t.Errorf("source %s constraint = %q, want the patched v0.23.1", name, got)
		}
	}
	if reg.Metadata.Name != "nvke-controller" {
		t.Errorf("metadata = %+v, want the base's", reg.Metadata)
	}
	lock, err := readLock(provider)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateLockAgainstRegistry(reg, lock, true); err != nil {
		t.Fatalf("example lock does not match the merged registry: %v", err)
	}
	component := "nvke-controller-profiles-image"
	resolved, err := resolveComponent(component, reg.Components[component], reg.Sources, lock)
	if err != nil {
		t.Fatal(err)
	}
	if err := resolvePatches(reg.patchProvider(component, provider), component, &resolved, reg.Components[component].Patches); err != nil {
		t.Fatal(err)
	}
	if resolved.Commit != "347baabc64ba320176c6c2ef956e555350124d19" || len(resolved.Patches) != 1 {
		t.Fatalf("component did not resolve against the example lock: commit %s, %d patches", resolved.Commit, len(resolved.Patches))
	}
	if fetched[baseURL+RegistryFile] != 1 || fetched[baseURL+"patches/profiles-image.patch"] != 1 {
		t.Fatalf("unexpected fetches: %v", fetched)
	}
}
