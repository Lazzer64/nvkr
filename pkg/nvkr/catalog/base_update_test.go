// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package catalog

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	e "github.com/dsx-ai-factory/nvkr/pkg/nvkr/errors"
)

const otherCommit = "1111111111111111111111111111111111111111"

// fakeRefs advertises refs/heads/release/v1 and an annotated refs/tags/v1.
func fakeRefs(_ context.Context, repository string, refs ...string) (string, error) {
	if repository != "https://github.com/acme/catalogs.git" {
		return "", e.New(e.CodeFetchFailed, "repository not found")
	}
	advertised := map[string]string{
		"refs/heads/main":       otherCommit,
		"refs/heads/release/v1": extendsCommit,
		"refs/tags/v1":          "2222222222222222222222222222222222222222",
		"refs/tags/v1^{}":       extendsCommit,
	}
	var out strings.Builder
	for _, ref := range []string{"refs/heads/main", "refs/heads/release/v1", "refs/tags/v1", "refs/tags/v1^{}"} {
		for _, pattern := range refs {
			if pattern == ref || (strings.HasSuffix(pattern, "/*") && strings.HasPrefix(ref, strings.TrimSuffix(pattern, "*"))) {
				out.WriteString(advertised[ref] + "\t" + ref + "\n")
				break
			}
		}
	}
	return out.String(), nil
}

func baseUpdateFixture(t *testing.T, withExtends bool) (*extendsFixture, string) {
	t.Helper()
	fixture := newExtendsFixture(t)
	previous := defaultRemoteFetch
	defaultRemoteFetch = fixture.fetch
	t.Cleanup(func() { defaultRemoteFetch = previous })
	child := fixture.child(t)
	if withExtends {
		child.Extends.URL = strings.Replace(child.Extends.URL, extendsCommit, otherCommit, 1)
		child.Extends.SHA256 = digestOf([]byte("previous base"))
	} else {
		child.Extends = nil
	}
	dir := t.TempDir()
	writeExtendsChild(t, dir, child, lockFixture(t))
	return fixture, dir
}

func TestUpdateBaseResolvesRefsAndRewritesOnlyBaseScalars(t *testing.T) {
	for _, tc := range []struct {
		name        string
		withExtends bool
		req         BaseUpdateRequest
		ref         string
	}{
		{"branch url with slash", false, BaseUpdateRequest{URL: "https://raw.githubusercontent.com/acme/catalogs/refs/heads/release/v1/catalogs/base/registry.yaml"}, "refs/heads/release/v1"},
		{"annotated tag url", false, BaseUpdateRequest{URL: "https://raw.githubusercontent.com/acme/catalogs/refs/tags/v1/catalogs/base/registry.yaml"}, "refs/tags/v1"},
		{"commit url", false, BaseUpdateRequest{URL: extendsBase}, extendsCommit},
		{"ref on existing base", true, BaseUpdateRequest{Ref: "refs/tags/v1"}, "refs/tags/v1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture, dir := baseUpdateFixture(t, tc.withExtends)
			before, err := os.ReadFile(filepath.Join(dir, RegistryFile))
			if err != nil {
				t.Fatal(err)
			}
			tc.req.Directory = dir
			update, err := updateBase(t.Context(), tc.req, fakeRefs)
			if err != nil {
				t.Fatal(err)
			}
			want := digestOf(fixture.files["catalogs/base/registry.yaml"])
			if update.Ref != tc.ref || update.AfterURL != extendsBase || update.AfterSHA != want || !update.Changed || !update.WroteResult {
				t.Fatalf("update = %+v", update)
			}
			after, err := os.ReadFile(filepath.Join(dir, RegistryFile))
			if err != nil {
				t.Fatal(err)
			}
			var restored []byte
			if tc.withExtends {
				restored = []byte(strings.NewReplacer(extendsBase, update.BeforeURL, want, update.BeforeSHA).Replace(string(after)))
			} else {
				restored = bytes.TrimSuffix(after, []byte("extends:\n  url: "+extendsBase+"\n  sha256: "+want+"\n"))
			}
			if !bytes.Equal(restored, before) {
				t.Fatalf("rewrite changed other registry bytes:\n%s", after)
			}
			if _, err := LoadContext(t.Context(), DirWithRemote(dir, fixture.fetch)); err != nil {
				t.Fatalf("updated registry does not load: %v", err)
			}
			again, err := updateBase(t.Context(), BaseUpdateRequest{Directory: dir, Ref: tc.ref}, fakeRefs)
			if err != nil || again.Changed || again.WroteResult {
				t.Fatalf("second update = %+v: %v", again, err)
			}
		})
	}
}

func TestUpdateBaseFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name        string
		withExtends bool
		req         BaseUpdateRequest
		code        e.Code
	}{
		{"neither", true, BaseUpdateRequest{}, e.CodeUsage},
		{"both", true, BaseUpdateRequest{URL: extendsBase, Ref: "refs/tags/v1"}, e.CodeUsage},
		{"ref without base", false, BaseUpdateRequest{Ref: "refs/tags/v1"}, e.CodeUsage},
		{"short commit url", false, BaseUpdateRequest{URL: "https://raw.githubusercontent.com/acme/catalogs/36c4128/catalogs/base/registry.yaml"}, e.CodeUsage},
		{"bare branch url", false, BaseUpdateRequest{URL: "https://raw.githubusercontent.com/acme/catalogs/main/catalogs/base/registry.yaml"}, e.CodeUsage},
		{"short commit ref", true, BaseUpdateRequest{Ref: "36c4128"}, e.CodeUsage},
		{"bare ref", true, BaseUpdateRequest{Ref: "main"}, e.CodeUsage},
		{"unknown ref", true, BaseUpdateRequest{Ref: "refs/heads/missing"}, e.CodeFetchFailed},
		{"unadvertised url ref", false, BaseUpdateRequest{URL: "https://raw.githubusercontent.com/acme/catalogs/refs/heads/missing/catalogs/base/registry.yaml"}, e.CodeInvalidCatalog},
		{"unknown repository", false, BaseUpdateRequest{URL: "https://raw.githubusercontent.com/acme/other/refs/heads/main/catalogs/base/registry.yaml"}, e.CodeFetchFailed},
		{"other host", false, BaseUpdateRequest{URL: "https://example.com/acme/catalogs/refs/heads/main/catalogs/base/registry.yaml"}, e.CodeUsage},
		{"base missing at commit", true, BaseUpdateRequest{Ref: "refs/heads/main"}, e.CodeFetchFailed},
		{"not a registry", false, BaseUpdateRequest{URL: "https://raw.githubusercontent.com/acme/catalogs/refs/heads/main/catalogs/base/other.yaml"}, e.CodeInvalidCatalog},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, dir := baseUpdateFixture(t, tc.withExtends)
			before, err := os.ReadFile(filepath.Join(dir, RegistryFile))
			if err != nil {
				t.Fatal(err)
			}
			tc.req.Directory = dir
			_, err = updateBase(t.Context(), tc.req, fakeRefs)
			if got := catalogErrorCode(err); got != tc.code {
				t.Fatalf("code = %s, want %s: %v", got, tc.code, err)
			}
			after, err := os.ReadFile(filepath.Join(dir, RegistryFile))
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("failed update wrote the registry: %v", err)
			}
		})
	}
}

func TestUpdateBaseCheckAndWriterGuard(t *testing.T) {
	_, dir := baseUpdateFixture(t, true)
	before, err := os.ReadFile(filepath.Join(dir, RegistryFile))
	if err != nil {
		t.Fatal(err)
	}
	update, err := updateBase(t.Context(), BaseUpdateRequest{Directory: dir, Ref: "refs/tags/v1", Check: true}, fakeRefs)
	if catalogErrorCode(err) != e.CodeDiff || !update.Changed || update.WroteResult {
		t.Fatalf("check = %+v: %v", update, err)
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		t.Fatal(err)
	}
	release, err := acquireCatalogWriter(abs)
	if err != nil {
		t.Fatal(err)
	}
	_, err = updateBase(t.Context(), BaseUpdateRequest{Directory: dir, Ref: "refs/tags/v1"}, fakeRefs)
	release()
	if catalogErrorCode(err) != e.CodeWriteConflict {
		t.Fatalf("concurrent writer: %v", err)
	}
	after, err := os.ReadFile(filepath.Join(dir, RegistryFile))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("check or conflicting update wrote the registry: %v", err)
	}
}

func TestUpdateBaseRejectsInvalidCandidate(t *testing.T) {
	fixture, dir := baseUpdateFixture(t, true)
	fixture.files["catalogs/base/registry.yaml"] = append(fixture.files["catalogs/base/registry.yaml"], []byte("recipes: {}\n")...)
	before, err := os.ReadFile(filepath.Join(dir, RegistryFile))
	if err != nil {
		t.Fatal(err)
	}
	_, err = updateBase(t.Context(), BaseUpdateRequest{Directory: dir, Ref: "refs/tags/v1"}, fakeRefs)
	if catalogErrorCode(err) != e.CodeInvalidCatalog {
		t.Fatalf("invalid base admitted: %v", err)
	}
	after, err := os.ReadFile(filepath.Join(dir, RegistryFile))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("invalid candidate was written: %v", err)
	}
}
