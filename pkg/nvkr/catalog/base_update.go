// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	e "github.com/dsx-ai-factory/nvkr/pkg/nvkr/errors"
	"sigs.k8s.io/yaml"
)

// BaseUpdateRequest selects a mutable GitHub ref for a registry's remote base.
// URL is a raw.githubusercontent.com address whose ref segment is
// refs/heads/<branch>, refs/tags/<tag> or a full commit; Ref alone re-resolves
// the registry's current repository and path. Exactly one is required.
type BaseUpdateRequest struct {
	Directory string
	URL       string
	Ref       string
	Check     bool
}

// BaseUpdate reports the commit-addressed base before and after resolution.
type BaseUpdate struct {
	Ref                  string
	BeforeURL, AfterURL  string
	BeforeSHA, AfterSHA  string
	Changed, WroteResult bool
}

// lsRemoteFunc lists the given refs of one repository in `git ls-remote` form.
type lsRemoteFunc func(ctx context.Context, repository string, refs ...string) (string, error)

// gitLsRemote runs through the hardened, bounded pinned-tree Git runner. It
// needs no repository, so the working directory is only a neutral -C target.
func gitLsRemote(ctx context.Context, repository string, refs ...string) (string, error) {
	out, err := pinTreeGit(ctx, os.TempDir(), int(MaxCatalogFileBytes), append([]string{"ls-remote", repository}, refs...)...)
	return string(out), err
}

// UpdateBaseContext resolves a branch or tag to one commit, authenticates the
// base at that commit and rewrites only the registry's extends url and sha256
// under the catalog writer guard and registry byte CAS (ADR-22). The lock is
// untouched; `catalog pin` reconciles it with the merged declarations.
func UpdateBaseContext(ctx context.Context, req BaseUpdateRequest) (BaseUpdate, error) {
	return updateBase(ctx, req, gitLsRemote)
}

func updateBase(ctx context.Context, req BaseUpdateRequest, lsRemote lsRemoteFunc) (BaseUpdate, error) {
	if err := requireCatalogContext(ctx, "catalog base update"); err != nil {
		return BaseUpdate{}, err
	}
	if (req.URL == "") == (req.Ref == "") {
		return BaseUpdate{}, e.New(e.CodeUsage, "catalog base update needs exactly one of --url or --ref")
	}
	abs, err := filepath.Abs(req.Directory)
	if err != nil {
		return BaseUpdate{}, e.Wrap(e.CodeInvalidCatalog, err, "resolving catalog directory %q", req.Directory)
	}
	bound, err := BindContext(ctx, Dir(abs))
	if err != nil {
		return BaseUpdate{}, err
	}
	before, err := bound.ReadFile(RegistryFile)
	if err != nil {
		return BaseUpdate{}, e.Wrap(e.CodeInvalidCatalog, err, "reading %s", RegistryFile)
	}
	var authored struct {
		Extends *registryExtends `json:"extends"`
	}
	if err := yaml.Unmarshal(before, &authored); err != nil {
		return BaseUpdate{}, e.Wrap(e.CodeInvalidCatalog, err, "parsing %s", RegistryFile)
	}
	var owner, repo, file, ref string
	switch {
	case req.URL != "":
		owner, repo, ref, file, err = resolveMutableBaseURL(ctx, req.URL, lsRemote)
	case authored.Extends == nil:
		err = e.New(e.CodeUsage, "%s has no extends base; pass --url to add one", RegistryFile)
	default:
		var current baseLocation
		current, err = parseBaseURL(authored.Extends.URL)
		owner, repo, file, ref = current.owner, current.repo, current.file, req.Ref
		if err == nil {
			err = validateBaseRef(ref)
		}
	}
	if err != nil {
		return BaseUpdate{}, err
	}
	commit := ref
	if !fullCommitPattern.MatchString(ref) {
		if commit, err = resolveBaseRef(ctx, owner, repo, ref, lsRemote); err != nil {
			return BaseUpdate{}, err
		}
	}
	location := baseLocation{owner: owner, repo: repo, commit: commit, file: file}
	update := BaseUpdate{Ref: ref, AfterURL: location.url()}
	if authored.Extends != nil {
		update.BeforeURL, update.BeforeSHA = authored.Extends.URL, authored.Extends.SHA256
	}
	raw, err := bound.(*boundContextProvider).readRemote(update.AfterURL)
	if err != nil {
		return BaseUpdate{}, err
	}
	sum := sha256.Sum256(raw)
	update.AfterSHA = "sha256:" + hex.EncodeToString(sum[:])
	update.Changed = update.BeforeURL != update.AfterURL || update.BeforeSHA != update.AfterSHA
	if !update.Changed {
		return update, nil
	}
	after, err := rewriteBase(before, authored.Extends, update)
	if err != nil {
		return BaseUpdate{}, err
	}
	// Admit the rewritten registry completely before writing: the new base,
	// its relative inputs and every patch operation must still apply.
	staged, err := BindContext(ctx, stagedRegistry{dirProvider: dirProvider{root: abs}, registry: after})
	if err != nil {
		return BaseUpdate{}, err
	}
	if _, err := decodeRegistry(after, RegistryFile, staged); err != nil {
		return BaseUpdate{}, err
	}
	if req.Check {
		return update, e.New(e.CodeDiff, "remote base would change from %s to %s", orUnset(update.BeforeURL), update.AfterURL)
	}
	release, err := acquireCatalogWriter(abs)
	if err != nil {
		return BaseUpdate{}, err
	}
	defer release()
	current, exists, err := readLockBytesContext(ctx, filepath.Join(abs, RegistryFile))
	if err != nil {
		return BaseUpdate{}, err
	}
	if !exists || !bytes.Equal(current, before) {
		return BaseUpdate{}, e.New(e.CodeWriteConflict, "%s changed during base update", RegistryFile)
	}
	if err := replaceCatalogFile(ctx, abs, RegistryFile, after); err != nil {
		return BaseUpdate{}, err
	}
	update.WroteResult = true
	return update, nil
}

func orUnset(value string) string {
	if value == "" {
		return "(none)"
	}
	return value
}

func (location baseLocation) url() string {
	return "https://" + remoteBaseHost + "/" + location.owner + "/" + location.repo + "/" + location.commit + "/" + location.file
}

func validateBaseRef(ref string) error {
	if fullCommitPattern.MatchString(ref) {
		return nil
	}
	name, ok := strings.CutPrefix(ref, "refs/heads/")
	if !ok {
		name, ok = strings.CutPrefix(ref, "refs/tags/")
	}
	if !ok || name == "" || strings.HasSuffix(name, "/") || strings.ContainsAny(name, " ~^:?*[\\\x00") ||
		strings.Contains(name, "..") || strings.Contains(name, "//") || strings.HasPrefix(name, "-") {
		return e.New(e.CodeUsage, "base ref %q must be refs/heads/<branch>, refs/tags/<tag> or a full 40-character commit", ref)
	}
	return nil
}

// resolveMutableBaseURL splits a raw URL whose ref may contain slashes. The
// repository's advertised refs decide the split; zero or several candidates fail.
func resolveMutableBaseURL(ctx context.Context, raw string, lsRemote lsRemoteFunc) (owner, repo, ref, file string, err error) {
	parsed, parseErr := url.Parse(raw)
	if parseErr != nil || parsed.Scheme != "https" || parsed.Host != remoteBaseHost || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.Fragment != "" || parsed.RawPath != "" || strings.ContainsAny(raw, "%\\\x00") {
		return "", "", "", "", e.New(e.CodeUsage, "base URL %q must be https://%s/<owner>/<repo>/<ref>/<path>", raw, remoteBaseHost)
	}
	parts := strings.SplitN(strings.TrimPrefix(parsed.Path, "/"), "/", 3)
	if len(parts) != 3 {
		return "", "", "", "", e.New(e.CodeUsage, "base URL %q must name an owner, repository, ref and path", raw)
	}
	owner, repo, rest := parts[0], parts[1], parts[2]
	if commit, path, found := strings.Cut(rest, "/"); found && fullCommitPattern.MatchString(commit) {
		location := baseLocation{owner: owner, repo: repo, commit: commit, file: path}
		if _, err := parseBaseURL(location.url()); err != nil {
			return "", "", "", "", err
		}
		return owner, repo, commit, path, nil
	}
	if !strings.HasPrefix(rest, "refs/heads/") && !strings.HasPrefix(rest, "refs/tags/") {
		return "", "", "", "", e.New(e.CodeUsage,
			"base URL %q must use refs/heads/<branch>, refs/tags/<tag> or a full commit; short commits are not accepted", raw)
	}
	if !githubNamePattern.MatchString(owner) || !githubNamePattern.MatchString(repo) {
		return "", "", "", "", e.New(e.CodeUsage, "base URL %q must name one GitHub owner and repository", raw)
	}
	out, err := lsRemote(ctx, githubRepository(owner, repo), "refs/heads/*", "refs/tags/*")
	if err != nil {
		return "", "", "", "", baseResolveError(ctx, err, owner, repo)
	}
	var candidates []string
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || strings.HasSuffix(fields[1], "^{}") {
			continue
		}
		if strings.HasPrefix(rest, fields[1]+"/") {
			candidates = append(candidates, fields[1])
		}
	}
	if len(candidates) != 1 {
		return "", "", "", "", e.New(e.CodeInvalidCatalog,
			"base URL %q matches %d advertised refs %v; exactly one is required", raw, len(candidates), candidates)
	}
	ref = candidates[0]
	file = strings.TrimPrefix(rest, ref+"/")
	if err := validateBaseRef(ref); err != nil {
		return "", "", "", "", err
	}
	if err := validateCatalogPath(file); err != nil || path.Base(file) != RegistryFile {
		return "", "", "", "", e.New(e.CodeInvalidCatalog, "base URL %q must name a canonical %s path", raw, RegistryFile)
	}
	return owner, repo, ref, file, nil
}

// resolveBaseRef maps one exact branch or tag to its commit, preferring the
// peeled target so annotated tags resolve to the commit they name.
func resolveBaseRef(ctx context.Context, owner, repo, ref string, lsRemote lsRemoteFunc) (string, error) {
	out, err := lsRemote(ctx, githubRepository(owner, repo), ref, ref+"^{}")
	if err != nil {
		return "", baseResolveError(ctx, err, owner, repo)
	}
	var direct, peeled string
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || !fullCommitPattern.MatchString(fields[0]) {
			continue
		}
		switch fields[1] {
		case ref:
			direct = fields[0]
		case ref + "^{}":
			peeled = fields[0]
		}
	}
	if peeled != "" {
		return peeled, nil
	}
	if direct == "" {
		return "", e.New(e.CodeFetchFailed, "ref %s not found in %s", ref, githubRepository(owner, repo))
	}
	return direct, nil
}

func githubRepository(owner, repo string) string {
	return "https://github.com/" + owner + "/" + repo + ".git"
}

func baseResolveError(ctx context.Context, err error, owner, repo string) error {
	if ctx.Err() != nil {
		return pinContextError(ctx)
	}
	return e.Wrap(e.CodeFetchFailed, err, "ls-remote %s", githubRepository(owner, repo))
}

// rewriteBase replaces exactly the authored url and sha256 scalars, or appends
// a new extends mapping, leaving every other registry byte unchanged.
func rewriteBase(raw []byte, current *registryExtends, update BaseUpdate) ([]byte, error) {
	if current == nil {
		if bytes.Contains(raw, []byte("\nextends:")) || bytes.HasPrefix(raw, []byte("extends:")) {
			return nil, e.New(e.CodeInvalidCatalog, "%s has an empty extends mapping", RegistryFile)
		}
		if len(raw) != 0 && raw[len(raw)-1] != '\n' {
			raw = append(raw, '\n')
		}
		return append(raw, []byte("extends:\n  url: "+update.AfterURL+"\n  sha256: "+update.AfterSHA+"\n")...), nil
	}
	text := string(raw)
	for _, value := range []string{current.URL, current.SHA256} {
		if value == "" || strings.Count(text, value) != 1 {
			return nil, e.New(e.CodeInvalidCatalog, "%s must contain the extends value %q exactly once to rewrite it", RegistryFile, value)
		}
	}
	text = strings.Replace(text, current.URL, update.AfterURL, 1)
	text = strings.Replace(text, current.SHA256, update.AfterSHA, 1)
	return []byte(text), nil
}

// stagedRegistry serves rewritten registry bytes over the catalog directory so
// the complete candidate is admitted before the writer guard is taken.
type stagedRegistry struct {
	dirProvider
	registry []byte
}

func (provider stagedRegistry) ReadFileContext(ctx context.Context, name string) ([]byte, error) {
	if name == RegistryFile {
		return append([]byte(nil), provider.registry...), nil
	}
	return provider.dirProvider.ReadFileContext(ctx, name)
}
