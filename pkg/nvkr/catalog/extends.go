// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strings"

	e "github.com/dsx-ai-factory/nvkr/pkg/nvkr/errors"
	jsonpatch "github.com/evanphx/json-patch/v5"
	"sigs.k8s.io/yaml"
)

const (
	remoteBaseHost = "raw.githubusercontent.com"
	// MaxBasePatchOperations bounds the RFC 6902 operations one registry applies to its base.
	MaxBasePatchOperations = 256
)

var (
	githubNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$`)
	fullCommitPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)
)

// RemoteDataProvider admits commit-addressed remote base inputs (ADR-22). The
// loader authenticates every result; implementations only fetch bounded bytes.
type RemoteDataProvider interface {
	ReadRemoteContext(context.Context, string) ([]byte, error)
}

// registryExtends is the authored remote base reference. The registry is the
// only owner of the base identity; the lock never records inheritance.
type registryExtends struct {
	URL     string            `json:"url"`
	SHA256  string            `json:"sha256"`
	Patches []json.RawMessage `json:"patches,omitempty"`
}

// baseLocation is a parsed commit-addressed raw.githubusercontent.com file.
type baseLocation struct {
	owner, repo, commit string
	file                string // repository-relative path
}

// parseRemoteInputURL admits exactly one canonical commit-addressed GitHub raw
// file. Branches, tags, short commits and any ambiguous URL spelling fail.
func parseRemoteInputURL(raw string) (baseLocation, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Host != remoteBaseHost || parsed.User != nil ||
		parsed.Opaque != "" || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" ||
		parsed.RawPath != "" || strings.ContainsAny(raw, "%\\\x00") || parsed.String() != raw {
		return baseLocation{}, e.New(e.CodeInvalidCatalog,
			"remote base %q must be a canonical https://%s/<owner>/<repo>/<commit>/<path> URL", raw, remoteBaseHost)
	}
	parts := strings.SplitN(strings.TrimPrefix(parsed.Path, "/"), "/", 4)
	if len(parts) != 4 || !githubNamePattern.MatchString(parts[0]) || !githubNamePattern.MatchString(parts[1]) ||
		strings.Trim(parts[1], ".") == "" {
		return baseLocation{}, e.New(e.CodeInvalidCatalog, "remote base %q must name one GitHub owner and repository", raw)
	}
	if !fullCommitPattern.MatchString(parts[2]) {
		return baseLocation{}, e.New(e.CodeInvalidCatalog,
			"remote base %q must name a full 40-character lowercase commit, not a branch, tag or short commit", raw)
	}
	if err := validateCatalogPath(parts[3]); err != nil {
		return baseLocation{}, e.Wrap(e.CodeInvalidCatalog, err, "remote base %q path", raw)
	}
	return baseLocation{owner: parts[0], repo: parts[1], commit: parts[2], file: parts[3]}, nil
}

// ValidateRemoteInputURL checks a remote catalog input location before any I/O.
func ValidateRemoteInputURL(location string) error {
	_, err := parseRemoteInputURL(location)
	return err
}

func parseBaseURL(raw string) (baseLocation, error) {
	location, err := parseRemoteInputURL(raw)
	if err != nil {
		return baseLocation{}, err
	}
	if path.Base(location.file) != RegistryFile {
		return baseLocation{}, e.New(e.CodeInvalidCatalog, "remote base %q must name a %s file", raw, RegistryFile)
	}
	return location, nil
}

// sibling resolves a path relative to the base registry's directory. Leading
// parent segments may reach sibling inputs but never leave the repository.
func (location baseLocation) sibling(name string) (string, error) {
	joined := path.Join(path.Dir(location.file), name)
	if joined == "." || joined == ".." || strings.HasPrefix(joined, "../") {
		return "", e.New(e.CodeInvalidCatalog, "remote base input %q leaves the base repository", name)
	}
	if err := validateCatalogPath(joined); err != nil {
		return "", err
	}
	return "https://" + remoteBaseHost + "/" + location.owner + "/" + location.repo + "/" + location.commit + "/" + joined, nil
}

// readRemote fetches one remote input through the bound provider's capability
// and charges it to the same aggregate byte bound as local catalog inputs.
func (provider *boundContextProvider) readRemote(location string) ([]byte, error) {
	if err := requireCatalogContext(provider.ctx, "remote catalog read"); err != nil {
		return nil, err
	}
	if _, err := parseRemoteInputURL(location); err != nil {
		return nil, err
	}
	reader, ok := provider.provider.(RemoteDataProvider)
	if !ok {
		return nil, e.New(e.CodeEngineCapability, "catalog provider %q does not support remote base registries", provider.Name())
	}
	raw, err := reader.ReadRemoteContext(provider.ctx, location)
	if err != nil {
		// Keep a transport's own code (unavailable, missing snapshot input);
		// only uncoded transport failures become fetch failures.
		code := e.CodeFetchFailed
		var coded *e.Error
		if errors.As(err, &coded) {
			code = coded.Code
		}
		return nil, normalizeCatalogContextError(provider.ctx,
			e.Wrap(code, err, "reading remote catalog input %s", location), "remote catalog read")
	}
	if int64(len(raw)) > MaxCatalogFileBytes {
		return nil, e.New(e.CodeInvalidCatalog, "remote catalog input %s exceeds the %d-byte bound", location, MaxCatalogFileBytes)
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if provider.totalBytes+int64(len(raw)) > MaxCatalogTotalBytes {
		return nil, e.New(e.CodeInvalidCatalog, "catalog load exceeds the %d-byte aggregate bound", MaxCatalogTotalBytes)
	}
	provider.totalBytes += int64(len(raw))
	return append([]byte(nil), raw...), nil
}

// remoteBaseProvider serves a base registry's relative inputs from the same
// commit. It never lists directories: legacy layout checks apply to local roots.
type remoteBaseProvider struct {
	parent   *boundContextProvider
	location baseLocation
}

func (provider remoteBaseProvider) ReadFile(name string) ([]byte, error) {
	return nil, e.New(e.CodeEngineCapability, "remote base file %q requires a context-bound read", name)
}

func (provider remoteBaseProvider) ReadFileContext(_ context.Context, name string) ([]byte, error) {
	if err := validateCatalogPath(name); err != nil {
		return nil, err
	}
	location, err := provider.location.sibling(name)
	if err != nil {
		return nil, err
	}
	return provider.parent.readRemote(location)
}

func (provider remoteBaseProvider) ReadFragmentContext(_ context.Context, name string) ([]byte, error) {
	if err := ValidateFragmentPath(name); err != nil {
		return nil, err
	}
	location, err := provider.location.sibling(name)
	if err != nil {
		return nil, err
	}
	return provider.parent.readRemote(location)
}

func (provider remoteBaseProvider) ReadDir(name string) ([]string, error) {
	return nil, e.New(e.CodeEngineCapability, "remote base directory %q cannot be listed", name)
}

func (provider remoteBaseProvider) ReadDirContext(_ context.Context, name string) ([]string, error) {
	return provider.ReadDir(name)
}

func (provider remoteBaseProvider) Name() string {
	return "remote:" + provider.location.owner + "/" + provider.location.repo + "@" + provider.location.commit
}

// memoryPatchFiles serves authenticated base patch bytes to resolvePatches.
type memoryPatchFiles struct {
	name  string
	files map[string][]byte
}

func (files memoryPatchFiles) ReadFile(name string) ([]byte, error) {
	raw, ok := files.files[name]
	if !ok {
		return nil, e.New(e.CodeInvalidCatalog, "base patch %q was not fetched", name)
	}
	return append([]byte(nil), raw...), nil
}

func (files memoryPatchFiles) ReadDir(name string) ([]string, error) {
	return nil, e.New(e.CodeEngineCapability, "base patch directory %q cannot be listed", name)
}

func (files memoryPatchFiles) Name() string { return files.name }

// patchProvider returns the provider that authenticates a component's patch
// files: the child's own root, or the bytes fetched from its remote base.
func (reg *registryDoc) patchProvider(component string, local DataProvider) DataProvider {
	if reg.inherited[component] {
		return memoryPatchFiles{name: reg.Extends.URL, files: reg.basePatchFiles}
	}
	return local
}

// expandBase fetches, authenticates, patches and unions the remote base. The
// child's own declarations (including its fragments) are already expanded.
func expandBase(reg *registryDoc, providers []DataProvider) error {
	if reg.Extends == nil {
		return nil
	}
	if len(providers) != 1 {
		return e.New(e.CodeEngineCapability, "remote base registries require a context-bound provider")
	}
	parent, ok := providers[0].(*boundContextProvider)
	if !ok {
		return e.New(e.CodeEngineCapability, "remote base registries require a context-bound provider")
	}
	location, err := parseBaseURL(reg.Extends.URL)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(reg.Extends.SHA256, "sha256:") || len(reg.Extends.SHA256) != 71 ||
		strings.Trim(reg.Extends.SHA256[7:], "0123456789abcdef") != "" {
		return e.New(e.CodeUnpinnedSource, "remote base %s needs a lowercase sha256:<64-hex> digest", reg.Extends.URL)
	}
	if len(reg.Extends.Patches) > MaxBasePatchOperations {
		return e.New(e.CodeInvalidCatalog, "remote base exceeds the %d-operation patch bound", MaxBasePatchOperations)
	}
	raw, err := parent.readRemote(reg.Extends.URL)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(raw)
	if "sha256:"+hex.EncodeToString(sum[:]) != reg.Extends.SHA256 {
		return e.New(e.CodeInvalidCatalog, "remote base %s bytes do not match %s", reg.Extends.URL, reg.Extends.SHA256)
	}
	baseProvider := &boundContextProvider{ctx: parent.ctx, provider: remoteBaseProvider{parent: parent, location: location}}
	base, err := decodeBaseRegistry(raw, reg.Extends.URL, baseProvider)
	if err != nil {
		return err
	}
	declared := basePatchDeclarations(base)
	patched, err := applyBasePatches(base, reg.Extends.Patches)
	if err != nil {
		return err
	}
	return unionBase(reg, patched, declared, baseProvider)
}

func decodeBaseRegistry(raw []byte, where string, provider *boundContextProvider) (*registryDoc, error) {
	providers := []DataProvider{provider}
	raw, err := expandCommandFiles(raw, RegistryFile, providers)
	if err != nil {
		return nil, e.Wrap(e.CodeInvalidCatalog, err, "expanding remote base %s", where)
	}
	var base registryDoc
	if err := yaml.UnmarshalStrict(raw, &base); err != nil {
		return nil, e.Wrap(e.CodeInvalidCatalog, err, "parsing remote base %s", where)
	}
	if base.APIVersion != catalogAPIVersion || base.Kind != catalogKind {
		return nil, e.New(e.CodeInvalidCatalog, "remote base %s must be %s %s", where, catalogAPIVersion, catalogKind)
	}
	if base.Extends != nil {
		return nil, e.New(e.CodeInvalidCatalog, "remote base %s cannot itself extend another registry", where)
	}
	if err := expandFragments(&base, providers); err != nil {
		return nil, err
	}
	return &base, nil
}

// basePatchDeclarations records the base's own patch-file declarations. A JSON
// patch may keep or drop them but never introduce or alter one.
func basePatchDeclarations(base *registryDoc) map[patchDeclaration]bool {
	declared := map[patchDeclaration]bool{}
	for _, component := range base.Components {
		for _, declaration := range component.Patches {
			declared[declaration] = true
		}
	}
	return declared
}

type baseDeclarations struct {
	Sources    map[string]Source       `json:"sources,omitempty"`
	Components map[string]rawComponent `json:"components,omitempty"`
}

// applyBasePatches applies RFC 6902 operations to the expanded base. JSON is
// parser transport only; the strict typed decode below is the admission gate.
func applyBasePatches(base *registryDoc, operations []json.RawMessage) (baseDeclarations, error) {
	document, err := json.Marshal(baseDeclarations{Sources: base.Sources, Components: base.Components})
	if err != nil {
		return baseDeclarations{}, e.Wrap(e.CodeInternal, err, "encoding remote base declarations")
	}
	if len(operations) != 0 {
		for i, operation := range operations {
			if err := validateBasePatchOperation(i, operation); err != nil {
				return baseDeclarations{}, err
			}
		}
		encoded, err := json.Marshal(operations)
		if err != nil {
			return baseDeclarations{}, e.Wrap(e.CodeInternal, err, "encoding remote base patch")
		}
		patch, err := jsonpatch.DecodePatch(encoded)
		if err != nil {
			return baseDeclarations{}, e.Wrap(e.CodeInvalidCatalog, err, "decoding remote base patch")
		}
		options := jsonpatch.NewApplyOptions()
		options.SupportNegativeIndices = false
		options.AccumulatedCopySizeLimit = MaxCatalogFileBytes
		options.AllowMissingPathOnRemove = false
		options.EnsurePathExistsOnAdd = false
		document, err = patch.ApplyWithOptions(document, options)
		if err != nil {
			return baseDeclarations{}, e.Wrap(e.CodeInvalidCatalog, err, "applying remote base patch")
		}
	}
	var patched baseDeclarations
	if err := yaml.UnmarshalStrict(document, &patched); err != nil {
		return baseDeclarations{}, e.Wrap(e.CodeInvalidCatalog, err, "decoding patched remote base")
	}
	return patched, nil
}

func validateBasePatchOperation(index int, raw json.RawMessage) error {
	var operation map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := decoder.Decode(&operation); err != nil {
		return e.Wrap(e.CodeInvalidCatalog, err, "remote base patch[%d] is not an operation object", index)
	}
	for _, field := range []string{"path", "from"} {
		encoded, present := operation[field]
		if !present {
			continue
		}
		var pointer string
		if err := json.Unmarshal(encoded, &pointer); err != nil {
			return e.Wrap(e.CodeInvalidCatalog, err, "remote base patch[%d] %s", index, field)
		}
		if !strings.HasPrefix(pointer, "/sources/") && !strings.HasPrefix(pointer, "/components/") {
			return e.New(e.CodeInvalidCatalog,
				"remote base patch[%d] %s %q must lie under /sources/ or /components/", index, field, pointer)
		}
	}
	return nil
}

// unionBase adds the patched base to the child's declarations. Collisions fail:
// changing an inherited declaration is the patch operations' job alone.
func unionBase(reg *registryDoc, patched baseDeclarations, declared map[patchDeclaration]bool, base *boundContextProvider) error {
	if reg.Sources == nil {
		reg.Sources = map[string]Source{}
	}
	if reg.Components == nil {
		reg.Components = map[string]rawComponent{}
	}
	for _, name := range sortedKeys(patched.Sources) {
		if _, duplicate := reg.Sources[name]; duplicate {
			return e.New(e.CodeInvalidCatalog, "source %q collides with remote base %s; patch the base instead", name, reg.Extends.URL)
		}
		reg.Sources[name] = patched.Sources[name]
	}
	reg.inherited = map[string]bool{}
	reg.basePatchFiles = map[string][]byte{}
	for _, name := range sortedKeys(patched.Components) {
		if _, duplicate := reg.Components[name]; duplicate {
			return e.New(e.CodeInvalidCatalog, "component %q collides with remote base %s; patch the base instead", name, reg.Extends.URL)
		}
		component := patched.Components[name]
		for _, declaration := range component.Patches {
			if !declared[declaration] {
				return e.New(e.CodeInvalidCatalog, "component %q: remote base patches cannot add or alter patch file %q", name, declaration.Name)
			}
			if _, fetched := reg.basePatchFiles[declaration.Name]; fetched || !validPatchPath(declaration.Name) {
				continue
			}
			content, err := base.ReadFile(declaration.Name)
			if err != nil {
				return e.Wrap(e.CodeInvalidCatalog, err, "reading remote base patch %q", declaration.Name)
			}
			reg.basePatchFiles[declaration.Name] = content
		}
		reg.Components[name] = component
		reg.inherited[name] = true
	}
	return nil
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
