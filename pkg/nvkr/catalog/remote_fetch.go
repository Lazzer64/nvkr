// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package catalog

import (
	"context"
	"io"
	"net/http"
	"sync"
	"time"

	e "github.com/dsx-ai-factory/nvkr/pkg/nvkr/errors"
)

const (
	remoteFetchTimeout    = 2 * time.Minute
	maxRemoteCacheEntries = 256
)

// RemoteFetchFunc fetches one validated, commit-addressed remote catalog input.
type RemoteFetchFunc func(context.Context, string) ([]byte, error)

// DirWithRemote returns Dir with an explicit remote base transport. Callers
// supply alternate transports (for example test servers); authentication of
// the returned bytes remains with the catalog loader.
func DirWithRemote(dir string, fetch RemoteFetchFunc) DataProvider {
	return dirProvider{root: dir, remote: fetch}
}

func (provider dirProvider) ReadRemoteContext(ctx context.Context, location string) ([]byte, error) {
	if err := requireCatalogContext(ctx, "remote catalog read"); err != nil {
		return nil, err
	}
	if _, err := parseRemoteInputURL(location); err != nil {
		return nil, err
	}
	fetch := provider.remote
	if fetch == nil {
		fetch = defaultRemoteFetch
	}
	return fetch(ctx, location)
}

// remoteFetcher memoizes commit-addressed bytes for one process. Commit URLs
// name immutable Git content; every consumer still authenticates declared digests.
type remoteFetcher struct {
	client *http.Client

	mu    sync.Mutex
	cache map[string][]byte
}

// defaultRemoteFetch serves Dir providers. Lock maintenance binds Dir internally,
// so package tests substitute this transport rather than threading a provider.
var defaultRemoteFetch RemoteFetchFunc = (&remoteFetcher{client: newRemoteCatalogClient()}).fetch

func newRemoteCatalogClient() *http.Client {
	client := newPinHTTPClient()
	client.Timeout = remoteFetchTimeout
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return e.New(e.CodeFetchFailed, "remote catalog inputs do not follow redirects")
	}
	return client
}

func (fetcher *remoteFetcher) fetch(ctx context.Context, location string) ([]byte, error) {
	fetcher.mu.Lock()
	cached, ok := fetcher.cache[location]
	fetcher.mu.Unlock()
	if ok {
		return append([]byte(nil), cached...), nil
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, location, nil)
	if err != nil {
		return nil, e.Wrap(e.CodeFetchFailed, err, "constructing GET %s", location)
	}
	response, err := fetcher.client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return nil, e.Wrap(e.CodeTimeout, ctx.Err(), "GET %s canceled", location)
		}
		return nil, e.Wrap(e.CodeUnavailable, err, "GET %s", location)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, e.New(e.CodeFetchFailed, "GET %s returned HTTP %s", location, response.Status)
	}
	if response.ContentLength > MaxCatalogFileBytes {
		return nil, e.New(e.CodeInvalidCatalog, "GET %s exceeds the %d-byte bound", location, MaxCatalogFileBytes)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, MaxCatalogFileBytes+1))
	if err != nil {
		if ctx.Err() != nil {
			return nil, e.Wrap(e.CodeTimeout, ctx.Err(), "GET %s canceled", location)
		}
		return nil, e.Wrap(e.CodeFetchFailed, err, "reading GET %s", location)
	}
	if int64(len(body)) > MaxCatalogFileBytes {
		return nil, e.New(e.CodeInvalidCatalog, "GET %s exceeds the %d-byte bound", location, MaxCatalogFileBytes)
	}
	fetcher.mu.Lock()
	if fetcher.cache == nil {
		fetcher.cache = map[string][]byte{}
	}
	if len(fetcher.cache) < maxRemoteCacheEntries {
		fetcher.cache[location] = append([]byte(nil), body...)
	}
	fetcher.mu.Unlock()
	return body, nil
}
