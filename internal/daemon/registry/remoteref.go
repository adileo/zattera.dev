package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// RemotePlatforms best-effort inspects an EXTERNAL image reference (docker hub,
// ghcr, …) and returns the OCI platforms it can run on: an index/manifest
// list's child platforms, or the single platform read from a plain manifest's
// config. It speaks anonymous distribution-API auth (bearer token challenge).
// Callers treat ANY error as "unknown" — a deploy must never fail over
// manifest inspection (T-88).
func RemotePlatforms(ctx context.Context, imageRef string) ([]string, error) {
	return remotePlatforms(ctx, &http.Client{Timeout: 10 * time.Second}, "https", imageRef)
}

// remotePlatforms is RemotePlatforms with an injectable client and scheme
// (tests run against a plain-HTTP httptest server).
func remotePlatforms(ctx context.Context, client *http.Client, scheme, imageRef string) ([]string, error) {
	host, repo, ref := splitRef(imageRef)
	r := &remoteRepo{client: client, scheme: scheme, host: host, repo: repo}

	body, mediaType, err := r.getManifest(ctx, ref)
	if err != nil {
		return nil, err
	}

	var pm parsedManifest
	if err := json.Unmarshal(body, &pm); err != nil {
		return nil, fmt.Errorf("registry: parse remote manifest: %w", err)
	}
	if mediaType == "" {
		mediaType = pm.MediaType
	}

	// Index / manifest list: collect the child platforms.
	if isIndexMediaType(mediaType) || len(pm.Manifests) > 0 {
		var out []string
		for _, c := range pm.Manifests {
			p := c.Platform
			if p == nil || p.OS == "" || p.Architecture == "" || p.OS == "unknown" {
				continue // skip attestation manifests ("unknown/unknown")
			}
			s := p.OS + "/" + p.Architecture
			if p.Variant != "" {
				s += "/" + p.Variant
			}
			out = append(out, s)
		}
		return out, nil
	}

	// Plain manifest: the config blob carries os/architecture.
	if pm.Config.Digest == "" {
		return nil, fmt.Errorf("registry: remote manifest has no config")
	}
	cfgBody, err := r.getBlob(ctx, pm.Config.Digest)
	if err != nil {
		return nil, err
	}
	var cfg struct {
		Architecture string `json:"architecture"`
		OS           string `json:"os"`
		Variant      string `json:"variant"`
	}
	if err := json.Unmarshal(cfgBody, &cfg); err != nil || cfg.OS == "" || cfg.Architecture == "" {
		return nil, fmt.Errorf("registry: remote config has no platform")
	}
	p := cfg.OS + "/" + cfg.Architecture
	if cfg.Variant != "" {
		p += "/" + cfg.Variant
	}
	return []string{p}, nil
}

// remoteRepo is a minimal anonymous distribution-API client for one repository.
type remoteRepo struct {
	client *http.Client
	scheme string
	host   string
	repo   string
	token  string // bearer token from a 401 challenge, once acquired
}

const acceptManifests = MediaTypeOCIIndex + ", " + MediaTypeDockerList + ", " +
	MediaTypeOCIManifest + ", " + MediaTypeDockerManifest

func (r *remoteRepo) getManifest(ctx context.Context, ref string) (body []byte, mediaType string, err error) {
	resp, err := r.get(ctx, fmt.Sprintf("%s://%s/v2/%s/manifests/%s", r.scheme, r.host, r.repo, ref), acceptManifests)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, "", err
	}
	return b, resp.Header.Get("Content-Type"), nil
}

func (r *remoteRepo) getBlob(ctx context.Context, digest string) ([]byte, error) {
	resp, err := r.get(ctx, fmt.Sprintf("%s://%s/v2/%s/blobs/%s", r.scheme, r.host, r.repo, digest), "")
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	return io.ReadAll(io.LimitReader(resp.Body, 4<<20))
}

// get performs a GET, answering one anonymous bearer-token challenge (the
// standard docker hub / ghcr flow for public pulls).
func (r *remoteRepo) get(ctx context.Context, rawURL, accept string) (*http.Response, error) {
	do := func() (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			return nil, err
		}
		if accept != "" {
			req.Header.Set("Accept", accept)
		}
		if r.token != "" {
			req.Header.Set("Authorization", "Bearer "+r.token)
		}
		return r.client.Do(req)
	}

	resp, err := do()
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusUnauthorized && r.token == "" {
		challenge := resp.Header.Get("WWW-Authenticate")
		_ = resp.Body.Close()
		if err := r.fetchToken(ctx, challenge); err != nil {
			return nil, err
		}
		resp, err = do()
		if err != nil {
			return nil, err
		}
	}
	if resp.StatusCode != http.StatusOK {
		defer func() { _ = resp.Body.Close() }()
		return nil, fmt.Errorf("registry: remote %s: %s", rawURL, resp.Status)
	}
	return resp, nil
}

// fetchToken acquires an anonymous pull token from a Bearer challenge like
// `Bearer realm="https://auth.docker.io/token",service="registry.docker.io"`.
func (r *remoteRepo) fetchToken(ctx context.Context, challenge string) error {
	if !strings.HasPrefix(strings.ToLower(challenge), "bearer ") {
		return fmt.Errorf("registry: unsupported auth challenge %q", challenge)
	}
	params := map[string]string{}
	for _, kv := range strings.Split(challenge[len("bearer "):], ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(kv), "=")
		if ok {
			params[strings.ToLower(k)] = strings.Trim(v, `"`)
		}
	}
	realm := params["realm"]
	if realm == "" {
		return fmt.Errorf("registry: auth challenge without realm")
	}
	q := url.Values{}
	if s := params["service"]; s != "" {
		q.Set("service", s)
	}
	q.Set("scope", "repository:"+r.repo+":pull")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, realm+"?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("registry: token endpoint: %s", resp.Status)
	}
	var tok struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&tok); err != nil {
		return err
	}
	r.token = tok.Token
	if r.token == "" {
		r.token = tok.AccessToken
	}
	if r.token == "" {
		return fmt.Errorf("registry: token endpoint returned no token")
	}
	return nil
}

// splitRef splits an image reference into registry host, repository and
// tag-or-digest, applying docker-style defaults: no registry host → docker hub
// (registry-1.docker.io) with the "library/" prefix for bare names; no
// tag/digest → "latest".
func splitRef(imageRef string) (host, repo, ref string) {
	rest := imageRef
	host = "registry-1.docker.io"
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		first := rest[:i]
		// Only a dotted name, a port, or "localhost" before the first slash is
		// a registry host; anything else is a docker hub namespace.
		if strings.ContainsAny(first, ".:") || first == "localhost" {
			host, rest = first, rest[i+1:]
		}
	}
	if host == "docker.io" || host == "index.docker.io" {
		host = "registry-1.docker.io"
	}

	repo, ref = SplitRepoRef(rest)
	if host == "registry-1.docker.io" && !strings.Contains(repo, "/") {
		repo = "library/" + repo
	}
	return host, repo, ref
}

// SplitRepoRef splits a host-less image path ("proj/app:tag",
// "proj/app@sha256:…") into repository and tag-or-digest ("latest" when bare).
func SplitRepoRef(path string) (repo, ref string) {
	ref = "latest"
	if i := strings.IndexByte(path, '@'); i >= 0 {
		path, ref = path[:i], path[i+1:]
		// A digest-pinned ref may also carry a tag ("name:tag@sha256:…").
		if j := strings.IndexByte(path, ':'); j >= 0 {
			path = path[:j]
		}
	} else if i := strings.LastIndexByte(path, ':'); i >= 0 && !strings.Contains(path[i:], "/") {
		path, ref = path[:i], path[i+1:]
	}
	return path, ref
}
