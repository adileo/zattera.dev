package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSplitRef(t *testing.T) {
	tests := []struct {
		in              string
		host, repo, ref string
	}{
		{"nginx", "registry-1.docker.io", "library/nginx", "latest"},
		{"nginx:alpine", "registry-1.docker.io", "library/nginx", "alpine"},
		{"acme/web:1.2", "registry-1.docker.io", "acme/web", "1.2"},
		{"docker.io/library/nginx:1", "registry-1.docker.io", "library/nginx", "1"},
		{"ghcr.io/org/app:v3", "ghcr.io", "org/app", "v3"},
		{"reg.local:5000/proj/app:tag", "reg.local:5000", "proj/app", "tag"},
		{"localhost/app", "localhost", "app", "latest"},
		{"reg.local:5000/proj/app@sha256:abc", "reg.local:5000", "proj/app", "sha256:abc"},
		{"reg.local:5000/proj/app:tag@sha256:abc", "reg.local:5000", "proj/app", "sha256:abc"},
	}
	for _, tt := range tests {
		host, repo, ref := splitRef(tt.in)
		if host != tt.host || repo != tt.repo || ref != tt.ref {
			t.Errorf("splitRef(%q) = (%q, %q, %q), want (%q, %q, %q)",
				tt.in, host, repo, ref, tt.host, tt.repo, tt.ref)
		}
	}
}

func TestSplitRepoRef(t *testing.T) {
	tests := []struct{ in, repo, ref string }{
		{"proj/app", "proj/app", "latest"},
		{"proj/app:v1", "proj/app", "v1"},
		{"proj/app@sha256:abc", "proj/app", "sha256:abc"},
		{"proj/app:v1@sha256:abc", "proj/app", "sha256:abc"},
	}
	for _, tt := range tests {
		repo, ref := SplitRepoRef(tt.in)
		if repo != tt.repo || ref != tt.ref {
			t.Errorf("SplitRepoRef(%q) = (%q, %q), want (%q, %q)", tt.in, repo, ref, tt.repo, tt.ref)
		}
	}
}

// fakeRemote serves a minimal distribution API: an optional one-shot bearer
// challenge, one manifest per ref, and config blobs by digest.
type fakeRemote struct {
	requireToken bool
	manifests    map[string]struct {
		body      []byte
		mediaType string
	}
	blobs map[string][]byte
}

func (f *fakeRemote) handler(t *testing.T, authURL string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if f.requireToken && r.Header.Get("Authorization") == "" {
			w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Bearer realm=%q,service="test"`, authURL))
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch {
		case strings.Contains(r.URL.Path, "/manifests/"):
			ref := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
			m, ok := f.manifests[ref]
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", m.mediaType)
			_, _ = w.Write(m.body)
		case strings.Contains(r.URL.Path, "/blobs/"):
			d := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
			b, ok := f.blobs[d]
			if !ok {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write(b)
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			http.NotFound(w, r)
		}
	})
}

func TestRemotePlatformsIndex(t *testing.T) {
	f := &fakeRemote{manifests: map[string]struct {
		body      []byte
		mediaType string
	}{
		"multi": {indexManifest(
			childRef{"sha256:" + strings.Repeat("a", 64), "linux", "amd64"},
			childRef{"sha256:" + strings.Repeat("b", 64), "linux", "arm64"},
			childRef{"sha256:" + strings.Repeat("c", 64), "unknown", "unknown"}, // attestation
		), MediaTypeOCIIndex},
	}}
	srv := httptest.NewServer(f.handler(t, ""))
	defer srv.Close()
	host := strings.TrimPrefix(srv.URL, "http://")

	got, err := remotePlatforms(context.Background(), srv.Client(), "http", host+"/proj/app:multi")
	if err != nil {
		t.Fatalf("remotePlatforms: %v", err)
	}
	if len(got) != 2 || !contains(got, "linux/amd64") || !contains(got, "linux/arm64") {
		t.Fatalf("platforms = %v", got)
	}
}

func TestRemotePlatformsPlainManifestWithToken(t *testing.T) {
	cfgDigest := "sha256:" + strings.Repeat("d", 64)
	cfgBody, _ := json.Marshal(map[string]string{"os": "linux", "architecture": "arm64"})

	auth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("scope") != "repository:proj/app:pull" {
			t.Errorf("unexpected scope %q", r.URL.Query().Get("scope"))
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"token": "tok123"})
	}))
	defer auth.Close()

	f := &fakeRemote{
		requireToken: true,
		manifests: map[string]struct {
			body      []byte
			mediaType string
		}{
			"v1": {imageManifest(cfgDigest, "sha256:"+strings.Repeat("e", 64)), MediaTypeOCIManifest},
		},
		blobs: map[string][]byte{cfgDigest: cfgBody},
	}
	srv := httptest.NewServer(f.handler(t, auth.URL))
	defer srv.Close()
	host := strings.TrimPrefix(srv.URL, "http://")

	got, err := remotePlatforms(context.Background(), srv.Client(), "http", host+"/proj/app:v1")
	if err != nil {
		t.Fatalf("remotePlatforms: %v", err)
	}
	if len(got) != 1 || got[0] != "linux/arm64" {
		t.Fatalf("platforms = %v, want [linux/arm64]", got)
	}
}

func TestRemotePlatformsErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusForbidden)
	}))
	defer srv.Close()
	host := strings.TrimPrefix(srv.URL, "http://")

	if _, err := remotePlatforms(context.Background(), srv.Client(), "http", host+"/proj/app:v1"); err == nil {
		t.Fatal("forbidden manifest fetch must return an error (caller treats it as unknown)")
	}
}

// TestManifestsPlatformsPlain covers the embedded registry's platform read for
// a single-arch (non-index) image: the platform comes from the config blob.
func TestManifestsPlatformsPlain(t *testing.T) {
	rg := newTestRegistry(t)
	cfgBody, _ := json.Marshal(map[string]string{"os": "linux", "architecture": "amd64"})
	cfg, _, err := rg.Blobs.Write(strings.NewReader(string(cfgBody)))
	if err != nil {
		t.Fatal(err)
	}
	layer := pushBlob(t, rg, "layer-data")
	if _, err := rg.Manifests.PutManifest("proj/app", "v1", MediaTypeOCIManifest, imageManifest(cfg, layer)); err != nil {
		t.Fatal(err)
	}

	plats, err := rg.Manifests.Platforms("proj/app", "v1")
	if err != nil {
		t.Fatalf("platforms: %v", err)
	}
	if len(plats) != 1 || plats[0] != "linux/amd64" {
		t.Fatalf("platforms = %v, want [linux/amd64]", plats)
	}
}
