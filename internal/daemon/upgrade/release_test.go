package upgrade

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

const (
	amdSum = "1111111111111111111111111111111111111111111111111111111111111111"
	armSum = "2222222222222222222222222222222222222222222222222222222222222222"
)

// releaseServer serves a checksum manifest and a /latest redirect.
func releaseServer(t *testing.T, latestTag string, hits *atomic.Int32) *httptest.Server {
	t.Helper()
	manifest := fmt.Sprintf("%s  zattera-linux-amd64\n%s  zattera-linux-arm64\n"+
		"3333333333333333333333333333333333333333333333333333333333333333  zattera-darwin-arm64\n", amdSum, armSum)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/latest"):
			http.Redirect(w, r, "/releases/tag/"+latestTag, http.StatusFound)
		case strings.HasSuffix(r.URL.Path, "/"+checksumFile):
			if hits != nil {
				hits.Add(1)
			}
			_, _ = w.Write([]byte(manifest))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestResolvePinnedVersion parses the manifest into per-arch assets.
func TestResolvePinnedVersion(t *testing.T) {
	srv := releaseServer(t, "v9.9.9", nil)
	r := NewHTTPResolver(srv.URL + "/releases")

	rel, err := r.Resolve(context.Background(), "v0.4.0")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if rel.Version != "v0.4.0" {
		t.Errorf("version = %q", rel.Version)
	}
	amd, ok := rel.Asset("linux/amd64")
	if !ok {
		t.Fatalf("no linux/amd64 asset in %+v", rel.Assets)
	}
	if amd.SHA256 != amdSum {
		t.Errorf("amd64 checksum = %q", amd.SHA256)
	}
	if !strings.HasSuffix(amd.URL, "/download/v0.4.0/zattera-linux-amd64") {
		t.Errorf("amd64 url = %q", amd.URL)
	}
	if arm, ok := rel.Asset("linux/arm64"); !ok || arm.SHA256 != armSum {
		t.Errorf("arm64 asset wrong: %+v", arm)
	}
	if _, ok := rel.Asset("linux/riscv64"); ok {
		t.Error("resolver invented an asset for an unpublished arch")
	}
}

// TestResolveLatest follows the redirect to learn the concrete tag — knowing it
// is what lets the plan say "already up to date" honestly.
func TestResolveLatest(t *testing.T) {
	srv := releaseServer(t, "v1.2.3", nil)
	r := NewHTTPResolver(srv.URL + "/releases")

	rel, err := r.Resolve(context.Background(), "")
	if err != nil {
		t.Fatalf("resolve latest: %v", err)
	}
	if rel.Version != "v1.2.3" {
		t.Fatalf("latest resolved to %q, want v1.2.3", rel.Version)
	}
	if a, _ := rel.Asset("linux/amd64"); !strings.Contains(a.URL, "/download/v1.2.3/") {
		t.Errorf("asset url does not use the resolved tag: %q", a.URL)
	}
}

// TestResolveCaches: one upgrade run must pin one release. Re-resolving per
// node would let a release retagged mid-rollout split the cluster.
func TestResolveCaches(t *testing.T) {
	var hits atomic.Int32
	srv := releaseServer(t, "v1.0.0", &hits)
	r := NewHTTPResolver(srv.URL + "/releases")

	for i := 0; i < 3; i++ {
		if _, err := r.Resolve(context.Background(), "v0.4.0"); err != nil {
			t.Fatal(err)
		}
	}
	if hits.Load() != 1 {
		t.Errorf("manifest fetched %d times, want 1 (cached)", hits.Load())
	}
}

// TestResolveMissingRelease surfaces a bad version rather than returning an
// empty release the planner would treat as "nothing to do".
func TestResolveMissingRelease(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)
	r := NewHTTPResolver(srv.URL + "/releases")
	if _, err := r.Resolve(context.Background(), "v0.0.0"); err == nil {
		t.Fatal("expected an error for a missing release")
	}
}

// TestParseChecksums tolerates the formats sha256sum produces and ignores junk.
func TestParseChecksums(t *testing.T) {
	got := parseChecksums(strings.Join([]string{
		amdSum + "  zattera-linux-amd64",
		armSum + " *zattera-linux-arm64", // binary-mode marker
		"# a comment",
		"short  zattera-bad",
		"",
	}, "\n"))
	if got["zattera-linux-amd64"] != amdSum {
		t.Errorf("amd64 = %q", got["zattera-linux-amd64"])
	}
	if got["zattera-linux-arm64"] != armSum {
		t.Errorf("binary-mode marker not stripped: %q", got["zattera-linux-arm64"])
	}
	if len(got) != 2 {
		t.Errorf("parsed %d entries, want 2: %v", len(got), got)
	}
}

// TestOsArchFromAsset maps release asset names to Node.os_arch values.
func TestOsArchFromAsset(t *testing.T) {
	cases := map[string]string{
		"zattera-linux-amd64":       "linux/amd64",
		"zattera-linux-arm64":       "linux/arm64",
		"zattera-darwin-arm64":      "darwin/arm64",
		"zattera-windows-amd64.exe": "windows/amd64",
	}
	for name, want := range cases {
		got, ok := osArchFromAsset(name)
		if !ok || got != want {
			t.Errorf("osArchFromAsset(%q) = %q,%v want %q", name, got, ok, want)
		}
	}
	for _, name := range []string{"sha256sums.txt", "README", "zattera", "other-linux-amd64"} {
		if _, ok := osArchFromAsset(name); ok {
			t.Errorf("osArchFromAsset(%q) unexpectedly matched", name)
		}
	}
}
