package github

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// --- fakes ---

type fakeApps struct{ app *App }

func (f fakeApps) AppByRepo(repo string) (*App, bool) {
	if f.app != nil && f.app.Repo == repo {
		return f.app, true
	}
	return nil, false
}

type fakeDeployer struct {
	mu    sync.Mutex
	calls []deployCall
}
type deployCall struct{ env, branch, sha, cloneURL, token string }

func (f *fakeDeployer) DeployGit(_ context.Context, _ *App, env, branch, sha, cloneURL, token string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, deployCall{env, branch, sha, cloneURL, token})
	return "dep-1", nil
}

type fakeDedup struct {
	mu   sync.Mutex
	seen map[string]bool
}

func (f *fakeDedup) Seen(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.seen == nil {
		f.seen = map[string]bool{}
	}
	if f.seen[id] {
		return true
	}
	f.seen[id] = true
	return false
}

type fakeTokens struct{ token string }

func (f fakeTokens) InstallationToken(context.Context, int64) (string, error) { return f.token, nil }

func testApp() *App {
	return &App{
		ProjectID: "proj", AppID: "app", Repo: "acme/api", InstallationID: 42,
		WebhookSecret:      []byte("s3cr3t"),
		BranchEnvironments: map[string]string{"main": "production", "develop": "staging"},
	}
}

func pushBody(branch, sha string) []byte {
	b, _ := json.Marshal(map[string]any{
		"ref":   "refs/heads/" + branch,
		"after": sha,
		"repository": map[string]string{
			"full_name": "acme/api",
			"clone_url": "https://github.com/acme/api.git",
		},
	})
	return b
}

func post(t *testing.T, h http.Handler, event, delivery, sig string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/github/webhook", strings.NewReader(string(body)))
	req.Header.Set("X-GitHub-Event", event)
	if delivery != "" {
		req.Header.Set("X-GitHub-Delivery", delivery)
	}
	if sig != "" {
		req.Header.Set("X-Hub-Signature-256", sig)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestWebhookPushDeploys(t *testing.T) {
	dep := &fakeDeployer{}
	h := NewWebhook(fakeApps{testApp()}, dep, &fakeDedup{}, fakeTokens{token: "ghs_tok"}, nil)

	body := pushBody("main", "abc123")
	sig := SignPayload([]byte("s3cr3t"), body)
	rec := post(t, h, "push", "d1", sig, body)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	h.Wait()

	if len(dep.calls) != 1 {
		t.Fatalf("expected 1 deploy, got %d", len(dep.calls))
	}
	c := dep.calls[0]
	if c.env != "production" || c.branch != "main" || c.sha != "abc123" || c.token != "ghs_tok" {
		t.Fatalf("wrong deploy args: %+v", c)
	}
	if c.cloneURL != "https://github.com/acme/api.git" {
		t.Errorf("clone url = %q", c.cloneURL)
	}
}

func TestWebhookBadSignature(t *testing.T) {
	dep := &fakeDeployer{}
	h := NewWebhook(fakeApps{testApp()}, dep, &fakeDedup{}, fakeTokens{}, nil)
	body := pushBody("main", "abc123")
	rec := post(t, h, "push", "d1", "sha256=deadbeef", body)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	h.Wait()
	if len(dep.calls) != 0 {
		t.Fatal("bad signature must not deploy")
	}
}

func TestWebhookUnmappedBranch(t *testing.T) {
	dep := &fakeDeployer{}
	h := NewWebhook(fakeApps{testApp()}, dep, &fakeDedup{}, fakeTokens{}, nil)
	body := pushBody("feature/x", "abc123")
	sig := SignPayload([]byte("s3cr3t"), body)
	rec := post(t, h, "push", "d1", sig, body)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	h.Wait()
	if len(dep.calls) != 0 {
		t.Fatal("unmapped branch must be ignored")
	}
}

func TestWebhookDedupe(t *testing.T) {
	dep := &fakeDeployer{}
	h := NewWebhook(fakeApps{testApp()}, dep, &fakeDedup{}, fakeTokens{token: "t"}, nil)
	body := pushBody("develop", "s1")
	sig := SignPayload([]byte("s3cr3t"), body)

	post(t, h, "push", "same-delivery", sig, body)
	post(t, h, "push", "same-delivery", sig, body) // replay
	h.Wait()

	if len(dep.calls) != 1 {
		t.Fatalf("redelivered webhook deployed %d times, want 1", len(dep.calls))
	}
}

func TestWebhookPing(t *testing.T) {
	h := NewWebhook(fakeApps{testApp()}, &fakeDeployer{}, &fakeDedup{}, fakeTokens{}, nil)
	body := []byte(`{"repository":{"full_name":"acme/api"}}`)
	sig := SignPayload([]byte("s3cr3t"), body)
	rec := post(t, h, "ping", "", sig, body)
	if rec.Code != http.StatusOK {
		t.Fatalf("ping status = %d, want 200", rec.Code)
	}
}

func TestWebhookUnknownRepo(t *testing.T) {
	h := NewWebhook(fakeApps{testApp()}, &fakeDeployer{}, &fakeDedup{}, fakeTokens{}, nil)
	body := []byte(`{"repository":{"full_name":"someone/else"}}`)
	rec := post(t, h, "push", "d1", "sha256=whatever", body)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("unknown repo status = %d, want 202", rec.Code)
	}
}

// TestInstallationToken exercises the App JWT + token exchange against a fake
// GitHub API, and verifies caching.
func TestInstallationToken(t *testing.T) {
	keyPEM := genKeyPEM(t)
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
			t.Errorf("missing app JWT bearer")
		}
		if r.URL.Path != "/app/installations/42/access_tokens" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		calls++
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"token":      "ghs_installation",
			"expires_at": "2999-01-01T00:00:00Z",
		})
	}))
	defer srv.Close()

	app, err := NewGitHubApp(123, keyPEM, WithBaseURL(srv.URL), WithClock(clock.NewFake()))
	if err != nil {
		t.Fatal(err)
	}
	tok, err := app.InstallationToken(context.Background(), 42)
	if err != nil || tok != "ghs_installation" {
		t.Fatalf("token = %q err = %v", tok, err)
	}
	// Second call is served from cache (no extra HTTP round-trip).
	if _, err := app.InstallationToken(context.Background(), 42); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("token endpoint hit %d times, want 1 (cached)", calls)
	}
}

func genKeyPEM(t *testing.T) []byte {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
}
