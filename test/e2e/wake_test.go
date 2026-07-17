//go:build e2e

package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// scaleToZeroTOML mirrors the go-hello fixture config but marks production
// scale-to-zero with a short idle window, so the env cools quickly after the
// last request.
const scaleToZeroTOML = `[app]
name = "hello"

[build]
type = "dockerfile"

[deploy]
healthcheck = { path = "/healthz", timeout = "5s" }

[env.production]
min_replicas = 1
max_replicas = 2
scale_to_zero = true
idle_timeout = "15s"
`

// TestWake exercises the T-70 activator end to end: deploy a scale-to-zero app,
// let it cool to zero replicas after its idle window, then a fresh request wakes
// it (park → Activate → flush) and is served.
func TestWake(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.login()
	h.mustCLI("projects", "create", "wake")

	fixture := fixtureDir(t, "go-hello")

	// Deploy from source (cold build → release → running).
	if out, err := h.cli(fixture, "deploy", "--prod", "--project", "wake"); err != nil {
		t.Fatalf("e2e: deploy failed: %v\n%s", err, out)
	}

	host := "hello-production." + h.domain
	httpURL := "http://" + host + portOf(h.banner["ingress_http"]) + "/"
	h.pollBody(httpURL, host, "Hello from Zattera fixture", 180*time.Second)

	// Enable scale-to-zero on the running env (no rebuild — just the spec).
	tomlPath := filepath.Join(t.TempDir(), "zattera.toml")
	if err := os.WriteFile(tomlPath, []byte(scaleToZeroTOML), 0o644); err != nil {
		t.Fatalf("e2e: write toml: %v", err)
	}
	h.mustCLI("apply", "-f", tomlPath, "--project", "wake")

	// Wait for the env to cool to zero (no requests during this window). The
	// idle loop ticks every 15s; give it room past the 15s idle window.
	waitCooledToZero(t, h, 120*time.Second)

	// A fresh request must wake the env and be served. The first (canceled-early)
	// request still triggers activation; a retry lands once the replica is back.
	h.pollBody(httpURL, host, "Hello from Zattera fixture", 120*time.Second)
}

// waitCooledToZero polls `ps` until the app has no running/healthy instances,
// i.e. the scale-to-zero loop has stopped them.
func waitCooledToZero(t *testing.T, h *harness, deadline time.Duration) {
	t.Helper()
	end := time.Now().Add(deadline)
	var last string
	for time.Now().Before(end) {
		out, err := h.cli("", "ps", "--app", "hello", "--project", "wake")
		if err == nil {
			last = out
			low := strings.ToLower(out)
			if !strings.Contains(low, "healthy") && !strings.Contains(low, "running") &&
				!strings.Contains(low, "starting") && !strings.Contains(low, "pending") {
				return // no live instances remain
			}
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("e2e: env did not cool to zero within %s; last ps:\n%s", deadline, last)
}
