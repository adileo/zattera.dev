//go:build cloud

package cloud

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestWebApp deploys a basic Go web app (the go-hello fixture) as a 3-replica
// service across the cluster and verifies the full path on real infra: source
// build → embedded registry → red/green rollout → 3 healthy replicas spread
// over the nodes → the app serving HTTP through the ingress.
//
//	HCLOUD_TOKEN=... go test -tags cloud ./test/cloud/ -run TestWebApp -v
func TestWebApp(t *testing.T) {
	c := NewCluster(t)

	// 3-node cluster (control is also a worker + the builder).
	c.StartControl("amd64", "cloud-webapp.zattera.invalid")
	c.JoinWorker("amd64")
	c.JoinWorker("amd64")
	c.WaitNodesReady(3)

	// Every node trusts the embedded registry so workers can pull the built image.
	c.TrustRegistryCA()

	// Deploy the fixture (pinned to 3 replicas) as a source build, via the API.
	appDir := prepareHelloFixture(t, 3)
	depID, envID := c.DeploySource("webapp", appDir)

	// Build → rollout completes (fails fast with a reason if the build breaks).
	c.WaitDeployment("webapp", depID, 8*time.Minute)

	// 3 healthy replicas spread across ≥2 nodes.
	nodes := c.WaitHealthyReplicas("webapp", envID, 3, 3*time.Minute)
	if len(nodes) < 2 {
		t.Errorf("cloud: 3 replicas should spread across ≥2 nodes, landed on %d: %v", len(nodes), nodes)
	}

	// The app actually serves its body through the ingress.
	host := "hello-production.cloud-webapp.zattera.invalid"
	c.PollAppBody(c.control, host, "Hello from Zattera fixture", 2*time.Minute)
}

// prepareHelloFixture copies the go-hello fixture to a temp dir and pins the
// production env to `replicas` replicas (the fixture ships min1/max2).
func prepareHelloFixture(t *testing.T, replicas int) string {
	t.Helper()
	src := filepath.Join(repoRootDir(), "test", "fixtures", "apps", "go-hello")
	dst := t.TempDir()
	for _, name := range []string{"main.go", "go.mod", "Dockerfile"} {
		b, err := os.ReadFile(filepath.Join(src, name))
		if err != nil {
			t.Fatalf("cloud: read fixture %s: %v", name, err)
		}
		if err := os.WriteFile(filepath.Join(dst, name), b, 0o644); err != nil {
			t.Fatalf("cloud: write %s: %v", name, err)
		}
	}
	toml := fmt.Sprintf(`[app]
name = "hello"

[build]
type = "dockerfile"

[deploy]
healthcheck = { path = "/healthz", timeout = "5s" }

[env.production]
min_replicas = %d
max_replicas = %d
`, replicas, replicas)
	if err := os.WriteFile(filepath.Join(dst, "zattera.toml"), []byte(toml), 0o644); err != nil {
		t.Fatalf("cloud: write zattera.toml: %v", err)
	}
	return dst
}
