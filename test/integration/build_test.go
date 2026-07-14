//go:build integration

package integration

import (
	"context"
	"fmt"
	"os/exec"
	"testing"
	"time"

	"github.com/zattera-dev/zattera/internal/daemon/builder"
	crt "github.com/zattera-dev/zattera/internal/daemon/runtime"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// TestDockerfileBuild builds the go-hello fixture through a managed buildkitd,
// pushes it to the embedded registry, and runs the result to confirm it serves
// HTTP. This is the T-33 acceptance (single-arch, native platform).
func TestDockerfileBuild(t *testing.T) {
	RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	rt, err := crt.NewDocker()
	if err != nil {
		t.Skipf("docker runtime: %v", err)
	}
	addr, reg := startRegistry(t)

	b := builder.New(rt, clock.Real{}, t.TempDir(), "", nil)
	req := builder.RunBuildRequest{
		BuildID:          "build1",
		Project:          "demo",
		App:              "go-hello",
		Registry:         addr,
		SourceDir:        FixtureDir(t, "go-hello"),
		RegistryInsecure: true,
	}

	events := make(chan builder.BuildEvent, 256)
	go drainBuildLogs(t, events)
	res, err := b.Build(ctx, req, events)
	close(events)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if res.ImageDigest == "" {
		t.Fatal("build produced no image digest")
	}
	t.Cleanup(func() { _ = reg.Close() })

	// The image is pullable and runs, serving the fixture's HTTP contract.
	ref := addr + "/demo/go-hello:build1"
	if out, err := exec.CommandContext(ctx, "docker", "pull", ref).CombinedOutput(); err != nil {
		t.Fatalf("docker pull: %v\n%s", err, out)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "rmi", "-f", ref).Run() })

	port := freePort(t)
	name := fmt.Sprintf("zt-build-it-%d", time.Now().UnixNano())
	run := exec.CommandContext(ctx, "docker", "run", "-d", "--rm", "--name", name,
		"-e", "FIXTURE_MESSAGE=built", "-p", fmt.Sprintf("127.0.0.1:%d:8080", port), ref)
	if out, err := run.CombinedOutput(); err != nil {
		t.Fatalf("docker run: %v\n%s", err, out)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", name).Run() })

	waitHTTP(t, fmt.Sprintf("http://127.0.0.1:%d/healthz", port), 60*time.Second)
}

// TestDockerfileBuildEmulated builds a two-platform image index (amd64+arm64)
// via QEMU emulation and asserts the registry stored both children. Skips when
// emulation is unavailable (the build fails without QEMU binfmt).
func TestDockerfileBuildEmulated(t *testing.T) {
	RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	rt, err := crt.NewDocker()
	if err != nil {
		t.Skipf("docker runtime: %v", err)
	}
	addr, reg := startRegistry(t)
	t.Cleanup(func() { _ = reg.Close() })

	b := builder.New(rt, clock.Real{}, t.TempDir(), "", nil)
	req := builder.RunBuildRequest{
		BuildID:          "multi1",
		Project:          "demo",
		App:              "go-hello",
		Registry:         addr,
		SourceDir:        FixtureDir(t, "go-hello"),
		Platforms:        []string{"linux/amd64", "linux/arm64"},
		RegistryInsecure: true,
	}
	events := make(chan builder.BuildEvent, 256)
	go drainBuildLogs(t, events)
	res, err := b.Build(ctx, req, events)
	close(events)
	if err != nil {
		t.Skipf("multi-arch build failed (needs QEMU emulation): %v", err)
	}
	if res.ImageDigest == "" {
		t.Fatal("no index digest")
	}

	plats, err := reg.Manifests.Platforms("demo/go-hello", "multi1")
	if err != nil {
		t.Fatalf("platforms: %v", err)
	}
	if !containsStr(plats, "linux/amd64") || !containsStr(plats, "linux/arm64") {
		t.Fatalf("expected both arches in the index, got %v", plats)
	}
}

// TestNixpacksBuild builds the node-hello fixture (no Dockerfile) via the
// nixpacks planner + BuildKit, pushes it, and runs the result. This is the
// T-34 acceptance.
func TestNixpacksBuild(t *testing.T) {
	RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	rt, err := crt.NewDocker()
	if err != nil {
		t.Skipf("docker runtime: %v", err)
	}
	src := FixtureDir(t, "node-hello")
	if builder.ResolveBuildType(src) != builder.BuildNixpacks {
		t.Fatal("node-hello should resolve to a nixpacks build")
	}
	addr, reg := startRegistry(t)
	t.Cleanup(func() { _ = reg.Close() })

	b := builder.New(rt, clock.Real{}, t.TempDir(), "", nil)
	req := builder.RunBuildRequest{
		BuildID:          "nix1",
		Project:          "demo",
		App:              "node-hello",
		Registry:         addr,
		SourceDir:        src,
		RegistryInsecure: true,
	}
	events := make(chan builder.BuildEvent, 256)
	go drainBuildLogs(t, events)
	res, err := b.BuildNixpacks(ctx, req, events)
	close(events)
	if err != nil {
		t.Fatalf("nixpacks build: %v", err)
	}
	if res.ImageDigest == "" {
		t.Fatal("no image digest")
	}

	ref := addr + "/demo/node-hello:nix1"
	if out, err := exec.CommandContext(ctx, "docker", "pull", ref).CombinedOutput(); err != nil {
		t.Fatalf("docker pull: %v\n%s", err, out)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "rmi", "-f", ref).Run() })

	port := freePort(t)
	name := fmt.Sprintf("zt-nix-it-%d", time.Now().UnixNano())
	run := exec.CommandContext(ctx, "docker", "run", "-d", "--rm", "--name", name,
		"-e", "PORT=8080", "-p", fmt.Sprintf("127.0.0.1:%d:8080", port), ref)
	if out, err := run.CombinedOutput(); err != nil {
		t.Fatalf("docker run: %v\n%s", err, out)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", name).Run() })

	waitHTTP(t, fmt.Sprintf("http://127.0.0.1:%d/healthz", port), 60*time.Second)
}

func drainBuildLogs(t *testing.T, events <-chan builder.BuildEvent) {
	for ev := range events {
		if ev.Done {
			t.Logf("build done: success=%v digest=%s err=%s", ev.Success, ev.ImageDigest, ev.Err)
			continue
		}
		if ev.Log != "" {
			t.Logf("build: %s", ev.Log)
		}
	}
}
