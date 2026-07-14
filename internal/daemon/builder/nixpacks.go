package builder

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/zattera-dev/zattera/internal/daemon/runtime"
)

// nixpacksImage is the pinned planner image. It only generates a Dockerfile +
// context (the heavy work happens in the BuildKit stage), so latest is fine to
// track; pin a digest in production wiring.
const nixpacksImage = "ghcr.io/railwayapp/nixpacks:latest"

// nixpacksOutDir is the subdirectory (within the source) the planner writes its
// generated Dockerfile and build context into.
const nixpacksOutDir = ".nixpacks-out"

// BuildKind is the resolved build strategy for a source tree.
type BuildKind int

const (
	// BuildDockerfile builds a Dockerfile already present in the source.
	BuildDockerfile BuildKind = iota
	// BuildNixpacks generates a Dockerfile with nixpacks first.
	BuildNixpacks
)

// ResolveBuildType decides how to build a source tree when the app's build type
// is unspecified: a Dockerfile in the context root means a Dockerfile build,
// otherwise nixpacks generates one. (T-35 maps the proto BuildType; this owns
// the BUILD_TYPE_UNSPECIFIED auto-detect.)
func ResolveBuildType(dir string) BuildKind {
	if fi, err := os.Stat(filepath.Join(dir, "Dockerfile")); err == nil && !fi.IsDir() {
		return BuildDockerfile
	}
	return BuildNixpacks
}

// BuildNixpacks generates a Dockerfile from req.SourceDir with the nixpacks
// planner, then builds it through the T-33 Dockerfile pipeline. The planner is
// architecture-independent, so a multi-arch request plans exactly once and lets
// BuildKit fan out per platform.
func (b *Builder) BuildNixpacks(ctx context.Context, req RunBuildRequest, events chan<- BuildEvent) (*BuildResult, error) {
	outDir, err := b.plan(ctx, req.SourceDir, events)
	if err != nil {
		return nil, b.fail(events, err)
	}
	// Build the generated context: nixpacks writes a Dockerfile plus the build
	// context it references, both under .nixpacks-out.
	nixReq := req
	nixReq.SourceDir = outDir
	nixReq.ContextDir = "."
	nixReq.Dockerfile = "Dockerfile"
	return b.Build(ctx, nixReq, events)
}

// BuildFromSource unpacks a source tarball into a scratch dir, resolves the
// build strategy (Dockerfile vs nixpacks), and builds it. This is the entry
// point the agent's build server calls after fetching the tarball from control.
func (b *Builder) BuildFromSource(ctx context.Context, req RunBuildRequest, src io.Reader, events chan<- BuildEvent) (*BuildResult, error) {
	dir, err := os.MkdirTemp(b.dataDir, "src-*")
	if err != nil {
		return nil, b.fail(events, fmt.Errorf("builder: scratch dir: %w", err))
	}
	defer func() { _ = os.RemoveAll(dir) }()
	if err := unpackSource(dir, src); err != nil {
		return nil, b.fail(events, err)
	}
	req.SourceDir = dir
	if ResolveBuildType(filepath.Join(dir, orDot(req.ContextDir))) == BuildNixpacks {
		return b.BuildNixpacks(ctx, req, events)
	}
	return b.Build(ctx, req, events)
}

// plan runs the nixpacks planner container over sourceDir, streaming its logs
// as "plan"-phase events, and returns the generated output directory.
func (b *Builder) plan(ctx context.Context, sourceDir string, events chan<- BuildEvent) (string, error) {
	outDir := filepath.Join(sourceDir, nixpacksOutDir)
	// Remove any leftovers from a previous attempt — a stale plan would give the
	// build the wrong context.
	if err := os.RemoveAll(outDir); err != nil {
		return "", fmt.Errorf("builder: clean nixpacks out: %w", err)
	}
	if err := b.rt.EnsureImage(ctx, nixpacksImage, nil, nil); err != nil {
		return "", fmt.Errorf("builder: pull nixpacks: %w", err)
	}

	id, err := b.rt.CreateContainer(ctx, runtime.ContainerSpec{
		Name:    fmt.Sprintf("zt-system-nixpacks-%d", b.clk.Now().UnixNano()),
		Image:   nixpacksImage,
		Command: []string{"build", "/src", "--out", "/src/" + nixpacksOutDir, "--name", "ignored"},
		Mounts:  []runtime.Mount{{HostPath: sourceDir, Target: "/src"}},
		Labels:  map[string]string{"zattera.system": "nixpacks"},
	})
	if err != nil {
		return "", fmt.Errorf("builder: create nixpacks: %w", err)
	}
	defer func() { _ = b.rt.RemoveContainer(context.Background(), id, true) }()
	if err := b.rt.StartContainer(ctx, id); err != nil {
		return "", fmt.Errorf("builder: start nixpacks: %w", err)
	}

	// Stream the planner's logs as plan-phase events.
	done := make(chan struct{})
	if logs, lerr := b.rt.Logs(ctx, id, runtime.LogsOptions{Follow: true}); lerr == nil {
		go func() {
			defer close(done)
			for e := range logs {
				if line := strings.TrimRight(e.Line, "\n"); line != "" {
					emit(events, BuildEvent{Phase: "plan", Log: line})
				}
			}
		}()
	} else {
		close(done)
	}

	if werr := b.waitExit(ctx, id); werr != nil {
		<-done
		return "", fmt.Errorf("builder: nixpacks plan: %w", werr)
	}
	<-done

	// The planner must have produced a Dockerfile we can build.
	if _, err := os.Stat(filepath.Join(outDir, "Dockerfile")); err != nil {
		return "", fmt.Errorf("builder: nixpacks produced no Dockerfile in %s", nixpacksOutDir)
	}
	return outDir, nil
}
