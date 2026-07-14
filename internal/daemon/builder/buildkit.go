package builder

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/docker/cli/cli/config/configfile"
	dockertypes "github.com/docker/cli/cli/config/types"
	bkclient "github.com/moby/buildkit/client"
	"github.com/moby/buildkit/exporter/containerimage/exptypes"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/session/auth/authprovider"
	"github.com/tonistiigi/fsutil"

	"github.com/zattera-dev/zattera/internal/daemon/runtime"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// Pinned system images. The buildkit version tracks the client library
// (github.com/moby/buildkit) so the wire protocol matches; binfmt provides the
// QEMU emulators for cross-architecture builds.
const (
	buildkitImage     = "moby/buildkit:v0.18.2"
	buildkitContainer = "zt-system-buildkitd"
	binfmtImage       = "tonistiigi/binfmt:qemu-v8.1.5"

	// dockerfileFrontend is BuildKit's built-in Dockerfile frontend.
	dockerfileFrontend = "dockerfile.v0"

	// buildkitd listens on TCP (not a unix socket) so the host daemon can reach
	// it via a published loopback port. A bind-mounted unix socket is not
	// connectable across the Docker Desktop VM boundary on macOS; a published
	// TCP port works identically on macOS and Linux.
	buildkitTCPPort  = 1234 // inside the container
	buildkitHostPort = 8372 // published on the host loopback

	buildkitBootTimeout = 90 * time.Second
	buildTimeout        = 30 * time.Minute
)

// RunBuildRequest describes one image build. T-35 maps the AgentLocalService
// RunBuild proto onto this struct; keeping it a plain type keeps the builder
// package free of proto/agent imports.
type RunBuildRequest struct {
	BuildID    string
	Project    string
	App        string
	Registry   string // "host:port" of the target registry
	Dockerfile string // path within the context; default "Dockerfile"
	ContextDir string // subdir within the source; default "."
	BuildArgs  map[string]string
	Platforms  []string     // target OCI platforms; empty → builder's native
	SourceDir  string       // unpacked source root
	Auth       RegistryAuth // creds for pushing to the registry
	// ImageRef, when set, is the exact push reference; otherwise it is derived
	// from Registry/Project/App/BuildID.
	ImageRef string
	// RegistryInsecure pushes over plain HTTP (integration tests only).
	RegistryInsecure bool
}

// RegistryAuth carries push credentials for the target registry.
type RegistryAuth struct {
	Registry string
	Username string
	Password string
}

// BuildEvent is one item of build progress. Log events carry a line (and a
// Phase: "plan" for the nixpacks planner, "build" for the BuildKit solve); the
// final event has Done set with the outcome and (on success) the pushed image
// ref, its digest (the INDEX digest for multi-arch), and the built platforms.
type BuildEvent struct {
	Phase       string
	Log         string
	Done        bool
	Success     bool
	ImageRef    string
	ImageDigest string
	Platforms   []string
	Err         string
}

// BuildResult is the terminal outcome of a successful build.
type BuildResult struct {
	ImageRef    string
	ImageDigest string
	Platforms   []string
}

// Builder ensures a managed buildkitd and runs Dockerfile builds against it.
type Builder struct {
	rt      runtime.ContainerRuntime
	clk     clock.Clock
	log     *slog.Logger
	dataDir string
	caPath  string
	native  string

	mu         sync.Mutex
	emuInstall map[string]bool // platforms whose emulator has been installed
}

// New constructs a Builder. dataDir is the node data dir (the buildkitd socket
// lives under it); caPath is the host path to the cluster CA bundle mounted
// into buildkitd so registry pushes verify TLS.
func New(rt runtime.ContainerRuntime, clk clock.Clock, dataDir, caPath string, log *slog.Logger) *Builder {
	if log == nil {
		log = slog.Default()
	}
	return &Builder{
		rt: rt, clk: clk, log: log,
		dataDir: dataDir, caPath: caPath, native: localPlatform(),
		emuInstall: map[string]bool{},
	}
}

// buildkitAddr is the TCP address the host daemon uses to reach buildkitd
// (published from the container onto the host loopback).
func (b *Builder) buildkitAddr() string {
	return fmt.Sprintf("tcp://127.0.0.1:%d", buildkitHostPort)
}

// EnsureBuildkitd makes sure the managed buildkitd container is running and
// its API answers, then returns a connected client. Safe to call before every
// build (idempotent).
func (b *Builder) EnsureBuildkitd(ctx context.Context, onLog func(string)) (*bkclient.Client, error) {
	if onLog == nil {
		onLog = func(string) {}
	}
	if err := os.MkdirAll(filepath.Join(b.dataDir, "buildkit"), 0o755); err != nil {
		return nil, fmt.Errorf("builder: buildkit dir: %w", err)
	}
	onLog("provisioning build environment (pulling buildkitd)…")
	if err := b.rt.EnsureImage(ctx, buildkitImage, nil, func(status string) { onLog(status) }); err != nil {
		return nil, fmt.Errorf("builder: pull buildkitd: %w", err)
	}

	running, err := b.rt.ListContainers(ctx, map[string]string{"zattera.system": "buildkitd"})
	if err != nil {
		return nil, fmt.Errorf("builder: list buildkitd: %w", err)
	}
	if len(running) == 0 {
		// Cache lives on a named Docker volume, never a host bind mount:
		// buildkitd creates unix sockets under /run/buildkit and its cache under
		// /var/lib/buildkit, and Docker Desktop's host filesystem cannot back
		// those (chmod on a socket fails). /run/buildkit stays on the container fs.
		mounts := []runtime.Mount{
			{VolumeName: "zt-buildkit-cache", Target: "/var/lib/buildkit"},
		}
		if b.caPath != "" {
			mounts = append(mounts, runtime.Mount{HostPath: b.caPath, Target: "/etc/ssl/certs/zattera-ca.pem", ReadOnly: true})
		}
		id, err := b.rt.CreateContainer(ctx, runtime.ContainerSpec{
			Name:       buildkitContainer,
			Image:      buildkitImage,
			Command:    []string{"--addr", fmt.Sprintf("tcp://0.0.0.0:%d", buildkitTCPPort)},
			Privileged: true,
			Mounts:     mounts,
			Ports: []runtime.PortBinding{{
				ContainerPort: buildkitTCPPort,
				Protocol:      "tcp",
				HostIP:        "127.0.0.1",
				HostPort:      buildkitHostPort,
			}},
			Restart: runtime.RestartUnlessStopped,
			Labels:  map[string]string{"zattera.system": "buildkitd"},
		})
		if err != nil {
			return nil, fmt.Errorf("builder: create buildkitd: %w", err)
		}
		if err := b.rt.StartContainer(ctx, id); err != nil {
			return nil, fmt.Errorf("builder: start buildkitd: %w", err)
		}
	}
	return b.waitBuildkitd(ctx, onLog)
}

// waitBuildkitd polls the buildkitd API until it answers or the boot timeout
// elapses, emitting a heartbeat each poll so a slow boot is not mistaken for a
// lost builder.
func (b *Builder) waitBuildkitd(ctx context.Context, onLog func(string)) (*bkclient.Client, error) {
	deadline := b.clk.Now().Add(buildkitBootTimeout)
	backoff := 200 * time.Millisecond
	for {
		c, err := bkclient.New(ctx, b.buildkitAddr())
		if err == nil {
			if _, ierr := c.Info(ctx); ierr == nil {
				return c, nil
			}
			_ = c.Close()
		}
		if b.clk.Now().After(deadline) {
			return nil, fmt.Errorf("builder: buildkitd did not become ready within %s", buildkitBootTimeout)
		}
		onLog("waiting for buildkitd to become ready…")
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-b.clk.After(backoff):
		}
		if backoff < 2*time.Second {
			backoff *= 2
		}
	}
}

// EnsureEmulators installs QEMU binfmt handlers for any target platform the
// builder cannot execute natively. It is a no-op when every target is native.
func (b *Builder) EnsureEmulators(ctx context.Context, platforms []string) error {
	need := platformsNeedingEmulation(b.native, platforms)
	if len(need) == 0 {
		return nil
	}
	b.mu.Lock()
	allInstalled := true
	for _, p := range need {
		if !b.emuInstall[p] {
			allInstalled = false
			break
		}
	}
	b.mu.Unlock()
	if allInstalled {
		return nil
	}

	if err := b.rt.EnsureImage(ctx, binfmtImage, nil, nil); err != nil {
		return fmt.Errorf("builder: pull binfmt: %w", err)
	}
	// The binfmt installer registers handlers with the fix-binary (F) flag so
	// they survive across the buildkitd container boundary, then exits.
	id, err := b.rt.CreateContainer(ctx, runtime.ContainerSpec{
		Name:       fmt.Sprintf("zt-system-binfmt-%d", b.clk.Now().UnixNano()),
		Image:      binfmtImage,
		Command:    []string{"--install", "all"},
		Privileged: true,
		Labels:     map[string]string{"zattera.system": "binfmt"},
	})
	if err != nil {
		return fmt.Errorf("builder: create binfmt: %w", err)
	}
	defer func() { _ = b.rt.RemoveContainer(context.Background(), id, true) }()
	if err := b.rt.StartContainer(ctx, id); err != nil {
		return fmt.Errorf("builder: start binfmt: %w", err)
	}
	if err := b.waitExit(ctx, id); err != nil {
		return fmt.Errorf("builder: binfmt install: %w", err)
	}

	b.mu.Lock()
	for _, p := range need {
		b.emuInstall[p] = true
	}
	b.mu.Unlock()
	return nil
}

// waitExit blocks until a container exits (Inspect reports not running).
func (b *Builder) waitExit(ctx context.Context, id string) error {
	deadline := b.clk.Now().Add(2 * time.Minute)
	for {
		st, err := b.rt.InspectContainer(ctx, id)
		if err != nil {
			return err
		}
		if !st.Running {
			if st.ExitCode != 0 {
				return fmt.Errorf("exited with code %d", st.ExitCode)
			}
			return nil
		}
		if b.clk.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for exit")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b.clk.After(500 * time.Millisecond):
		}
	}
}

// Build runs a Dockerfile build for req and pushes the result to the registry.
// Progress (log lines and the terminal outcome) is streamed on events; the
// terminal BuildResult is also returned. For more than one platform the
// exporter emits an OCI image index and ImageDigest is the index digest.
func (b *Builder) Build(ctx context.Context, req RunBuildRequest, events chan<- BuildEvent) (*BuildResult, error) {
	ctx, cancel := context.WithTimeout(ctx, buildTimeout)
	defer cancel()

	emit(events, BuildEvent{Phase: "build", Log: "starting build"})
	onLog := func(s string) { emit(events, BuildEvent{Phase: "build", Log: s}) }

	platforms := resolvePlatforms(req.Platforms, b.native)
	if err := b.EnsureEmulators(ctx, platforms); err != nil {
		return nil, b.fail(events, err)
	}
	c, err := b.EnsureBuildkitd(ctx, onLog)
	if err != nil {
		return nil, b.fail(events, err)
	}
	defer func() { _ = c.Close() }()

	contextDir := filepath.Join(req.SourceDir, orDot(req.ContextDir))
	srcFS, err := fsutil.NewFS(contextDir)
	if err != nil {
		return nil, b.fail(events, fmt.Errorf("builder: context fs: %w", err))
	}

	dockerfile := req.Dockerfile
	if dockerfile == "" {
		dockerfile = "Dockerfile"
	}
	imageRef := req.ImageRef
	if imageRef == "" {
		imageRef = fmt.Sprintf("%s/%s/%s:%s", req.Registry, req.Project, req.App, req.BuildID)
	}

	attrs := map[string]string{
		"filename": dockerfile,
		"platform": frontendPlatformAttr(platforms),
	}
	for k, v := range req.BuildArgs {
		attrs["build-arg:"+k] = v
	}

	exportAttrs := map[string]string{"name": imageRef, "push": "true"}
	if req.RegistryInsecure {
		exportAttrs["registry.insecure"] = "true"
	}

	solveOpt := bkclient.SolveOpt{
		Frontend:      dockerfileFrontend,
		FrontendAttrs: attrs,
		LocalMounts:   map[string]fsutil.FS{"context": srcFS, "dockerfile": srcFS},
		Exports: []bkclient.ExportEntry{{
			Type:  bkclient.ExporterImage,
			Attrs: exportAttrs,
		}},
		Session: []session.Attachable{dockerAuth(req.Auth)},
	}

	statusCh := make(chan *bkclient.SolveStatus)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for s := range statusCh {
			for _, line := range solveStatusLines(s) {
				emit(events, BuildEvent{Phase: "build", Log: line})
			}
		}
	}()

	resp, err := c.Solve(ctx, nil, solveOpt, statusCh)
	<-done
	if err != nil {
		return nil, b.fail(events, fmt.Errorf("builder: solve: %w", err))
	}

	digest := resp.ExporterResponse[exptypes.ExporterImageDigestKey]
	res := &BuildResult{ImageRef: imageRef, ImageDigest: digest, Platforms: platforms}
	emit(events, BuildEvent{Done: true, Success: true, ImageRef: imageRef, ImageDigest: digest, Platforms: platforms})
	return res, nil
}

// fail emits a terminal failure event and returns the error.
func (b *Builder) fail(events chan<- BuildEvent, err error) error {
	emit(events, BuildEvent{Done: true, Success: false, Err: err.Error()})
	return err
}

// emit sends an event without blocking a slow/absent consumer forever; a nil
// channel drops silently.
func emit(events chan<- BuildEvent, ev BuildEvent) {
	if events == nil {
		return
	}
	events <- ev
}

// solveStatusLines flattens a BuildKit SolveStatus into human-readable log
// lines: completed/cached build steps by name, then any log output.
func solveStatusLines(s *bkclient.SolveStatus) []string {
	var out []string
	for _, v := range s.Vertexes {
		if v.Name == "" {
			continue
		}
		switch {
		case v.Cached:
			out = append(out, "CACHED "+v.Name)
		case v.Completed != nil:
			out = append(out, v.Name)
		}
	}
	for _, l := range s.Logs {
		text := strings.TrimRight(string(l.Data), "\n")
		if text == "" {
			continue
		}
		out = append(out, strings.Split(text, "\n")...)
	}
	return out
}

// dockerAuth builds a BuildKit auth provider from a single registry credential.
func dockerAuth(a RegistryAuth) session.Attachable {
	cf := configfile.New("")
	if a.Registry != "" {
		cf.AuthConfigs = map[string]dockertypes.AuthConfig{
			a.Registry: {Username: a.Username, Password: a.Password, ServerAddress: a.Registry},
		}
	}
	return authprovider.NewDockerAuthProvider(cf, nil)
}

func orDot(s string) string {
	if s == "" {
		return "."
	}
	return s
}
