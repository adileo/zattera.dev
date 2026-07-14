package builder

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/zattera-dev/zattera/internal/daemon/runtime"
)

// nixpacksVersion pins the nixpacks CLI. The railwayapp/nixpacks Docker images
// are build *base* images (the generated Dockerfile's FROM), not the CLI, so the
// planner runs the CLI binary — distributed as a static musl executable — inside
// a minimal image instead. Bumping the version means updating the checksums.
const nixpacksVersion = "v1.41.0"

// nixpacksRunnerImage is a minimal image used only to execute the mounted static
// nixpacks CLI (it needs a filesystem + the source, nothing else). The heavy
// build runs later in BuildKit against the base image the generated Dockerfile
// points its FROM at.
const nixpacksRunnerImage = "alpine:3.21"

// nixpacksOutSubdir is the directory (within the build context) the CLI writes
// its generated Dockerfile and nix files into. The build context stays the
// source tree; the generated Dockerfile COPYs the source and this metadata.
const nixpacksOutSubdir = ".nixpacks"

// nixpacksDownload is a pinned per-arch CLI archive and its SHA-256 (the release
// ships no checksums file, so they are recorded here; verified before exec).
type nixpacksDownload struct{ url, sha256 string }

var nixpacksDownloads = map[string]nixpacksDownload{
	"amd64": {
		url:    "https://github.com/railwayapp/nixpacks/releases/download/" + nixpacksVersion + "/nixpacks-" + nixpacksVersion + "-x86_64-unknown-linux-musl.tar.gz",
		sha256: "0f55de7874507b9cf7502113120bd96f2ab6979f78d10eaf2eb2ade9207b3af6",
	},
	"arm64": {
		url:    "https://github.com/railwayapp/nixpacks/releases/download/" + nixpacksVersion + "/nixpacks-" + nixpacksVersion + "-aarch64-unknown-linux-musl.tar.gz",
		sha256: "912bd02dd2bb6f9c3a9ed965fe8a68b4aa318dc7a2546e2eca6f2806a894ba39",
	},
}

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

// BuildNixpacks generates a Dockerfile from req's context with the nixpacks
// planner, then builds it through the T-33 Dockerfile pipeline. The planner is
// architecture-independent, so a multi-arch request plans exactly once and lets
// BuildKit fan out per platform. The build context stays the source tree; only
// the Dockerfile path changes to the generated one under .nixpacks/.
func (b *Builder) BuildNixpacks(ctx context.Context, req RunBuildRequest, events chan<- BuildEvent) (*BuildResult, error) {
	contextDir := filepath.Join(req.SourceDir, orDot(req.ContextDir))
	if err := b.plan(ctx, contextDir, events); err != nil {
		return nil, b.fail(events, err)
	}
	nixReq := req
	nixReq.Dockerfile = nixpacksOutSubdir + "/Dockerfile"
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

// plan runs the nixpacks planner over contextDir, streaming its logs as
// "plan"-phase events. It writes the generated Dockerfile + nix files into
// contextDir/.nixpacks (the build context stays contextDir).
func (b *Builder) plan(ctx context.Context, contextDir string, events chan<- BuildEvent) error {
	outDir := filepath.Join(contextDir, nixpacksOutSubdir)
	// Remove any leftovers from a previous attempt — a stale plan would give the
	// build the wrong context.
	if err := os.RemoveAll(outDir); err != nil {
		return fmt.Errorf("builder: clean nixpacks out: %w", err)
	}
	binPath, err := b.nixpacksBinary(ctx)
	if err != nil {
		return err
	}
	if err := b.rt.EnsureImage(ctx, nixpacksRunnerImage, nil, nil); err != nil {
		return fmt.Errorf("builder: pull nixpacks runner: %w", err)
	}

	id, err := b.rt.CreateContainer(ctx, runtime.ContainerSpec{
		Name:       fmt.Sprintf("zt-system-nixpacks-%d", b.clk.Now().UnixNano()),
		Image:      nixpacksRunnerImage,
		Entrypoint: []string{"/usr/local/bin/nixpacks"},
		// --out /src writes .nixpacks/ into the source; the generated Dockerfile
		// then COPYs the source (context) and that metadata. --name is unused
		// because we only generate, never build, here.
		Command: []string{"build", "/src", "--out", "/src", "--name", "ignored"},
		Mounts: []runtime.Mount{
			{HostPath: contextDir, Target: "/src"},
			{HostPath: binPath, Target: "/usr/local/bin/nixpacks", ReadOnly: true},
		},
		Labels: map[string]string{"zattera.system": "nixpacks"},
	})
	if err != nil {
		return fmt.Errorf("builder: create nixpacks: %w", err)
	}
	defer func() { _ = b.rt.RemoveContainer(context.Background(), id, true) }()
	if err := b.rt.StartContainer(ctx, id); err != nil {
		return fmt.Errorf("builder: start nixpacks: %w", err)
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
		return fmt.Errorf("builder: nixpacks plan: %w", werr)
	}
	<-done

	// The planner must have produced a Dockerfile we can build.
	if _, err := os.Stat(filepath.Join(outDir, "Dockerfile")); err != nil {
		return fmt.Errorf("builder: nixpacks produced no Dockerfile in %s", nixpacksOutSubdir)
	}
	return nil
}

// nixpacksBinary ensures the pinned nixpacks CLI is downloaded, checksum-verified
// and cached under the data dir, returning its host path. The binary is a static
// musl executable, so it runs unchanged inside the minimal runner image.
func (b *Builder) nixpacksBinary(ctx context.Context) (string, error) {
	arch := strings.TrimPrefix(b.native, "linux/")
	dl, ok := nixpacksDownloads[arch]
	if !ok {
		return "", fmt.Errorf("builder: no pinned nixpacks CLI for arch %q", arch)
	}
	dir := filepath.Join(b.dataDir, "nixpacks", nixpacksVersion)
	bin := filepath.Join(dir, "nixpacks")
	if _, err := os.Stat(bin); err == nil {
		return bin, nil // already cached
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("builder: nixpacks cache dir: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, dl.url, nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("builder: download nixpacks: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("builder: download nixpacks: HTTP %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("builder: download nixpacks: %w", err)
	}
	if got := fmt.Sprintf("%x", sha256.Sum256(raw)); got != dl.sha256 {
		return "", fmt.Errorf("builder: nixpacks checksum mismatch: got %s want %s", got, dl.sha256)
	}
	if err := extractNixpacksBinary(raw, bin); err != nil {
		return "", err
	}
	return bin, nil
}

// extractNixpacksBinary pulls the "nixpacks" entry out of the release tar.gz and
// writes it to dest (executable), publishing atomically so a concurrent build
// never sees a half-written binary.
func extractNixpacksBinary(targz []byte, dest string) error {
	gz, err := gzip.NewReader(bytes.NewReader(targz))
	if err != nil {
		return fmt.Errorf("builder: nixpacks gunzip: %w", err)
	}
	tr := tar.NewReader(gz)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return fmt.Errorf("builder: nixpacks binary not found in archive")
		}
		if err != nil {
			return fmt.Errorf("builder: nixpacks archive: %w", err)
		}
		if h.Typeflag != tar.TypeReg || filepath.Base(h.Name) != "nixpacks" {
			continue
		}
		tmp := dest + ".tmp"
		f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
		if err != nil {
			return err
		}
		if _, err := io.Copy(f, tr); err != nil { //nolint:gosec // size bounded by the pinned release
			_ = f.Close()
			_ = os.Remove(tmp)
			return fmt.Errorf("builder: write nixpacks: %w", err)
		}
		if err := f.Close(); err != nil {
			return err
		}
		return os.Rename(tmp, dest)
	}
}
