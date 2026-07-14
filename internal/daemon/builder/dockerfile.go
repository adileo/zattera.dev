// Package builder builds container images from source using a managed
// buildkitd (spec §3.4). T-33 covers Dockerfile builds — including multi-arch
// image indexes via BuildKit's multi-platform solve — pushed straight to the
// embedded registry. Nixpacks source resolution is layered on in T-34, and the
// build queue/dispatch/RPC in T-35.
package builder

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// maxContextBytes caps an unpacked build context (spec: 512MB) so a malicious
// or runaway tarball cannot fill the builder's disk.
const maxContextBytes = 512 << 20

// localPlatform is this node's own OCI platform (e.g. "linux/arm64"). Builds
// with no explicit platform target it, so the single-node/native path never
// needs emulation.
func localPlatform() string { return runtime.GOOS + "/" + runtime.GOARCH }

// resolvePlatforms returns the platforms to build for: the request's list, or
// the builder's native platform when none is given.
func resolvePlatforms(requested []string, native string) []string {
	if len(requested) == 0 {
		return []string{native}
	}
	return requested
}

// frontendPlatformAttr encodes platforms for BuildKit's dockerfile frontend
// `platform` attribute — a comma-separated list. The frontend fans the build
// out per platform and the image exporter emits an OCI index for more than one.
func frontendPlatformAttr(platforms []string) string {
	return strings.Join(platforms, ",")
}

// platformsNeedingEmulation returns the target platforms that differ from the
// builder's native platform and therefore require QEMU binfmt handlers.
func platformsNeedingEmulation(native string, targets []string) []string {
	var need []string
	for _, p := range targets {
		if p != native {
			need = append(need, p)
		}
	}
	return need
}

// unpackSource extracts a gzipped tar build context into destDir, rejecting any
// entry whose path would escape destDir (path traversal) and capping the total
// extracted size. Symlinks are skipped for safety (build contexts rarely need
// them, and a symlink can point outside the tree).
func unpackSource(destDir string, r io.Reader) error {
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return fmt.Errorf("builder: prepare context dir: %w", err)
	}
	gz, err := gzip.NewReader(r)
	if err != nil {
		return fmt.Errorf("builder: gzip: %w", err)
	}
	defer func() { _ = gz.Close() }()

	tr := tar.NewReader(gz)
	var total int64
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("builder: tar: %w", err)
		}
		target, err := safeJoin(destDir, hdr.Name)
		if err != nil {
			return err
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return fmt.Errorf("builder: mkdir %s: %w", hdr.Name, err)
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return fmt.Errorf("builder: mkdir parent %s: %w", hdr.Name, err)
			}
			total += hdr.Size
			if total > maxContextBytes {
				return fmt.Errorf("builder: context exceeds %d bytes", maxContextBytes)
			}
			if err := writeFile(target, tr, hdr.Size, os.FileMode(hdr.Mode)&0o777); err != nil {
				return err
			}
		case tar.TypeSymlink, tar.TypeLink:
			// Skip links: they are a traversal risk and unnecessary for builds.
			continue
		default:
			continue
		}
	}
	return nil
}

func writeFile(path string, r io.Reader, size int64, mode os.FileMode) error {
	if mode == 0 {
		mode = 0o644
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return fmt.Errorf("builder: create %s: %w", path, err)
	}
	// Limit the copy to the declared size to avoid a decompression bomb writing
	// past what the header claims.
	if _, err := io.CopyN(f, r, size); err != nil && err != io.EOF {
		_ = f.Close()
		return fmt.Errorf("builder: write %s: %w", path, err)
	}
	return f.Close()
}

// safeJoin joins name under base and rejects any result that escapes base.
// Absolute paths and leading "../" are neutralised by Join+Clean; an entry that
// still lands outside base (e.g. "../evil") is rejected outright.
func safeJoin(base, name string) (string, error) {
	target := filepath.Join(base, name)
	cleanBase := filepath.Clean(base)
	if target != cleanBase && !strings.HasPrefix(target, cleanBase+string(os.PathSeparator)) {
		return "", fmt.Errorf("builder: unsafe path in context: %q", name)
	}
	return target, nil
}
