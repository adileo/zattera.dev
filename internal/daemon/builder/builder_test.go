package builder

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"testing"
	"time"

	bkclient "github.com/moby/buildkit/client"
)

func TestResolvePlatforms(t *testing.T) {
	if got := resolvePlatforms(nil, "linux/arm64"); len(got) != 1 || got[0] != "linux/arm64" {
		t.Fatalf("empty → native failed: %v", got)
	}
	req := []string{"linux/amd64", "linux/arm64"}
	if got := resolvePlatforms(req, "linux/arm64"); len(got) != 2 {
		t.Fatalf("explicit platforms dropped: %v", got)
	}
}

func TestFrontendPlatformAttr(t *testing.T) {
	if got := frontendPlatformAttr([]string{"linux/amd64", "linux/arm64"}); got != "linux/amd64,linux/arm64" {
		t.Fatalf("attr = %q", got)
	}
	if got := frontendPlatformAttr([]string{"linux/arm64"}); got != "linux/arm64" {
		t.Fatalf("single attr = %q", got)
	}
}

func TestPlatformsNeedingEmulation(t *testing.T) {
	native := "linux/arm64"
	// Native only → nothing to emulate.
	if got := platformsNeedingEmulation(native, []string{native}); len(got) != 0 {
		t.Fatalf("native should need no emulation, got %v", got)
	}
	// A foreign arch is flagged; the native one is skipped.
	got := platformsNeedingEmulation(native, []string{"linux/arm64", "linux/amd64"})
	if len(got) != 1 || got[0] != "linux/amd64" {
		t.Fatalf("needing = %v, want [linux/amd64]", got)
	}
}

func TestSolveStatusLines(t *testing.T) {
	now := time.Now()
	s := &bkclient.SolveStatus{
		Vertexes: []*bkclient.Vertex{
			{Name: "[1/2] FROM alpine", Completed: &now},
			{Name: "[2/2] RUN build", Cached: true},
			{Name: "", Completed: &now}, // no name → skipped
		},
		Logs: []*bkclient.VertexLog{
			{Data: []byte("compiling...\nlinking...\n")},
			{Data: []byte("")}, // empty → skipped
		},
	}
	lines := solveStatusLines(s)
	want := []string{"[1/2] FROM alpine", "CACHED [2/2] RUN build", "compiling...", "linking..."}
	if len(lines) != len(want) {
		t.Fatalf("lines = %v, want %v", lines, want)
	}
	for i := range want {
		if lines[i] != want[i] {
			t.Fatalf("line[%d] = %q, want %q", i, lines[i], want[i])
		}
	}
}

func TestUnpackSourceHappy(t *testing.T) {
	dest := t.TempDir()
	tgz := makeTarGz(t, map[string]string{
		"Dockerfile":  "FROM alpine\n",
		"app/main.go": "package main\n",
		"app/":        "", // dir entry
	})
	if err := unpackSource(dest, bytes.NewReader(tgz)); err != nil {
		t.Fatalf("unpack: %v", err)
	}
	if b, err := os.ReadFile(filepath.Join(dest, "Dockerfile")); err != nil || string(b) != "FROM alpine\n" {
		t.Fatalf("Dockerfile = %q err=%v", b, err)
	}
	if b, err := os.ReadFile(filepath.Join(dest, "app", "main.go")); err != nil || string(b) != "package main\n" {
		t.Fatalf("main.go = %q err=%v", b, err)
	}
}

func TestUnpackSourceRejectsTraversal(t *testing.T) {
	dest := t.TempDir()
	tgz := makeTarGz(t, map[string]string{"../escape.txt": "pwned"})
	if err := unpackSource(dest, bytes.NewReader(tgz)); err == nil {
		t.Fatal("expected traversal to be rejected")
	}
	// The escaped file must not exist in the parent dir.
	if _, err := os.Stat(filepath.Join(filepath.Dir(dest), "escape.txt")); err == nil {
		t.Fatal("traversal wrote outside the context dir")
	}
}

func TestUnpackSourceContextCap(t *testing.T) {
	dest := t.TempDir()
	// A tar header claiming a huge file must trip the size cap.
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	_ = tw.WriteHeader(&tar.Header{Name: "big", Typeflag: tar.TypeReg, Size: maxContextBytes + 1, Mode: 0o644})
	// Body doesn't need to be fully written for the cap check to trigger.
	_, _ = tw.Write(make([]byte, 1024))
	_ = tw.Close()
	_ = gz.Close()
	if err := unpackSource(dest, bytes.NewReader(buf.Bytes())); err == nil {
		t.Fatal("expected context size cap to be enforced")
	}
}

func TestSafeJoin(t *testing.T) {
	base := t.TempDir()
	ok := []string{"a.txt", "dir/b.txt", "/etc/passwd"} // absolute is neutralised into base
	for _, n := range ok {
		if _, err := safeJoin(base, n); err != nil {
			t.Errorf("safeJoin(%q) unexpectedly rejected: %v", n, err)
		}
	}
	bad := []string{"../evil", "dir/../../evil", "../../etc/shadow"}
	for _, n := range bad {
		if _, err := safeJoin(base, n); err == nil {
			t.Errorf("safeJoin(%q) should be rejected", n)
		}
	}
}

// makeTarGz builds a gzipped tar from name→content (a trailing "/" name with
// empty content is a directory entry).
func makeTarGz(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for name, content := range files {
		if len(name) > 0 && name[len(name)-1] == '/' {
			if err := tw.WriteHeader(&tar.Header{Name: name, Typeflag: tar.TypeDir, Mode: 0o755}); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := tw.WriteHeader(&tar.Header{Name: name, Typeflag: tar.TypeReg, Size: int64(len(content)), Mode: 0o644}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(content)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}
