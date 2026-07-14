package cli

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// writeFiles creates a tree of files under dir (keys are slash paths).
func writeFiles(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	for name, content := range files {
		full := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// tarEntries decompresses a tar.gz and returns its entry names, sorted.
func tarEntries(t *testing.T, b []byte) []string {
	t.Helper()
	gz, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(gz)
	var names []string
	for {
		hdr, err := tr.Next()
		if err != nil {
			break
		}
		names = append(names, hdr.Name)
	}
	sort.Strings(names)
	return names
}

func TestDeploySourceTarDeterminism(t *testing.T) {
	dir := t.TempDir()
	writeFiles(t, dir, map[string]string{
		"main.go":     "package main\n",
		"lib/util.go": "package lib\n",
		"README.md":   "# hi\n",
	})

	var a, b bytes.Buffer
	if err := writeSourceTar(dir, &a); err != nil {
		t.Fatal(err)
	}
	if err := writeSourceTar(dir, &b); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a.Bytes(), b.Bytes()) {
		t.Fatal("tar output is not byte-identical across runs")
	}
}

func TestDeploySourceIgnoreHandling(t *testing.T) {
	dir := t.TempDir()
	writeFiles(t, dir, map[string]string{
		"main.go":           "package main\n",
		"app.log":           "noise\n",       // *.log → ignored
		"keep.log":          "kept\n",        // !keep.log → re-included
		"sub/deep.go":       "package sub\n", // kept
		"sub/temp.log":      "noise\n",       // *.log at depth → ignored
		"build/out.bin":     "binary\n",      // build/ → ignored
		"node_modules/x.js": "n\n",           // .zatteraignore node_modules
		"secret.txt":        "s3cr3t\n",      // /secret.txt → ignored
		".git/config":       "[core]\n",      // always ignored
		".gitignore":        "*.log\n!keep.log\nbuild/\n/secret.txt\n",
		".zatteraignore":    "node_modules\n",
	})

	var buf bytes.Buffer
	if err := writeSourceTar(dir, &buf); err != nil {
		t.Fatal(err)
	}
	got := tarEntries(t, buf.Bytes())

	want := []string{".gitignore", ".zatteraignore", "keep.log", "main.go", "sub/deep.go"}
	if len(got) != len(want) {
		t.Fatalf("entries = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("entries = %v, want %v", got, want)
		}
	}

	// Spot-check the explicitly-excluded paths never appear.
	for _, bad := range []string{"app.log", "sub/temp.log", "build/out.bin", "node_modules/x.js", "secret.txt", ".git/config"} {
		for _, e := range got {
			if e == bad {
				t.Errorf("%s should have been ignored", bad)
			}
		}
	}
}

func TestDeploySourceZeroedMetadata(t *testing.T) {
	dir := t.TempDir()
	writeFiles(t, dir, map[string]string{"a.txt": "x"})
	var buf bytes.Buffer
	if err := writeSourceTar(dir, &buf); err != nil {
		t.Fatal(err)
	}
	gz, _ := gzip.NewReader(bytes.NewReader(buf.Bytes()))
	tr := tar.NewReader(gz)
	hdr, err := tr.Next()
	if err != nil {
		t.Fatal(err)
	}
	if hdr.Uid != 0 || hdr.Gid != 0 {
		t.Errorf("uid/gid not zeroed: %d/%d", hdr.Uid, hdr.Gid)
	}
	if !hdr.ModTime.Equal(hdr.ModTime.Truncate(1)) || hdr.ModTime.Unix() != 0 {
		t.Errorf("modtime not zeroed: %v", hdr.ModTime)
	}
}
