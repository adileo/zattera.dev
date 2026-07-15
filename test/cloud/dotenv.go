//go:build cloud

package cloud

import (
	"bufio"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// loadDotEnv loads KEY=VALUE pairs from the repo-root .env into the process
// environment for any key NOT already set (a real environment variable always
// wins). It is called once from TestMain so every cloud scenario can pick up
// HCLOUD_TOKEN (and the ZT_CLOUD_* knobs) from .env without exporting them.
// Best-effort: a missing or malformed .env is silently ignored.
//
// Supported lines: `KEY=value`, `export KEY=value`, `# comments`, blank lines,
// and single/double-quoted values. This is intentionally minimal — for anything
// fancier, export the variable yourself.
func loadDotEnv() {
	root := repoRootDir()
	if root == "" {
		return
	}
	f, err := os.Open(filepath.Join(root, ".env"))
	if err != nil {
		return
	}
	defer func() { _ = f.Close() }()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		if key == "" || os.Getenv(key) != "" {
			continue // real env wins; never override
		}
		_ = os.Setenv(key, trimQuotes(strings.TrimSpace(val)))
	}
}

func trimQuotes(s string) string {
	if len(s) >= 2 && (s[0] == '"' || s[0] == '\'') && s[len(s)-1] == s[0] {
		return s[1 : len(s)-1]
	}
	return s
}

// repoRootDir returns the module root relative to this file (test/cloud → ../..).
func repoRootDir() string {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return ""
	}
	return filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
}
