// Package platform normalizes and matches OCI platform strings ("linux/amd64").
// It is the single vocabulary for node arch reporting (T-87) and arch-aware
// placement (T-88): nodes report Local(), user input goes through Normalize(),
// and the scheduler filters with Supports().
package platform

import (
	"fmt"
	"runtime"
	"strings"
)

// Local is the platform string of the running binary, e.g. "linux/amd64".
func Local() string {
	return runtime.GOOS + "/" + runtime.GOARCH
}

// archAliases maps common spellings to the canonical GOARCH name.
var archAliases = map[string]string{
	"x86_64":   "amd64",
	"x86-64":   "amd64",
	"aarch64":  "arm64",
	"arm64/v8": "arm64",
	"armv7l":   "arm",
	"arm/v7":   "arm",
}

var knownOS = map[string]bool{"linux": true, "darwin": true, "windows": true, "freebsd": true}

var knownArch = map[string]bool{
	"amd64": true, "arm64": true, "arm": true, "386": true,
	"riscv64": true, "ppc64le": true, "s390x": true,
}

// Normalize lowercases and validates an "os/arch" platform string, mapping
// common arch aliases (x86_64→amd64, aarch64→arm64, arm64/v8→arm64). It
// returns an error for anything that is not a recognizable OCI platform.
func Normalize(s string) (string, error) {
	p := strings.ToLower(strings.TrimSpace(s))
	osPart, archPart, ok := strings.Cut(p, "/")
	if !ok || osPart == "" || archPart == "" {
		return "", fmt.Errorf("platform: %q is not an os/arch pair (want e.g. \"linux/amd64\")", s)
	}
	if a, ok := archAliases[archPart]; ok {
		archPart = a
	}
	if !knownOS[osPart] {
		return "", fmt.Errorf("platform: unknown os %q in %q", osPart, s)
	}
	if !knownArch[archPart] {
		return "", fmt.Errorf("platform: unknown arch %q in %q", archPart, s)
	}
	return osPart + "/" + archPart, nil
}

// Supports reports whether a node with the given os_arch can run an image
// constrained to platforms. An empty platforms list means "runs anywhere"
// (legacy releases and uninspectable images). Both sides are normalized
// best-effort so stored aliases still match.
func Supports(nodeArch string, platforms []string) bool {
	if len(platforms) == 0 {
		return true
	}
	node := loose(nodeArch)
	for _, p := range platforms {
		if loose(p) == node {
			return true
		}
	}
	return false
}

// loose normalizes for comparison, falling back to lowercase on invalid input.
func loose(s string) string {
	if n, err := Normalize(s); err == nil {
		return n
	}
	return strings.ToLower(strings.TrimSpace(s))
}
