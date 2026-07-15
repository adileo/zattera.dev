package platform

import (
	"runtime"
	"strings"
	"testing"
)

func TestLocal(t *testing.T) {
	want := runtime.GOOS + "/" + runtime.GOARCH
	if got := Local(); got != want {
		t.Fatalf("Local() = %q, want %q", got, want)
	}
	if _, err := Normalize(Local()); err != nil {
		t.Fatalf("Local() %q does not normalize: %v", Local(), err)
	}
}

func TestNormalize(t *testing.T) {
	tests := []struct {
		in   string
		want string
		err  bool
	}{
		{"linux/amd64", "linux/amd64", false},
		{"Linux/AMD64", "linux/amd64", false},
		{" linux/arm64 ", "linux/arm64", false},
		{"linux/x86_64", "linux/amd64", false},
		{"linux/x86-64", "linux/amd64", false},
		{"linux/aarch64", "linux/arm64", false},
		{"linux/arm64/v8", "linux/arm64", false},
		{"linux/arm/v7", "linux/arm", false},
		{"darwin/arm64", "darwin/arm64", false},
		{"linux/riscv64", "linux/riscv64", false},
		{"linux", "", true},
		{"", "", true},
		{"linux/", "", true},
		{"/amd64", "", true},
		{"plan9/amd64", "", true},
		{"linux/sparc", "", true},
	}
	for _, tt := range tests {
		got, err := Normalize(tt.in)
		if tt.err {
			if err == nil {
				t.Errorf("Normalize(%q) = %q, want error", tt.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("Normalize(%q): %v", tt.in, err)
			continue
		}
		if got != tt.want {
			t.Errorf("Normalize(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestNormalizeErrorIsActionable(t *testing.T) {
	_, err := Normalize("bogus")
	if err == nil || !strings.Contains(err.Error(), "linux/amd64") {
		t.Fatalf("error should show the expected format, got %v", err)
	}
}

func TestSupports(t *testing.T) {
	tests := []struct {
		name      string
		nodeArch  string
		platforms []string
		want      bool
	}{
		{"empty platforms = any node", "linux/amd64", nil, true},
		{"empty platforms = any node (arm)", "linux/arm64", []string{}, true},
		{"exact match", "linux/amd64", []string{"linux/amd64"}, true},
		{"mismatch", "linux/arm64", []string{"linux/amd64"}, false},
		{"multi-platform hits", "linux/arm64", []string{"linux/amd64", "linux/arm64"}, true},
		{"alias in platforms", "linux/amd64", []string{"linux/x86_64"}, true},
		{"alias on node side", "linux/aarch64", []string{"linux/arm64"}, true},
		{"case-insensitive", "Linux/AMD64", []string{"linux/amd64"}, true},
		{"unknown node arch never matches constrained", "", []string{"linux/amd64"}, false},
		{"unknown node arch runs unconstrained", "", nil, true},
	}
	for _, tt := range tests {
		if got := Supports(tt.nodeArch, tt.platforms); got != tt.want {
			t.Errorf("%s: Supports(%q, %v) = %v, want %v", tt.name, tt.nodeArch, tt.platforms, got, tt.want)
		}
	}
}
