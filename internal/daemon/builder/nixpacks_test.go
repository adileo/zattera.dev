package builder

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveBuildType(t *testing.T) {
	// A Dockerfile present → Dockerfile build.
	withDockerfile := t.TempDir()
	if err := os.WriteFile(filepath.Join(withDockerfile, "Dockerfile"), []byte("FROM alpine\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := ResolveBuildType(withDockerfile); got != BuildDockerfile {
		t.Errorf("with Dockerfile → %v, want BuildDockerfile", got)
	}

	// No Dockerfile → nixpacks.
	noDockerfile := t.TempDir()
	if err := os.WriteFile(filepath.Join(noDockerfile, "package.json"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := ResolveBuildType(noDockerfile); got != BuildNixpacks {
		t.Errorf("without Dockerfile → %v, want BuildNixpacks", got)
	}

	// A directory literally named "Dockerfile" must not count as a Dockerfile.
	dirNamed := t.TempDir()
	if err := os.Mkdir(filepath.Join(dirNamed, "Dockerfile"), 0o755); err != nil {
		t.Fatal(err)
	}
	if got := ResolveBuildType(dirNamed); got != BuildNixpacks {
		t.Errorf("Dockerfile-as-dir → %v, want BuildNixpacks", got)
	}

	// Empty dir → nixpacks (planner decides what to do).
	if got := ResolveBuildType(t.TempDir()); got != BuildNixpacks {
		t.Errorf("empty dir → %v, want BuildNixpacks", got)
	}
}
