package daemon

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/zattera-dev/zattera/internal/config"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/platform"
)

// Boot registration is the one place Node.os_arch must always be right (T-87):
// arch-aware placement reads the field, so a node that registers without it
// silently becomes "runs anything".
func TestRegisterLocalNodeSetsOsArch(t *testing.T) {
	rs := raftstore.NewTestStore(t)
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := config.Config{NodeName: "boot-1", DataDir: t.TempDir(), Roles: []string{config.RoleControl, config.RoleWorker}}

	if err := registerLocalNode(context.Background(), rs, cfg, "node-boot-1", log); err != nil {
		t.Fatalf("registerLocalNode: %v", err)
	}

	node, ok := rs.State().Node("node-boot-1")
	if !ok {
		t.Fatal("node not registered")
	}
	if got := node.GetOsArch(); got != platform.Local() {
		t.Fatalf("os_arch = %q, want %q", got, platform.Local())
	}
	if got := node.GetLabels()["zattera.dev/os-arch"]; got != platform.Local() {
		t.Fatalf("os-arch label = %q, want %q", got, platform.Local())
	}
}
