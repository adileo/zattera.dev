//go:build cloud

package cloud

import (
	"os"
	"testing"
)

// TestMain loads .env (best-effort) before any cloud scenario runs, so
// HCLOUD_TOKEN and the ZT_CLOUD_* knobs can live in the repo-root .env
// (gitignored) instead of being exported by hand.
func TestMain(m *testing.M) {
	loadDotEnv()
	os.Exit(m.Run())
}
