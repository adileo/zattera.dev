package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
)

// upgradeFixture serves a fake binary and points a LocalServer at a temp
// "installed" binary it is allowed to replace.
func upgradeFixture(t *testing.T, payload []byte) (*LocalServer, string, string, *atomic.Int32) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/missing") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write(payload)
	}))
	t.Cleanup(srv.Close)

	dir := t.TempDir()
	exe := filepath.Join(dir, "zattera")
	if err := os.WriteFile(exe, []byte("OLD BINARY"), 0o755); err != nil {
		t.Fatal(err)
	}

	var restarts atomic.Int32
	ls := &LocalServer{}
	ls.EnableSelfUpgrade(&UpgradeConfig{
		AllowedBaseURL: srv.URL + "/releases",
		ExecPath:       exe,
		Restart: func(context.Context) error {
			restarts.Add(1)
			return nil
		},
	})
	return ls, srv.URL + "/releases/download/v0.4.0/zattera-linux-amd64", exe, &restarts
}

func digest(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// TestUpgradeBinary covers the happy path: verified download, .prev retained,
// binary swapped, restart triggered.
func TestUpgradeBinary(t *testing.T) {
	payload := []byte("NEW BINARY CONTENTS")
	ls, url, exe, restarts := upgradeFixture(t, payload)

	resp, err := ls.UpgradeBinary(context.Background(), &clusterv1.AgentUpgradeRequest{
		Version: "v0.4.0", Url: url, Sha256: digest(payload),
	})
	if err != nil {
		t.Fatalf("upgrade: %v", err)
	}
	if resp.GetStagedVersion() != "v0.4.0" {
		t.Errorf("staged version = %q", resp.GetStagedVersion())
	}

	got, err := os.ReadFile(exe)
	if err != nil || string(got) != string(payload) {
		t.Fatalf("binary not swapped: %q err=%v", got, err)
	}
	// The previous binary must survive, or --rollback has nothing to restore.
	prev, err := os.ReadFile(exe + ".prev")
	if err != nil || string(prev) != "OLD BINARY" {
		t.Fatalf(".prev not retained: %q err=%v", prev, err)
	}
	if info, err := os.Stat(exe); err != nil || info.Mode().Perm()&0o111 == 0 {
		t.Errorf("swapped binary is not executable: %v", info.Mode())
	}

	// The restart is deferred so the reply lands first.
	deadline := time.Now().Add(3 * time.Second)
	for restarts.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if restarts.Load() != 1 {
		t.Errorf("restart count = %d, want 1", restarts.Load())
	}
}

// TestUpgradeBinaryChecksumMismatch is the security-critical case: a payload
// that does not match the control plane's digest must never be installed.
func TestUpgradeBinaryChecksumMismatch(t *testing.T) {
	ls, url, exe, restarts := upgradeFixture(t, []byte("TAMPERED"))

	_, err := ls.UpgradeBinary(context.Background(), &clusterv1.AgentUpgradeRequest{
		Version: "v0.4.0", Url: url, Sha256: digest([]byte("WHAT WE EXPECTED")),
	})
	if status.Code(err) != codes.DataLoss {
		t.Fatalf("mismatch error = %v, want DataLoss", err)
	}
	got, _ := os.ReadFile(exe)
	if string(got) != "OLD BINARY" {
		t.Fatalf("binary was replaced despite a checksum mismatch: %q", got)
	}
	if _, err := os.Stat(exe + ".prev"); err == nil {
		t.Error("a rejected upgrade wrote .prev; nothing should have been touched")
	}
	if restarts.Load() != 0 {
		t.Error("a rejected upgrade restarted the daemon")
	}
}

// TestUpgradeBinaryRejectsForeignURL: the node bounds where it will download
// from, so a control plane cannot point it at arbitrary code.
func TestUpgradeBinaryRejectsForeignURL(t *testing.T) {
	payload := []byte("EVIL")
	ls, _, exe, _ := upgradeFixture(t, payload)

	_, err := ls.UpgradeBinary(context.Background(), &clusterv1.AgentUpgradeRequest{
		Version: "v0.4.0", Url: "https://evil.test/zattera-linux-amd64", Sha256: digest(payload),
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("foreign URL error = %v, want PermissionDenied", err)
	}
	if got, _ := os.ReadFile(exe); string(got) != "OLD BINARY" {
		t.Fatal("binary replaced from a disallowed URL")
	}
}

// TestUpgradeBinaryRequiresChecksum: no digest, no install. An empty checksum
// must not be read as "skip verification".
func TestUpgradeBinaryRequiresChecksum(t *testing.T) {
	ls, url, exe, _ := upgradeFixture(t, []byte("ANYTHING"))
	_, err := ls.UpgradeBinary(context.Background(), &clusterv1.AgentUpgradeRequest{Version: "v0.4.0", Url: url})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing checksum error = %v, want InvalidArgument", err)
	}
	if got, _ := os.ReadFile(exe); string(got) != "OLD BINARY" {
		t.Fatal("binary replaced without a checksum")
	}
}

// TestUpgradeBinaryDownloadFailure leaves the node untouched.
func TestUpgradeBinaryDownloadFailure(t *testing.T) {
	ls, url, exe, restarts := upgradeFixture(t, []byte("X"))
	bad := strings.Replace(url, "zattera-linux-amd64", "missing", 1)

	_, err := ls.UpgradeBinary(context.Background(), &clusterv1.AgentUpgradeRequest{
		Version: "v0.4.0", Url: bad, Sha256: digest([]byte("X")),
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("download failure = %v, want Unavailable", err)
	}
	if got, _ := os.ReadFile(exe); string(got) != "OLD BINARY" {
		t.Fatal("binary replaced after a failed download")
	}
	if restarts.Load() != 0 {
		t.Error("restarted after a failed download")
	}
}

// TestUpgradeBinaryDisabled: a node without self-upgrade enabled says so.
func TestUpgradeBinaryDisabled(t *testing.T) {
	ls := &LocalServer{}
	_, err := ls.UpgradeBinary(context.Background(), &clusterv1.AgentUpgradeRequest{Url: "https://x/y", Sha256: digest(nil)})
	if status.Code(err) != codes.Unimplemented {
		t.Errorf("disabled self-upgrade = %v, want Unimplemented", err)
	}
}

// TestCheckAllowedURL pins the allowlist rules, including that an unset base is
// closed rather than open.
func TestCheckAllowedURL(t *testing.T) {
	cases := []struct {
		url, allowed string
		wantCode     codes.Code
	}{
		{"https://host/releases/download/v1/zattera-linux-amd64", "https://host/releases", codes.OK},
		{"https://host/releases/download/v1/zattera-linux-amd64", "https://host/releases/", codes.OK},
		{"https://evil/releases/download/v1/x", "https://host/releases", codes.PermissionDenied},
		// Prefix matching must not accept a sibling path that merely starts the same.
		{"https://host/releases-evil/x", "https://host/releases", codes.PermissionDenied},
		{"https://anything/at/all", "*", codes.OK},
		{"https://host/releases/x", "", codes.FailedPrecondition},
		{"", "https://host/releases", codes.InvalidArgument},
	}
	for _, tc := range cases {
		err := checkAllowedURL(tc.url, tc.allowed)
		if status.Code(err) != tc.wantCode {
			t.Errorf("checkAllowedURL(%q, %q) = %v, want %v", tc.url, tc.allowed, status.Code(err), tc.wantCode)
		}
	}
}
