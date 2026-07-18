package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/version"
)

// restartDelay gives the gRPC reply time to reach the control plane before the
// process goes away. Without it the caller sees a broken connection and cannot
// tell "upgrade staged" from "upgrade failed".
const restartDelay = 500 * time.Millisecond

// maxBinaryBytes bounds the download.
const maxBinaryBytes = 512 << 20

// UpgradeConfig configures self-upgrade on a node.
type UpgradeConfig struct {
	// AllowedBaseURL is the only prefix this node will download from. The
	// control plane picks the URL, but a node that blindly executed whatever
	// URL it was handed would turn any control-plane compromise into arbitrary
	// code on every node; this bounds that.
	AllowedBaseURL string
	// Client fetches the binary (nil = a default with a long timeout).
	Client *http.Client
	// Restart replaces the running process. Nil uses restartSelf.
	Restart func(context.Context) error
	// ExecPath overrides the binary location (tests).
	ExecPath string
	Logger   *slog.Logger
}

// UpgradeBinary downloads, verifies and installs a new zattera binary, then
// restarts the daemon (T-95).
//
// Workload containers are docker-managed and outlive this process — the
// executor returns on context cancel without stopping anything — so the restart
// costs this node's ingress and agent stream for a few seconds, not its
// workloads.
func (s *LocalServer) UpgradeBinary(ctx context.Context, req *clusterv1.AgentUpgradeRequest) (*clusterv1.AgentUpgradeResponse, error) {
	cfg := s.upgrade
	if cfg == nil {
		return nil, status.Error(codes.Unimplemented, "self-upgrade is not enabled on this node")
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}

	if err := checkAllowedURL(req.GetUrl(), cfg.AllowedBaseURL); err != nil {
		return nil, err
	}
	if req.GetSha256() == "" {
		return nil, status.Error(codes.InvalidArgument, "a sha256 is required; refusing to install an unverified binary")
	}

	exe := cfg.ExecPath
	if exe == "" {
		var err error
		if exe, err = os.Executable(); err != nil {
			return nil, status.Errorf(codes.Internal, "locate the running binary: %v", err)
		}
	}
	if resolved, err := filepath.EvalSymlinks(exe); err == nil {
		exe = resolved // /usr/local/bin/zt is a symlink to zattera
	}

	blob, err := s.download(ctx, cfg, req.GetUrl())
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(blob)
	if got := hex.EncodeToString(sum[:]); !strings.EqualFold(got, req.GetSha256()) {
		// Nothing has been written yet, so there is nothing to undo.
		return nil, status.Errorf(codes.DataLoss,
			"checksum mismatch for %s: got %s, want %s — not installing", req.GetUrl(), got, req.GetSha256())
	}

	if err := installBinary(exe, blob); err != nil {
		return nil, status.Errorf(codes.Internal, "install: %v", err)
	}
	log.Info("binary upgraded", "from", version.Version, "to", req.GetVersion(), "path", exe)

	restart := cfg.Restart
	if restart == nil {
		restart = restartSelf
	}
	go func() {
		time.Sleep(restartDelay)
		if err := restart(context.Background()); err != nil {
			// The new binary is already in place, so the next restart (systemd
			// Restart=always, or an operator) picks it up regardless.
			log.Error("restart after upgrade failed", "err", err)
		}
	}()

	return &clusterv1.AgentUpgradeResponse{
		PreviousVersion: version.Version,
		StagedVersion:   req.GetVersion(),
	}, nil
}

// checkAllowedURL enforces the node-side download allowlist.
func checkAllowedURL(url, allowed string) error {
	if url == "" {
		return status.Error(codes.InvalidArgument, "an asset URL is required")
	}
	if allowed == "*" {
		return nil // explicitly opted out (air-gapped/self-hosted mirrors)
	}
	if allowed == "" {
		return status.Error(codes.FailedPrecondition, "no upgrade base URL configured on this node")
	}
	if !strings.HasPrefix(url, strings.TrimRight(allowed, "/")+"/") {
		return status.Errorf(codes.PermissionDenied,
			"refusing to download from %s: this node only accepts %s", url, allowed)
	}
	return nil
}

func (s *LocalServer) download(ctx context.Context, cfg *UpgradeConfig, url string) ([]byte, error) {
	client := cfg.Client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Minute}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "bad URL: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "download %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, status.Errorf(codes.Unavailable, "download %s: HTTP %d", url, resp.StatusCode)
	}
	blob, err := io.ReadAll(io.LimitReader(resp.Body, maxBinaryBytes))
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "download %s: %v", url, err)
	}
	if len(blob) == 0 {
		return nil, status.Errorf(codes.Unavailable, "download %s: empty response", url)
	}
	return blob, nil
}

// installBinary keeps the running binary as <exe>.prev and swaps the new one in
// atomically. The .prev copy is what makes `cluster upgrade --rollback`
// possible without another download.
func installBinary(exe string, blob []byte) error {
	dir := filepath.Dir(exe)
	mode := os.FileMode(0o755)
	if info, err := os.Stat(exe); err == nil {
		mode = info.Mode().Perm()
		// Copy rather than rename: renaming the running binary would leave no
		// file at exe if the write below fails.
		if cur, rerr := os.ReadFile(exe); rerr == nil {
			if werr := os.WriteFile(exe+".prev", cur, mode); werr != nil {
				return fmt.Errorf("save previous binary: %w", werr)
			}
		}
	}
	tmp := filepath.Join(dir, ".zattera.new")
	if err := os.WriteFile(tmp, blob, mode); err != nil {
		return fmt.Errorf("write new binary: %w", err)
	}
	// Rename over a running executable is fine on unix: the kernel keeps the
	// open inode alive for this process, and the next exec picks up the new one.
	if err := os.Rename(tmp, exe); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("swap binary: %w", err)
	}
	return nil
}

// restartSelf restarts the daemon. Under systemd that is a unit restart (the
// unit's ExecStart already points at the swapped path); otherwise the process
// exits and leaves it to whatever supervises it.
func restartSelf(ctx context.Context) error {
	if _, err := exec.LookPath("systemctl"); err == nil {
		if out, err := exec.CommandContext(ctx, "systemctl", "restart", "zattera").CombinedOutput(); err != nil {
			return fmt.Errorf("systemctl restart zattera: %v: %s", err, strings.TrimSpace(string(out)))
		}
		return nil
	}
	return execSelf()
}
