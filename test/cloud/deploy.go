//go:build cloud

package cloud

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// LoginCLI builds the zattera CLI for the local platform and logs a throwaway
// context in against the control node's public API. TLS verification is skipped
// (--insecure): the cluster is ephemeral and its self-signed API cert carries
// only 127.0.0.1 + the mesh IP as SANs, not the public IP we dial.
func (c *Cluster) LoginCLI() {
	c.T.Helper()
	if c.control == nil {
		c.T.Fatal("cloud: LoginCLI requires StartControl first")
	}
	c.cliBin = c.buildLocalCLI()
	c.cliConfig = filepath.Join(c.keyDir, "cli-config.toml")
	c.mustCLI("login",
		"--server", "https://"+net.JoinHostPort(c.control.PublicIPv4(), "8443"),
		"--token", c.control.bootstrapToken,
		"--insecure")
}

// buildLocalCLI builds the zattera binary for the HOST platform (it doubles as
// the CLI) into the shared bin dir; cached across calls.
func (c *Cluster) buildLocalCLI() string {
	c.T.Helper()
	if c.binDir == "" {
		c.binDir = c.T.TempDir()
	}
	out := filepath.Join(c.binDir, "zattera-cli")
	if _, err := os.Stat(out); err == nil {
		return out
	}
	c.T.Logf("cloud: building local zattera CLI (%s/%s)", runtime.GOOS, runtime.GOARCH)
	cmd := exec.CommandContext(c.Ctx, "go", "build", "-o", out, "./cmd/zattera")
	cmd.Dir = repoRootDir()
	if b, err := cmd.CombinedOutput(); err != nil {
		c.T.Fatalf("cloud: build CLI: %v\n%s", err, b)
	}
	return out
}

// cli runs the zattera CLI with an isolated config (dir "" = repo root).
func (c *Cluster) cli(dir string, args ...string) (string, error) {
	cmd := exec.CommandContext(c.Ctx, c.cliBin, args...)
	if dir != "" {
		cmd.Dir = dir
	}
	cmd.Env = append(os.Environ(), "ZATTERA_CONFIG="+c.cliConfig, "NO_COLOR=1")
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	return out.String(), cmd.Run()
}

// mustCLI runs the CLI and fails the test on a non-zero exit.
func (c *Cluster) mustCLI(args ...string) string {
	c.T.Helper()
	out, err := c.cli("", args...)
	if err != nil {
		c.T.Fatalf("cloud: cli %v: %v\n%s", args, err, out)
	}
	return out
}

// TrustRegistryCA distributes the cluster CA to every node's Docker trust store
// for the embedded registry (control mesh IP:5000), so workers can pull built
// images over TLS. Required for source deploys whose replicas land on workers.
func (c *Cluster) TrustRegistryCA() {
	c.T.Helper()
	caPEM := c.control.MustRun("cat /var/lib/zattera/ca/ca.crt")
	ctrl := c.NodeByName(c.control.Name())
	if ctrl == nil || ctrl.GetMeshIp() == "" {
		c.T.Fatal("cloud: control mesh IP unknown; cannot configure registry trust")
	}
	regAddr := ctrl.GetMeshIp() + ":5000"
	for _, node := range c.nodes {
		node.Push([]byte(caPEM), fmt.Sprintf("/etc/docker/certs.d/%s/ca.crt", regAddr), "0644")
	}
	c.T.Logf("cloud: registry CA trusted for %s on %d node(s)", regAddr, len(c.nodes))
}

// WaitHealthyReplicas polls `ps` until at least want replicas of app report
// HEALTHY, returning the set of node identifiers they run on. The generous
// default timeout covers a cold source build (buildkit + go build in-container).
func (c *Cluster) WaitHealthyReplicas(project, app string, want int, timeout time.Duration) map[string]bool {
	c.T.Helper()
	deadline := time.Now().Add(timeout)
	var lastPs string
	for time.Now().Before(deadline) {
		ps, err := c.cli("", "ps", "--app", app, "--project", project)
		lastPs = ps
		if err == nil {
			healthy, nodes := parseHealthyReplicas(ps)
			if healthy >= want {
				c.T.Logf("cloud: %d healthy replica(s) of %s across %d node(s)\n%s", healthy, app, len(nodes), ps)
				return nodes
			}
		}
		time.Sleep(5 * time.Second)
	}
	c.T.Fatalf("cloud: %s never reached %d healthy replicas within %s\nlast ps:\n%s", app, want, timeout, lastPs)
	return nil
}

// parseHealthyReplicas counts HEALTHY rows in a `ps` table and collects their
// nodes. Columns: APP ENV RELEASE NODE STATE RESTARTS.
func parseHealthyReplicas(ps string) (int, map[string]bool) {
	nodes := map[string]bool{}
	healthy := 0
	for _, line := range strings.Split(ps, "\n") {
		f := strings.Fields(line)
		if len(f) >= 5 && f[4] == "HEALTHY" {
			healthy++
			nodes[f[3]] = true
		}
	}
	return healthy, nodes
}

// PollAppBody polls the app through a node's OWN ingress until the response body
// contains want. It follows the :80 → 308 → :443 redirect and accepts the
// self-signed cert (-k), resolving the app host to loopback so no public DNS or
// ingress exposure is needed.
func (c *Cluster) PollAppBody(node *Node, host, want string, timeout time.Duration) {
	c.T.Helper()
	cmd := fmt.Sprintf("curl -ksSL --max-time 10 --resolve %s:80:127.0.0.1 --resolve %s:443:127.0.0.1 http://%s/",
		host, host, host)
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		out, _ := node.Run(cmd)
		if strings.Contains(out, want) {
			c.T.Logf("cloud: app serving via ingress: %q", strings.TrimSpace(out))
			return
		}
		last = out
		time.Sleep(3 * time.Second)
	}
	c.T.Fatalf("cloud: app never served %q via %s ingress; last response: %q", want, node.Name(), last)
}
