//go:build cloud

package cloud

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestMeshsockRelay verifies the DERP-lite relay (T-58) end to end on real
// infra. Two PUBLIC workers run the meshsock datapath and are paired directly.
// We then drop WireGuard UDP between the two workers (but not to the control
// hub), so no direct or hole-punched path can form — their only remaining
// worker↔worker path is the control-node TCP relay. The test asserts mesh-IP
// reachability between them, which can only succeed over the relay.
//
//	HCLOUD_TOKEN=... go test -tags cloud ./test/cloud/ -run TestMeshsockRelay -v -timeout 40m
func TestMeshsockRelay(t *testing.T) {
	c := NewCluster(t)

	control := c.StartControl("amd64", "")
	a := c.JoinMeshsockWorker("amd64")
	b := c.JoinMeshsockWorker("amd64")
	c.WaitNodesReady(3)
	t.Logf("cloud: control=%s meshsock workers=%s,%s", control.Name(), a.Name(), b.Name())

	aMesh := c.meshIP(a.Name())
	bMesh := c.meshIP(b.Name())
	if aMesh == "" || bMesh == "" {
		t.Fatalf("meshsock workers missing mesh IPs: a=%q b=%q", aMesh, bMesh)
	}

	// Sever the direct/punched UDP path between the two workers (leave each
	// worker↔control intact). meshsock must fall back to the relay.
	a.BlockMeshUDPTo(b.PublicIPv4())
	b.BlockMeshUDPTo(a.PublicIPv4())
	t.Logf("cloud: blocked direct worker↔worker UDP; only the relay remains")

	assertMeshReachable(t, b, aMesh, 90*time.Second)
	assertMeshReachable(t, a, bMesh, 90*time.Second)
	t.Logf("cloud: T-58 — worker↔worker connectivity survives via the relay")
}

// meshIP returns a node's mesh IP from the cluster API.
func (c *Cluster) meshIP(name string) string {
	for _, n := range c.Nodes() {
		if n.GetName() == name {
			return n.GetMeshIp()
		}
	}
	return ""
}

// assertMeshReachable pings meshIP from `from` until it succeeds, or fails.
func assertMeshReachable(t *testing.T, from *Node, meshIP string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		out, err := from.Run(fmt.Sprintf("ping -c 2 -W 3 %s", meshIP))
		if err == nil && strings.Contains(out, " 0% packet loss") {
			return
		}
		last = out
		time.Sleep(5 * time.Second)
	}
	t.Fatalf("cloud: %s could not reach mesh IP %s within %s via the relay\nlast ping:\n%s", from.Name(), meshIP, timeout, last)
}
