//go:build cloud

package cloud

import (
	"fmt"
	"strings"
	"testing"
	"time"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

// TestMeshsockRelay spins up a REAL topology where two workers have NO public
// IPv4 and sit behind the same NAT gateway, both on the meshsock datapath. They
// cannot reach each other directly (no public endpoint, symmetric-ish NAT), so
// their only worker↔worker path is the control-node DERP-lite relay (T-58).
// The test asserts that mesh IP → mesh IP connectivity between the two NAT'd
// workers works, which can only happen over the relay.
//
//	HCLOUD_TOKEN=... go test -tags cloud ./test/cloud/ -run TestMeshsockRelay -v -timeout 40m
//
// Note: the punched-UDP path (T-57) is exercised by the package integration
// tests over a NAT simulator; demonstrating it on real infra additionally needs
// reflexive-endpoint discovery (T-57b), so this cloud test targets the relay
// fallback, which needs no reflexive endpoints.
func TestMeshsockRelay(t *testing.T) {
	c := NewCluster(t)

	// Control (public) + a NAT gateway + two NAT'd meshsock workers behind it.
	control := c.StartControl("amd64", "")
	gateway := c.JoinWorker("amd64") // public node that also NATs the private net
	a := c.JoinMeshsockWorkerNAT(gateway, "amd64")
	b := c.JoinMeshsockWorkerNAT(gateway, "amd64")

	c.WaitNodesReady(4)
	t.Logf("cloud: control=%s gateway=%s meshsock workers=%s,%s", control.Name(), gateway.Name(), a.Name(), b.Name())

	aMesh := c.meshIP(a.Name())
	bMesh := c.meshIP(b.Name())
	if aMesh == "" || bMesh == "" {
		t.Fatalf("meshsock workers missing mesh IPs: a=%q b=%q", aMesh, bMesh)
	}

	// From B, reach A's mesh IP. Both are NAT'd with no public endpoint, so this
	// only succeeds once the meshsock bind has escalated to the relay path.
	assertMeshReachable(t, b, aMesh, 90*time.Second)
	assertMeshReachable(t, a, bMesh, 90*time.Second)
	t.Logf("cloud: T-58 — NAT'd worker↔worker connectivity works over the relay")
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

// assertMeshReachable pings `meshIP` from `from` until it succeeds, or fails.
// A NAT'd meshsock peer is only pingable once the relay path carries the WG
// tunnel (there is no direct/punched path in this topology).
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
	t.Fatalf("cloud: %s could not reach mesh IP %s within %s via the relay\nlast ping output:\n%s", from.Name(), meshIP, timeout, last)
}

var _ = zatterav1.NodeStatus_NODE_STATUS_ALIVE
