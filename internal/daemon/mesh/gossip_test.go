package mesh

import (
	"testing"
	"time"

	"github.com/hashicorp/memberlist"

	"github.com/zattera-dev/zattera/internal/daemon/nodehealth"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// TestGossip covers the memberlist event → liveness mapping (with faked events,
// no real network) and the Decide flap guard.
func TestGossip(t *testing.T) {
	t.Run("events map to per-node liveness", testGossipEvents)
	t.Run("flap guard combines gossip + heartbeat", testGossipFlapGuard)
	t.Run("secret is a stable 32-byte key from the CA hash", testGossipSecret)
}

// testGossipEvents drives the event delegate with fake memberlist nodes and
// asserts the tracker snapshot, including that a node never tracks itself.
func testGossipEvents(t *testing.T) {
	clk := clock.NewFake()
	tr := newTracker(clk)
	d := &eventDelegate{self: "self", tracker: tr}

	d.NotifyJoin(&memberlist.Node{Name: "n1"})
	d.NotifyJoin(&memberlist.Node{Name: "n2"})
	d.NotifyJoin(&memberlist.Node{Name: "self"}) // ignored: never judge ourselves

	snap := tr.snapshot()
	if _, ok := snap["self"]; ok {
		t.Fatal("gossip tracked itself")
	}
	if !snap["n1"].Alive || !snap["n2"].Alive {
		t.Fatalf("expected n1,n2 alive: %+v", snap)
	}
	aliveSince := snap["n1"].Since

	// A leave flips n1 to dead and stamps a new transition time.
	clk.Advance(5 * time.Second)
	d.NotifyLeave(&memberlist.Node{Name: "n1"})
	snap = tr.snapshot()
	if snap["n1"].Alive {
		t.Fatalf("n1 should be dead after leave: %+v", snap["n1"])
	}
	if !snap["n1"].Since.After(aliveSince) {
		t.Fatalf("dead transition time not advanced: alive=%v dead=%v", aliveSince, snap["n1"].Since)
	}

	// A repeated leave is idempotent — Since must not move on a no-op.
	deadSince := snap["n1"].Since
	clk.Advance(5 * time.Second)
	d.NotifyLeave(&memberlist.Node{Name: "n1"})
	if got := tr.snapshot()["n1"].Since; !got.Equal(deadSince) {
		t.Fatalf("redundant leave moved Since: %v -> %v", deadSince, got)
	}

	// Rejoin flips it back to alive.
	d.NotifyJoin(&memberlist.Node{Name: "n1"})
	if !tr.snapshot()["n1"].Alive {
		t.Fatal("n1 should be alive after rejoin")
	}
}

// testGossipFlapGuard checks the Decide truth table: neither detector can flap a
// node on its own; DOWN needs gossip-dead AND a sufficiently stale heartbeat.
func testGossipFlapGuard(t *testing.T) {
	cases := []struct {
		name string
		in   nodehealth.LivenessInputs
		want nodehealth.Verdict
	}{
		{"heartbeat fresh alone → alive", nodehealth.LivenessInputs{HeartbeatReceived: true, HeartbeatStaleFor: 3 * time.Second}, nodehealth.VerdictAlive},
		{"gossip alive alone → alive", nodehealth.LivenessInputs{GossipKnown: true, GossipAlive: true}, nodehealth.VerdictAlive},
		{"fresh heartbeat overrides gossip-dead → alive", nodehealth.LivenessInputs{GossipKnown: true, GossipAlive: false, HeartbeatReceived: true, HeartbeatStaleFor: 3 * time.Second}, nodehealth.VerdictAlive},
		{"gossip-dead + heartbeat stale >10s → down (accelerated)", nodehealth.LivenessInputs{GossipKnown: true, GossipAlive: false, HeartbeatReceived: true, HeartbeatStaleFor: 11 * time.Second}, nodehealth.VerdictDown},
		{"gossip-dead + never heard a heartbeat → down", nodehealth.LivenessInputs{GossipKnown: true, GossipAlive: false, HeartbeatReceived: false}, nodehealth.VerdictDown},
		{"gossip-dead but heartbeat only 5s stale → alive (too fresh to demote)", nodehealth.LivenessInputs{GossipKnown: true, GossipAlive: false, HeartbeatReceived: true, HeartbeatStaleFor: 5 * time.Second}, nodehealth.VerdictAlive},
		{"heartbeat stale but gossip unknown → no change (defer to baseline)", nodehealth.LivenessInputs{GossipKnown: false, HeartbeatReceived: true, HeartbeatStaleFor: 60 * time.Second}, nodehealth.VerdictNoChange},
		{"nothing known → no change", nodehealth.LivenessInputs{}, nodehealth.VerdictNoChange},
	}
	for _, tc := range cases {
		if got := nodehealth.Decide(tc.in); got != tc.want {
			t.Errorf("%s: Decide = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func testGossipSecret(t *testing.T) {
	caHash := []byte("cluster-ca-hash")
	a := gossipSecret(caHash)
	b := gossipSecret(caHash)
	if len(a) != 32 {
		t.Fatalf("gossip secret len = %d, want 32 (AES-256)", len(a))
	}
	if string(a) != string(b) {
		t.Fatal("gossip secret is not deterministic for the same CA hash")
	}
	if string(a) == string(gossipSecret([]byte("other-ca"))) {
		t.Fatal("gossip secret does not depend on the CA hash")
	}
}
