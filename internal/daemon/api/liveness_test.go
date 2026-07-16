package api

import (
	"context"
	"testing"
	"time"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/livestate"
	"github.com/zattera-dev/zattera/internal/daemon/nodehealth"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// fakeGossip is a static gossip snapshot for liveness tests.
type fakeGossip map[string]nodehealth.GossipLiveness

func (f fakeGossip) Snapshot() map[string]nodehealth.GossipLiveness { return f }

func TestLiveness(t *testing.T) {
	newMon := func(t *testing.T) (*LivenessMonitor, *raftstore.Store, *livestate.Registry, *clock.Fake) {
		rs := raftstore.NewTestStore(t)
		clk := clock.NewFake()
		live := livestate.New(clk)
		rs.State().PutNode(aliveNode("local"))
		rs.State().PutNode(aliveNode("n1"))
		return NewLivenessMonitor(rs.State(), rs, live, clk, "local", nil), rs, live, clk
	}

	status := func(rs *raftstore.Store, id string) zatterav1.NodeStatus {
		n, _ := rs.State().Node(id)
		return n.GetStatus()
	}

	t.Run("stale heartbeat marks a node DOWN", func(t *testing.T) {
		m, rs, _, _ := newMon(t)
		m.leaderSince = m.clock.Now().Add(-time.Hour) // past grace
		m.evaluate(context.Background())
		if status(rs, "n1") != zatterav1.NodeStatus_NODE_STATUS_DOWN {
			t.Fatalf("n1 should be DOWN, got %v", status(rs, "n1"))
		}
	})

	t.Run("fresh heartbeat recovers a DOWN node to ALIVE", func(t *testing.T) {
		m, rs, live, clk := newMon(t)
		m.leaderSince = clk.Now().Add(-time.Hour)
		m.evaluate(context.Background()) // → DOWN
		if status(rs, "n1") != zatterav1.NodeStatus_NODE_STATUS_DOWN {
			t.Fatalf("precondition: n1 should be DOWN, got %v", status(rs, "n1"))
		}
		live.Heartbeat("n1", &clusterv1.Heartbeat{})
		m.evaluate(context.Background())
		if status(rs, "n1") != zatterav1.NodeStatus_NODE_STATUS_ALIVE {
			t.Fatalf("n1 should recover to ALIVE, got %v", status(rs, "n1"))
		}
	})

	t.Run("leader grace window defers DOWN", func(t *testing.T) {
		m, rs, _, clk := newMon(t)
		m.leaderSince = clk.Now() // just acquired leadership
		m.evaluate(context.Background())
		if status(rs, "n1") != zatterav1.NodeStatus_NODE_STATUS_ALIVE {
			t.Fatalf("within grace, n1 should stay ALIVE, got %v", status(rs, "n1"))
		}
		clk.Advance(leaderGracePeriod + time.Second) // past grace
		m.evaluate(context.Background())
		if status(rs, "n1") != zatterav1.NodeStatus_NODE_STATUS_DOWN {
			t.Fatalf("after grace with no heartbeat, n1 should be DOWN, got %v", status(rs, "n1"))
		}
	})

	t.Run("never marks the local node DOWN", func(t *testing.T) {
		m, rs, _, clk := newMon(t)
		m.leaderSince = clk.Now().Add(-time.Hour)
		m.evaluate(context.Background())
		if status(rs, "local") != zatterav1.NodeStatus_NODE_STATUS_ALIVE {
			t.Fatalf("local node must never be demoted, got %v", status(rs, "local"))
		}
	})

	t.Run("stale heartbeat past the deadline demotes even after prior freshness", func(t *testing.T) {
		m, rs, live, clk := newMon(t)
		m.leaderSince = clk.Now().Add(-time.Hour)
		live.Heartbeat("n1", &clusterv1.Heartbeat{})
		m.evaluate(context.Background())
		if status(rs, "n1") != zatterav1.NodeStatus_NODE_STATUS_ALIVE {
			t.Fatalf("fresh n1 should be ALIVE, got %v", status(rs, "n1"))
		}
		clk.Advance(heartbeatDeadline + time.Second) // heartbeat now stale
		m.evaluate(context.Background())
		if status(rs, "n1") != zatterav1.NodeStatus_NODE_STATUS_DOWN {
			t.Fatalf("stale n1 should be DOWN, got %v", status(rs, "n1"))
		}
	})

	// --- gossip-accelerated detection (T-56) ---------------------------------

	t.Run("gossip-confirmed death demotes before the heartbeat deadline", func(t *testing.T) {
		m, rs, live, clk := newMon(t)
		m.leaderSince = clk.Now().Add(-time.Hour)
		live.Heartbeat("n1", &clusterv1.Heartbeat{})
		clk.Advance(15 * time.Second) // stale >10s but WELL within the 30s deadline
		m.WithGossip(fakeGossip{"n1": {Alive: false}})
		m.evaluate(context.Background())
		if status(rs, "n1") != zatterav1.NodeStatus_NODE_STATUS_DOWN {
			t.Fatalf("gossip-dead n1 should be DOWN before the deadline, got %v", status(rs, "n1"))
		}
	})

	t.Run("fresh heartbeat overrides a gossip false-positive (flap guard)", func(t *testing.T) {
		m, rs, live, clk := newMon(t)
		m.leaderSince = clk.Now().Add(-time.Hour)
		live.Heartbeat("n1", &clusterv1.Heartbeat{}) // stale 0s — too fresh to demote
		m.WithGossip(fakeGossip{"n1": {Alive: false}})
		m.evaluate(context.Background())
		if status(rs, "n1") != zatterav1.NodeStatus_NODE_STATUS_ALIVE {
			t.Fatalf("a very recent heartbeat must keep n1 ALIVE despite gossip, got %v", status(rs, "n1"))
		}
	})

	t.Run("gossip keeps a node ALIVE past the heartbeat deadline", func(t *testing.T) {
		m, rs, live, clk := newMon(t)
		m.leaderSince = clk.Now().Add(-time.Hour)
		live.Heartbeat("n1", &clusterv1.Heartbeat{})
		clk.Advance(heartbeatDeadline + 10*time.Second) // heartbeat stale past 30s
		m.WithGossip(fakeGossip{"n1": {Alive: true}})
		m.evaluate(context.Background())
		if status(rs, "n1") != zatterav1.NodeStatus_NODE_STATUS_ALIVE {
			t.Fatalf("gossip-alive should keep n1 ALIVE past the heartbeat deadline, got %v", status(rs, "n1"))
		}
	})

	t.Run("gossip-confirmed death demotes even during the grace window", func(t *testing.T) {
		m, rs, _, clk := newMon(t)
		m.leaderSince = clk.Now() // just elected → in grace
		m.WithGossip(fakeGossip{"n1": {Alive: false}})
		m.evaluate(context.Background())
		if status(rs, "n1") != zatterav1.NodeStatus_NODE_STATUS_DOWN {
			t.Fatalf("gossip death should bypass grace, got %v", status(rs, "n1"))
		}
	})
}

func aliveNode(id string) *zatterav1.Node {
	return &zatterav1.Node{
		Meta:   &zatterav1.Meta{Id: id},
		Status: zatterav1.NodeStatus_NODE_STATUS_ALIVE,
	}
}
