package api

import (
	"context"
	"strings"
	"testing"
	"time"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/livestate"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// eventsOfKind returns the recorded events with the given kind.
func eventsOfKind(rs *raftstore.Store, kind string) []*zatterav1.Event {
	var out []*zatterav1.Event
	for _, ev := range rs.State().ListEvents(0) {
		if ev.GetKind() == kind {
			out = append(out, ev)
		}
	}
	return out
}

// TestLivenessEmitsNodeDown covers the built-in node-down alert rule's feed
// (T-109): the event fires on the durable ALIVE→DOWN transition, exactly once,
// and not again while the node stays down.
func TestLivenessEmitsNodeDown(t *testing.T) {
	rs := raftstore.NewTestStore(t)
	clk := clock.NewFake()
	live := livestate.New(clk)
	rs.State().PutNode(aliveNode("local"))
	n1 := aliveNode("n1")
	n1.Name = "worker-1"
	rs.State().PutNode(n1)
	m := NewLivenessMonitor(rs.State(), rs, live, clk, "local", nil)
	m.leaderSince = clk.Now().Add(-time.Hour) // past grace

	m.evaluate(context.Background())
	got := eventsOfKind(rs, "node.down")
	if len(got) != 1 {
		t.Fatalf("node.down events = %d, want 1", len(got))
	}
	if got[0].GetNodeId() != "n1" {
		t.Errorf("node_id = %q, want n1", got[0].GetNodeId())
	}
	if got[0].GetSeverity() != "error" {
		t.Errorf("severity = %q, want error", got[0].GetSeverity())
	}
	// The human-facing name should be preferred over the raw id.
	if msg := got[0].GetMessage(); msg == "" || !strings.Contains(msg, "worker-1") {
		t.Errorf("message = %q, want it to name worker-1", msg)
	}

	// Staying down must not re-notify: the rule fires on transition, and the
	// engine's dedupe window is not a substitute for not spamming the log.
	for i := 0; i < 3; i++ {
		clk.Advance(livenessTick)
		m.evaluate(context.Background())
	}
	if n := len(eventsOfKind(rs, "node.down")); n != 1 {
		t.Fatalf("node.down events after sustained DOWN = %d, want 1", n)
	}
}

// TestLivenessRecoveryEmitsNothing guards the other direction: coming back
// ALIVE is a status change too, and must not emit a node.down.
func TestLivenessRecoveryEmitsNothing(t *testing.T) {
	rs := raftstore.NewTestStore(t)
	clk := clock.NewFake()
	live := livestate.New(clk)
	rs.State().PutNode(aliveNode("local"))
	rs.State().PutNode(aliveNode("n1"))
	m := NewLivenessMonitor(rs.State(), rs, live, clk, "local", nil)
	m.leaderSince = clk.Now().Add(-time.Hour)

	m.evaluate(context.Background()) // → DOWN (1 event)
	live.Heartbeat("n1", &clusterv1.Heartbeat{})
	m.evaluate(context.Background()) // → ALIVE

	if n := len(eventsOfKind(rs, "node.down")); n != 1 {
		t.Fatalf("node.down events = %d, want 1 (recovery must not emit)", n)
	}
}

// TestEmitEventBestEffort documents the contract: a nil applier or event is a
// no-op rather than a panic, because callers report failures that already
// happened and must not be broken by the reporting.
func TestEmitEventBestEffort(t *testing.T) {
	emitEvent(context.Background(), nil, nil, "system:test", &zatterav1.Event{Kind: "x"})
	rs := raftstore.NewTestStore(t)
	emitEvent(context.Background(), rs, nil, "system:test", nil)

	// A real emit fills Meta so the event is addressable.
	emitEvent(context.Background(), rs, nil, "system:test", &zatterav1.Event{Kind: "test.kind"})
	got := eventsOfKind(rs, "test.kind")
	if len(got) != 1 {
		t.Fatalf("events = %d, want 1", len(got))
	}
	if got[0].GetMeta().GetId() == "" {
		t.Error("emitted event has no Meta.Id")
	}
}
