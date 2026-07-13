package livestate

import (
	"testing"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

func TestRegistryConnectAndHeartbeat(t *testing.T) {
	clk := clock.NewFake()
	r := New(clk)

	if _, ok := r.Get("n1"); ok {
		t.Fatal("unknown node should not be present")
	}

	release := r.Connect("n1")
	ns, ok := r.Get("n1")
	if !ok || !ns.Connected {
		t.Fatalf("node should be connected: %+v ok=%v", ns, ok)
	}
	if ns.Heartbeat != nil {
		t.Fatal("no heartbeat yet")
	}

	clk.Advance(3)
	r.Heartbeat("n1", &clusterv1.Heartbeat{CpuPercent: 42})
	ns, _ = r.Get("n1")
	if ns.Heartbeat.GetCpuPercent() != 42 {
		t.Fatalf("heartbeat not recorded: %+v", ns.Heartbeat)
	}
	if !ns.LastHeartbeat.Equal(clk.Now()) {
		t.Fatalf("last heartbeat time = %v, want %v", ns.LastHeartbeat, clk.Now())
	}

	release()
	ns, _ = r.Get("n1")
	if ns.Connected {
		t.Fatal("node should be disconnected after release")
	}
	// Sample survives disconnect (liveness loop still reads the last value).
	if ns.Heartbeat.GetCpuPercent() != 42 {
		t.Fatal("heartbeat should survive disconnect")
	}
}

// A reconnect that races the old stream's release must keep the node present:
// the stale release targets an older generation and is a no-op.
func TestRegistryReconnectSupersedesStaleRelease(t *testing.T) {
	r := New(clock.NewFake())

	releaseOld := r.Connect("n1")
	releaseNew := r.Connect("n1")

	releaseOld() // stale teardown
	if ns, _ := r.Get("n1"); !ns.Connected {
		t.Fatal("fresh connection must survive a stale release")
	}

	releaseNew()
	if ns, _ := r.Get("n1"); ns.Connected {
		t.Fatal("current release should disconnect")
	}
}

func TestRegistrySnapshotClones(t *testing.T) {
	r := New(clock.NewFake())
	r.Connect("n1")
	r.Heartbeat("n1", &clusterv1.Heartbeat{CpuPercent: 10})

	ns, _ := r.Get("n1")
	ns.Heartbeat.CpuPercent = 99 // mutate the returned copy

	again, _ := r.Get("n1")
	if again.Heartbeat.GetCpuPercent() != 10 {
		t.Fatal("snapshot must be a defensive clone")
	}
	if len(r.Snapshot()) != 1 {
		t.Fatal("snapshot should list the node")
	}
}
