package raftstore

import (
	"context"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/zattera-dev/zattera/internal/state"
)

// NewTestStore spins up a single-node in-memory raft store and waits for
// leadership. The returned Store is ready for Apply.
func NewTestStore(t *testing.T) *Store {
	t.Helper()
	return NewTestNode(t, "node-test-1", true, nil)
}

// NewTestNode creates one in-memory raft node with a loopback transport.
// Wire multiple nodes together by connecting their transports before
// bootstrap (see testutil/simcluster).
func NewTestNode(t *testing.T, nodeID string, bootstrap bool, transport raft.Transport) *Store {
	t.Helper()
	if transport == nil {
		_, transport = raft.NewInmemTransport(raft.ServerAddress(nodeID))
	}
	st, err := New(Config{
		NodeID:    nodeID,
		Inmem:     true,
		Bootstrap: bootstrap,
		Transport: transport,
	}, state.New())
	if err != nil {
		t.Fatalf("raftstore.New: %v", err)
	}
	t.Cleanup(func() { _ = st.Shutdown() })
	if bootstrap {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := st.WaitForLeader(ctx); err != nil {
			t.Fatalf("wait for leader: %v", err)
		}
		// Leadership is known; wait until *this* node can accept applies.
		deadline := time.Now().Add(10 * time.Second)
		for !st.IsLeader() {
			if time.Now().After(deadline) {
				t.Fatal("node never became leader")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	return st
}
