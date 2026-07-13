// Package livestate holds the leader's in-memory view of connected nodes:
// presence, last heartbeat time and the most recent live sample. It is the
// authoritative source for node liveness (T-21) and feeds autoscaling and
// scale-to-zero activation. It NEVER touches Raft — this data is ephemeral by
// design (heartbeats would bloat the log and every leader rebuilds it from the
// agent streams on election).
package livestate

import (
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// Registry tracks connected agents and their latest live samples. All methods
// are safe for concurrent use.
type Registry struct {
	clock clock.Clock
	mu    sync.Mutex
	nodes map[string]*node
}

type node struct {
	// gen is bumped on every Connect; a release only clears its own generation
	// so a reconnect that races the old stream's teardown cannot wipe fresh
	// presence.
	gen           uint64
	connected     bool
	lastHeartbeat time.Time
	heartbeat     *clusterv1.Heartbeat
}

// NodeState is a snapshot of one node's live view.
type NodeState struct {
	NodeID        string
	Connected     bool
	LastHeartbeat time.Time
	// Heartbeat is the most recent sample (cloned), or nil if none received.
	Heartbeat *clusterv1.Heartbeat
}

// New builds an empty registry. clk stamps heartbeat arrival times.
func New(clk clock.Clock) *Registry {
	if clk == nil {
		clk = clock.Real{}
	}
	return &Registry{clock: clk, nodes: map[string]*node{}}
}

// Connect marks nodeID's sync stream present and returns a release func to call
// when the stream ends. A newer Connect for the same node supersedes an older
// one: the stale stream's release then becomes a no-op.
func (r *Registry) Connect(nodeID string) (release func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := r.nodes[nodeID]
	if n == nil {
		n = &node{}
		r.nodes[nodeID] = n
	}
	n.gen++
	n.connected = true
	gen := n.gen
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		if cur := r.nodes[nodeID]; cur != nil && cur.gen == gen {
			cur.connected = false
		}
	}
}

// Heartbeat records a live sample for nodeID and stamps its arrival time.
func (r *Registry) Heartbeat(nodeID string, hb *clusterv1.Heartbeat) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := r.nodes[nodeID]
	if n == nil {
		n = &node{}
		r.nodes[nodeID] = n
	}
	n.lastHeartbeat = r.clock.Now()
	if hb != nil {
		n.heartbeat = proto.Clone(hb).(*clusterv1.Heartbeat)
	}
}

// Get returns a snapshot of one node's live state.
func (r *Registry) Get(nodeID string) (NodeState, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := r.nodes[nodeID]
	if n == nil {
		return NodeState{}, false
	}
	return r.snapshot(nodeID, n), true
}

// Snapshot returns the live state of every known node.
func (r *Registry) Snapshot() []NodeState {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]NodeState, 0, len(r.nodes))
	for id, n := range r.nodes {
		out = append(out, r.snapshot(id, n))
	}
	return out
}

func (r *Registry) snapshot(id string, n *node) NodeState {
	ns := NodeState{NodeID: id, Connected: n.connected, LastHeartbeat: n.lastHeartbeat}
	if n.heartbeat != nil {
		ns.Heartbeat = proto.Clone(n.heartbeat).(*clusterv1.Heartbeat)
	}
	return ns
}
