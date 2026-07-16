// Package nodehealth holds the pure node-liveness decision types shared by the
// gossip failure detector (internal/daemon/mesh) and the liveness monitor
// (internal/daemon/api). It is a dependency-free leaf so both can import it
// without a cycle (the mesh package's tests import api, so api must not import
// mesh).
package nodehealth

import "time"

// GossipDownStaleThreshold is the flap guard: gossip may only push a node to
// DOWN once its heartbeat has ALSO been stale at least this long. It is well
// under the 30s heartbeat deadline, so gossip still accelerates detection, but a
// transient gossip false-positive alone cannot demote a still-heart-beating
// node.
const GossipDownStaleThreshold = 10 * time.Second

// GossipLiveness is the gossip layer's view of one node.
type GossipLiveness struct {
	Alive bool
	// Since is when this alive/dead state was first observed.
	Since time.Time
}

// Verdict is the durable status the combined signals imply for a node.
type Verdict int

const (
	// VerdictNoChange means the signals are inconclusive — leave the status as is.
	VerdictNoChange Verdict = iota
	VerdictAlive
	VerdictDown
)

// LivenessInputs combines the two detectors for one node.
type LivenessInputs struct {
	// GossipKnown is false when gossip has no opinion (single-node, gossip down,
	// or a node it has never seen).
	GossipKnown bool
	GossipAlive bool
	// HeartbeatReceived is whether livestate holds any heartbeat for the node;
	// HeartbeatStaleFor is the age of that heartbeat when it does.
	HeartbeatReceived bool
	HeartbeatStaleFor time.Duration
}

// Decide applies the flap guard (T-56) at the gossip time scale
// (GossipDownStaleThreshold, ~10s — well under the 30s heartbeat deadline):
//
//   - ALIVE if EITHER signal is fresh — a heartbeat within the threshold, or
//     gossip reporting the node up.
//   - DOWN only when gossip reports the node dead AND its heartbeat is stale
//     past the threshold (or never arrived). This is what lets gossip demote a
//     failed node in seconds instead of waiting the full heartbeat deadline.
//   - NoChange otherwise — notably when gossip has no opinion and the heartbeat
//     is merely stale: the caller falls back to the heartbeat-only deadline.
//
// Because DOWN needs both detectors to agree, neither can flap a node alone.
func Decide(in LivenessInputs) Verdict {
	heartbeatFresh := in.HeartbeatReceived && in.HeartbeatStaleFor <= GossipDownStaleThreshold
	if heartbeatFresh || (in.GossipKnown && in.GossipAlive) {
		return VerdictAlive
	}
	if in.GossipKnown && !in.GossipAlive && (!in.HeartbeatReceived || in.HeartbeatStaleFor > GossipDownStaleThreshold) {
		return VerdictDown
	}
	return VerdictNoChange
}
