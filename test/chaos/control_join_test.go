//go:build chaos

package chaos

import (
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/zattera-dev/zattera/internal/daemon/ca"
)

// TestControlJoin validates the T-55b property: control nodes that join the
// quorum DYNAMICALLY (via AddVoter, as the enrollment path does — not via
// bootstrap) are first-class voters. They replicate, they count toward quorum,
// and one of them can take over when the original bootstrap leader is killed.
func TestControlJoin(t *testing.T) {
	authority, err := ca.LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatalf("ca: %v", err)
	}
	a1, a2, a3 := freePort(t), freePort(t), freePort(t)

	// n1 bootstraps alone, then n2 and n3 come up empty (Bootstrap=false) and are
	// enrolled dynamically — exactly what a control-node join does once the new
	// node's raft transport is reachable.
	n1 := newHANode(t, authority, "n1", a1, []raft.Server{{ID: "n1", Address: raft.ServerAddress(a1)}})
	n2 := newHANode(t, authority, "n2", a2, nil)
	n3 := newHANode(t, authority, "n3", a3, nil)

	leader := leaderOf(t, 10*time.Second, n1)
	if err := leader.store.AddVoter("n2", a2); err != nil {
		t.Fatalf("enroll n2: %v", err)
	}
	if err := leader.store.AddVoter("n3", a3); err != nil {
		t.Fatalf("enroll n3: %v", err)
	}
	if got := configSize(t, leader); got != 3 {
		t.Fatalf("config size = %d, want 3", got)
	}

	// A pre-failover write replicates to the dynamically-joined nodes.
	putKV(t, leader, "seed", "1")
	waitKV(t, n2, "seed", "1")
	waitKV(t, n3, "seed", "1")

	// Kill the original bootstrap leader (n1). The two dynamically-joined nodes
	// must elect a new leader among THEMSELVES and keep accepting writes — proving
	// they are full voters, not read-only learners.
	n1.kill()
	joined := []*haNode{n2, n3}
	newLeader := leaderOf(t, 10*time.Second, joined...)
	if newLeader.id != "n2" && newLeader.id != "n3" {
		t.Fatalf("new leader %q is not one of the joined nodes", newLeader.id)
	}
	putKV(t, newLeader, "after-failover", "2")
	for _, n := range joined {
		if n != newLeader {
			waitKV(t, n, "after-failover", "2")
		}
	}
}
