//go:build cloud

package cloud

import (
	"testing"
)

// TestControlNodeWorkloads proves T-55d on real infra: workloads scheduled onto a
// FOLLOWER control node become visible to the leader.
//
// Three control+worker nodes form a quorum; the bootstrap leads, the two joined
// nodes are followers. Deploying a 3-replica app spreads a replica onto the
// follower control nodes, and each replica only counts as HEALTHY once the agent
// on its node reports observed state to the LEADER's (leader-memory) livestate. A
// follower control node's own agent runs over loopback to its OWN API, so without
// T-55d (dial the leader + the SyncServer leader guard) a follower's replica would
// never report healthy. The test asserts a healthy replica lands on a follower.
//
//	HCLOUD_TOKEN=... go test -tags cloud ./test/cloud/ -run TestControlNodeWorkloads -v -timeout 60m
func TestControlNodeWorkloads(t *testing.T) {
	c := NewCluster(t)

	c.StartControl("amd64", "")
	f1 := c.JoinControl("amd64")
	f2 := c.JoinControl("amd64")
	c.WaitNodesReady(3)
	c.TrustRegistryCA()

	appDir := prepareHelloFixture(t, 3)
	_, nodes := c.DeploySourceHealthy("controlapps", appDir, 3, 3)

	// The healthy-replica node set is keyed by node id; map the follower handles.
	f1ID := c.NodeByName(f1.Name()).GetMeta().GetId()
	f2ID := c.NodeByName(f2.Name()).GetMeta().GetId()
	if !nodes[f1ID] && !nodes[f2ID] {
		t.Errorf("cloud: no healthy replica on a follower control node (%s/%s) — a follower's observed state never reached the leader (T-55d). healthy nodes=%v", f1.Name(), f2.Name(), nodes)
	}
	t.Logf("cloud: T-55d — healthy replicas across %d control node(s) incl. a follower", len(nodes))
}
