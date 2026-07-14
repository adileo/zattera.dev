//go:build chaos

package chaos

import (
	"testing"
	"time"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

// TestSchedulerInvariants exercises the scheduler/orchestrator under a network
// partition and a worker-node loss, checking the safety invariants hold.
func TestSchedulerInvariants(t *testing.T) {
	t.Run("minority partition during PLACING still converges", func(t *testing.T) {
		h := New(t)
		h.allowHealthy()
		depID := h.Deploy(t)
		h.waitPhaseAtLeast(t, depID, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PLACING, 30*time.Second)

		// Isolate the current leader (minority of 1); the other two re-elect.
		leader := h.C.Leader()
		if leader == nil {
			t.Fatal("no leader to partition")
		}
		others := otherIDs(h, leader.ID)
		h.C.Partition([]string{leader.ID}, others)
		h.C.WaitLeader(15 * time.Second)

		// The new leader must drive the deployment to completion.
		h.waitPhase(t, depID, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_SUCCEEDED, 60*time.Second)

		h.C.Heal()
		h.C.WaitLeader(15 * time.Second)
		h.checkInvariants(t)
		h.checkDeploymentConsistent(t, depID)
	})

	t.Run("losing a worker mid-deploy re-places its replica", func(t *testing.T) {
		h := New(t)
		h.allowRunning() // green RUNNING; pause at HEALTHCHECKING
		depID := h.Deploy(t)
		h.waitPhaseAtLeast(t, depID, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_HEALTHCHECKING, 30*time.Second)

		// Pick a worker that holds a green replica and mark it DOWN.
		victim := greenNode(h, depID)
		if victim == "" {
			t.Fatal("no green replica placed yet")
		}
		if err := h.C.Apply(withMeta(&clusterv1.Command{Mutation: &clusterv1.Command_SetNodeStatus{SetNodeStatus: &clusterv1.SetNodeStatus{
			NodeId: victim, Status: zatterav1.NodeStatus_NODE_STATUS_DOWN,
		}}})); err != nil {
			t.Fatalf("mark node down: %v", err)
		}

		// The scheduler replaces the lost replica; the deploy still promotes.
		h.allowHealthy()
		h.waitPhase(t, depID, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_SUCCEEDED, 60*time.Second)

		// No RUN replica remains on the dead node.
		st := h.leaderState()
		for _, a := range st.ListAssignments(cEnvID) {
			if a.GetNodeId() == victim && a.GetDesired() == zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN {
				t.Fatalf("a RUN replica still sits on the dead node %s", victim)
			}
		}
		h.checkInvariants(t)
		h.checkDeploymentConsistent(t, depID)
	})
}

func otherIDs(h *Harness, exclude string) []string {
	var ids []string
	for _, n := range h.C.Nodes {
		if n.ID != exclude {
			ids = append(ids, n.ID)
		}
	}
	return ids
}

// greenNode returns a node id holding a green replica of the deployment.
func greenNode(h *Harness, depID string) string {
	st := h.leaderState()
	if st == nil {
		return ""
	}
	for _, a := range st.ListAssignments(cEnvID) {
		if a.GetDeploymentId() == depID && a.GetDesired() == zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN {
			return a.GetNodeId()
		}
	}
	return ""
}
