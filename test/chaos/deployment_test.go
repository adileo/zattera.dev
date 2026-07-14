//go:build chaos

package chaos

import (
	"testing"
	"time"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

// TestDeploymentFailover kills the leader at each deployment phase and asserts
// the deployment still converges to a consistent terminal state.
func TestDeploymentFailover(t *testing.T) {
	phases := []zatterav1.DeploymentPhase{
		zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PENDING,
		zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PLACING,
		zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_STARTING,
		zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_HEALTHCHECKING,
		zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PROMOTING,
		zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_DRAINING_OLD,
	}

	for _, phase := range phases {
		phase := phase
		t.Run(phase.String(), func(t *testing.T) {
			h := New(t)

			// Gate the agent so the deployment pauses at (or just before) the
			// target phase, giving us a window to kill the leader there.
			switch phase {
			case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_STARTING,
				zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PENDING,
				zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PLACING:
				// leave green unobserved → pauses at STARTING
			case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_HEALTHCHECKING:
				h.allowRunning() // green RUNNING → pauses at HEALTHCHECKING
			default:
				h.allowHealthy() // fly to promotion/drain
			}

			depID := h.Deploy(t)
			h.waitPhaseAtLeast(t, depID, phase, 30*time.Second)

			// Kill the leader mid-flight; a new one must resume the deployment.
			h.C.KillLeader()
			h.C.WaitLeader(15 * time.Second)

			// Let it finish and assert it reaches SUCCEEDED consistently.
			h.allowHealthy()
			h.waitPhase(t, depID, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_SUCCEEDED, 60*time.Second)

			h.checkInvariants(t)
			h.checkDeploymentConsistent(t, depID)
		})
	}
}
