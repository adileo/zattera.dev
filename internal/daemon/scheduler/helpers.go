package scheduler

import (
	"fmt"

	"google.golang.org/protobuf/types/known/timestamppb"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

// desiredReplicas is the target replica count for an env: the autoscaler's
// effective count when set, else replicas.min. 0 when there is no active
// release (nothing to run).
func desiredReplicas(env *zatterav1.Environment) int {
	if env.GetActiveReleaseId() == "" {
		return 0
	}
	if r := env.GetEffectiveReplicas(); r > 0 {
		return int(r)
	}
	return int(env.GetService().GetReplicas().GetMin())
}

// newAssignment builds a desired-RUN assignment for a replica of rel on nodeID.
func newAssignment(env *zatterav1.Environment, rel *zatterav1.Release, nodeID string) *zatterav1.Assignment {
	now := timestamppb.Now()
	return &zatterav1.Assignment{
		Meta:          &zatterav1.Meta{Id: ids.New(), CreatedAt: now, UpdatedAt: now},
		NodeId:        nodeID,
		ProjectId:     env.GetProjectId(),
		AppId:         env.GetAppId(),
		EnvironmentId: env.GetMeta().GetId(),
		ReleaseId:     rel.GetMeta().GetId(),
		Desired:       zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN,
		ConfigHash:    rel.GetConfigHash(),
	}
}

// nodeDown reports whether a node is unknown or not ALIVE.
func nodeDown(st *state.Store, nodeID string) bool {
	n, ok := st.Node(nodeID)
	return !ok || n.GetStatus() != zatterav1.NodeStatus_NODE_STATUS_ALIVE
}

func isStopped(a *zatterav1.Assignment) bool {
	return a.GetObserved().GetState() == zatterav1.InstanceState_INSTANCE_STATE_STOPPED
}

func isStateful(rel *zatterav1.Release) bool {
	return rel.GetService().GetStateful()
}

// labelsMatch reports whether node labels satisfy all placement constraints.
func labelsMatch(nodeLabels, constraints map[string]string) bool {
	for k, v := range constraints {
		if nodeLabels[k] != v {
			return false
		}
	}
	return true
}

// isTerminalPhase reports whether a deployment phase is finished, so the
// scheduler may resume ownership of the env. T-26 refines which live phases own
// placement.
func isTerminalPhase(p zatterav1.DeploymentPhase) bool {
	switch p {
	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_SUCCEEDED,
		zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_FAILED,
		zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_ROLLED_BACK,
		zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_SUPERSEDED,
		zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_UNSPECIFIED:
		return true
	default:
		return false
	}
}

func sprintf(format string, args ...any) string { return fmt.Sprintf(format, args...) }
