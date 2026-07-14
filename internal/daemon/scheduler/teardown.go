package scheduler

import (
	"context"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/state"
)

// reconcileOrphans stops and reaps assignments whose environment or release no
// longer exists — the convergence half of app/project/env deletion (the API
// deletes the desired-state objects, the scheduler drains the containers).
//
// Two-step, mirroring scale-down so containers stop gracefully: a RUN orphan is
// flipped to STOP, and a STOP orphan the agent has reported STOPPED is deleted.
// A later evaluation collects whatever a single pass could not.
func (s *Scheduler) reconcileOrphans(ctx context.Context, st *state.Store) error {
	var stopIDs, deleteIDs []string
	for _, a := range st.ListAssignments("") {
		if !isOrphan(st, a) {
			continue
		}
		switch a.GetDesired() {
		case zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN:
			stopIDs = append(stopIDs, a.GetMeta().GetId())
		case zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_STOP:
			if isStopped(a) {
				deleteIDs = append(deleteIDs, a.GetMeta().GetId())
			}
		}
	}
	if len(stopIDs) == 0 && len(deleteIDs) == 0 {
		return nil
	}
	return s.applyBatch(ctx, "orphans", nil, stopIDs, deleteIDs)
}

// isOrphan reports whether an assignment's environment or referenced release is
// gone.
func isOrphan(st *state.Store, a *zatterav1.Assignment) bool {
	if _, ok := st.Environment(a.GetEnvironmentId()); !ok {
		return true
	}
	if relID := a.GetReleaseId(); relID != "" {
		if _, ok := st.Release(relID); !ok {
			return true
		}
	}
	return false
}
