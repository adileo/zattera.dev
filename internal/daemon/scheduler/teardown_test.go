package scheduler

import (
	"testing"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

func TestTeardown(t *testing.T) {
	t.Run("deleting an env stops then removes its assignments over two passes", func(t *testing.T) {
		s, rs := newSched(t)
		st := rs.State()
		addNodes(st, "n1", "n2")
		addEnvRelease(st, 2) // env "env1" + rel "rel1"
		mustEval(t, s)       // scheduler places 2 RUN replicas

		if got := len(runningByNode(st)); got != 2 {
			t.Fatalf("precondition: want 2 running, got %d", got)
		}

		// The env is deleted (API cascade); its assignments are now orphans.
		st.DeleteEnvironment(envID)

		// Pass 1: RUN orphans flip to STOP.
		mustEval(t, s)
		stopping, running := 0, 0
		for _, a := range st.ListAssignments(envID) {
			switch a.GetDesired() {
			case zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_STOP:
				stopping++
			case zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN:
				running++
			}
		}
		if running != 0 || stopping != 2 {
			t.Fatalf("after pass 1 want 0 RUN / 2 STOP, got %d/%d", running, stopping)
		}

		// The agents report the containers stopped.
		for _, a := range st.ListAssignments(envID) {
			st.SetAssignmentObserved(a.GetNodeId(), map[string]*zatterav1.AssignmentObserved{
				a.GetMeta().GetId(): {State: zatterav1.InstanceState_INSTANCE_STATE_STOPPED},
			})
		}

		// Pass 2: STOPPED orphans are deleted.
		mustEval(t, s)
		if got := len(st.ListAssignments(envID)); got != 0 {
			t.Fatalf("after pass 2 all orphan assignments should be gone, got %d", got)
		}
	})

	t.Run("an assignment whose release vanished is also an orphan", func(t *testing.T) {
		s, rs := newSched(t)
		st := rs.State()
		addNodes(st, "n1")
		// Env exists but references a release that never existed.
		st.PutEnvironment(&zatterav1.Environment{Meta: &zatterav1.Meta{Id: envID}})
		st.PutAssignment(&zatterav1.Assignment{
			Meta: &zatterav1.Meta{Id: "orphan1"}, NodeId: "n1",
			EnvironmentId: envID, ReleaseId: "ghost-release",
			Desired: zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN,
		})

		mustEval(t, s)
		a, _ := st.Assignment("orphan1")
		if a.GetDesired() != zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_STOP {
			t.Fatalf("orphan (missing release) should be flipped to STOP, got %v", a.GetDesired())
		}
	})
}
