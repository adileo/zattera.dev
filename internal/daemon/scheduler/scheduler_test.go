package scheduler

import (
	"context"
	"testing"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/state"
)

const (
	envID = "env1"
	relID = "rel1"
)

func TestEvaluate(t *testing.T) {
	t.Run("scale up 0 to 3 spreads one replica per node", func(t *testing.T) {
		s, rs := newSched(t)
		st := rs.State()
		addNodes(st, "n1", "n2", "n3")
		addEnvRelease(st, 3)

		mustEval(t, s)

		run := runningByNode(st)
		if len(run) != 3 {
			t.Fatalf("want 3 running replicas, got %d: %v", len(run), run)
		}
		for _, n := range []string{"n1", "n2", "n3"} {
			if run[n] != 1 {
				t.Fatalf("node %s should have exactly 1 replica, got %d", n, run[n])
			}
		}
	})

	t.Run("scale down 3 to 1 stops the excess", func(t *testing.T) {
		s, rs := newSched(t)
		st := rs.State()
		addNodes(st, "n1", "n2", "n3")
		addEnvRelease(st, 3)
		mustEval(t, s) // → 3 running
		setReplicas(st, 1)

		mustEval(t, s)

		running, stopping := 0, 0
		for _, a := range st.ListAssignments(envID) {
			switch a.GetDesired() {
			case zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN:
				running++
			case zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_STOP:
				stopping++
			}
		}
		if running != 1 || stopping != 2 {
			t.Fatalf("want 1 RUN + 2 STOP, got %d RUN / %d STOP", running, stopping)
		}
	})

	t.Run("a DOWN node's stateless replica is replaced on a live node", func(t *testing.T) {
		s, rs := newSched(t)
		st := rs.State()
		addNodes(st, "n1", "n2", "n3")
		addEnvRelease(st, 3)
		mustEval(t, s) // spread across n1,n2,n3

		// n3 goes DOWN.
		down, _ := st.Node("n3")
		down.Status = zatterav1.NodeStatus_NODE_STATUS_DOWN
		st.PutNode(down)

		mustEval(t, s)

		run := runningByNode(st)
		total := 0
		for node, c := range run {
			total += c
			if node == "n3" {
				t.Fatalf("no replica should run on the DOWN node n3")
			}
		}
		if total != 3 {
			t.Fatalf("want 3 running replicas after replacement, got %d: %v", total, run)
		}
	})

	t.Run("repeated evaluation does not double-place", func(t *testing.T) {
		s, rs := newSched(t)
		st := rs.State()
		addNodes(st, "n1", "n2", "n3")
		addEnvRelease(st, 3)

		mustEval(t, s)
		first := len(st.ListAssignments(envID))
		mustEval(t, s)
		mustEval(t, s)
		if got := len(st.ListAssignments(envID)); got != first {
			t.Fatalf("assignment count changed on re-evaluation: %d → %d", first, got)
		}
		if len(runningByNode(st)) != 3 {
			t.Fatalf("should remain 3 replicas, got %v", runningByNode(st))
		}
	})

	t.Run("no active release means no placement", func(t *testing.T) {
		s, rs := newSched(t)
		st := rs.State()
		addNodes(st, "n1")
		st.PutEnvironment(&zatterav1.Environment{
			Meta:    &zatterav1.Meta{Id: envID},
			Service: &zatterav1.ServiceSpec{Replicas: &zatterav1.ReplicaRange{Min: 2}},
			// no ActiveReleaseId
		})
		mustEval(t, s)
		if got := len(st.ListAssignments(envID)); got != 0 {
			t.Fatalf("no active release should place nothing, got %d", got)
		}
	})
}

// --- helpers --------------------------------------------------------------

func newSched(t *testing.T) (*Scheduler, *raftstore.Store) {
	t.Helper()
	rs := raftstore.NewTestStore(t)
	return New(rs, clock.NewFake(), nil), rs
}

func mustEval(t *testing.T, s *Scheduler) {
	t.Helper()
	if err := s.evaluate(context.Background()); err != nil {
		t.Fatalf("evaluate: %v", err)
	}
}

func addNodes(st *state.Store, ids ...string) {
	for _, id := range ids {
		st.PutNode(&zatterav1.Node{
			Meta:        &zatterav1.Meta{Id: id},
			Status:      zatterav1.NodeStatus_NODE_STATUS_ALIVE,
			Schedulable: true,
			Roles:       []zatterav1.NodeRole{zatterav1.NodeRole_NODE_ROLE_WORKER},
		})
	}
}

func addEnvRelease(st *state.Store, min uint32) {
	spec := &zatterav1.ServiceSpec{Replicas: &zatterav1.ReplicaRange{Min: min, Max: min + 5}}
	st.PutRelease(&zatterav1.Release{
		Meta:          &zatterav1.Meta{Id: relID},
		EnvironmentId: envID,
		ConfigHash:    "hash-v1",
		Service:       spec,
	})
	st.PutEnvironment(&zatterav1.Environment{
		Meta:            &zatterav1.Meta{Id: envID},
		Name:            "production",
		ActiveReleaseId: relID,
		Service:         spec,
	})
}

func setReplicas(st *state.Store, min uint32) {
	env, _ := st.Environment(envID)
	env.GetService().Replicas = &zatterav1.ReplicaRange{Min: min, Max: min + 5}
	env.EffectiveReplicas = 0
	st.PutEnvironment(env)
}

// runningByNode counts desired-RUN assignments per node for the test env.
func runningByNode(st *state.Store) map[string]int {
	out := map[string]int{}
	for _, a := range st.ListAssignments(envID) {
		if a.GetDesired() == zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN {
			out[a.GetNodeId()]++
		}
	}
	return out
}
