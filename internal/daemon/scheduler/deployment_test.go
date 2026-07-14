package scheduler

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/state"
)

const (
	depID    = "dep1"
	dEnvID   = "env-d"
	blueRel  = "rel1"
	greenRel = "rel2"
)

func TestDeployment(t *testing.T) {
	t.Run("happy path walks through to SUCCEEDED and promotes traffic", func(t *testing.T) {
		o, rs, clk := newDeployRig(t)
		st := rs.State()

		step(t, o, depID) // PENDING → PLACING
		phaseIs(t, st, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PLACING)
		step(t, o, depID) // PLACING (place green) → STARTING
		phaseIs(t, st, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_STARTING)
		if got := len(greenOf(st)); got != 2 {
			t.Fatalf("want 2 green replicas, got %d", got)
		}

		setGreen(st, zatterav1.InstanceState_INSTANCE_STATE_RUNNING)
		step(t, o, depID) // STARTING → HEALTHCHECKING
		phaseIs(t, st, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_HEALTHCHECKING)

		setGreen(st, zatterav1.InstanceState_INSTANCE_STATE_HEALTHY)
		step(t, o, depID) // HEALTHCHECKING → PROMOTING
		phaseIs(t, st, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PROMOTING)

		step(t, o, depID) // PROMOTING → DRAINING_OLD (+ traffic switch)
		phaseIs(t, st, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_DRAINING_OLD)
		env, _ := st.Environment(dEnvID)
		if env.GetActiveReleaseId() != greenRel || env.GetRouteGeneration() == 0 {
			t.Fatalf("promote should switch active release + bump route gen: %+v", env)
		}

		// Blue stays RUN during the drain window.
		if n := runningBlue(st); n != 2 {
			t.Fatalf("blue should stay running during drain, got %d", n)
		}
		clk.Advance(drainWindow + time.Minute)
		step(t, o, depID) // DRAINING_OLD → SUCCEEDED (reap blue)
		phaseIs(t, st, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_SUCCEEDED)
		if n := runningBlue(st); n != 0 {
			t.Fatalf("blue should be stopped after drain, got %d still RUN", n)
		}
	})

	t.Run("a failing green instance aborts and leaves blue untouched", func(t *testing.T) {
		o, rs, _ := newDeployRig(t)
		st := rs.State()
		step(t, o, depID) // → PLACING
		step(t, o, depID) // → STARTING (green placed)

		// One green instance fails.
		green := greenOf(st)
		st.SetAssignmentObserved(green[0].GetNodeId(), map[string]*zatterav1.AssignmentObserved{
			green[0].GetMeta().GetId(): {State: zatterav1.InstanceState_INSTANCE_STATE_FAILED},
		})
		step(t, o, depID) // STARTING → FAILED (abort)

		phaseIs(t, st, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_FAILED)
		if got := len(greenOf(st)); got != 0 {
			t.Fatalf("green should be reaped on abort, got %d", got)
		}
		if n := runningBlue(st); n != 2 {
			t.Fatalf("blue must be untouched on abort, got %d", n)
		}
		env, _ := st.Environment(dEnvID)
		if env.GetActiveReleaseId() != blueRel {
			t.Fatalf("traffic must not move on abort, active = %s", env.GetActiveReleaseId())
		}
	})

	t.Run("rollback within the drain window promotes immediately", func(t *testing.T) {
		o, rs, clk := newDeployRig(t)
		st := rs.State()
		// Simulate: rel2 is already active, rel1 (blue) still warm & healthy.
		env, _ := st.Environment(dEnvID)
		env.ActiveReleaseId = greenRel
		st.PutEnvironment(env)
		markBlueHealthy(st)

		// A rollback deployment targeting the still-warm rel1 (newer than dep1).
		clk.Advance(time.Minute)
		st.PutDeployment(&zatterav1.Deployment{
			Meta:          &zatterav1.Meta{Id: "roll1", CreatedAt: pbTime(clk)},
			EnvironmentId: dEnvID, AppId: "a1", ProjectId: "p1",
			ReleaseId:         blueRel,
			PreviousReleaseId: greenRel,
			Phase:             zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PENDING,
			IsRollback:        true,
		})

		d, _ := st.Deployment("roll1")
		if err := o.reconcile(context.Background(), d); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		roll, _ := st.Deployment("roll1")
		if roll.GetPhase() != zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PROMOTING {
			t.Fatalf("warm rollback should jump to PROMOTING, got %v", roll.GetPhase())
		}
	})

	t.Run("a newer deployment supersedes the older one", func(t *testing.T) {
		o, rs, clk := newDeployRig(t)
		st := rs.State()
		// A newer, in-flight deployment for the same env.
		clk.Advance(time.Minute)
		st.PutDeployment(&zatterav1.Deployment{
			Meta:          &zatterav1.Meta{Id: "dep2", CreatedAt: pbTime(clk)},
			EnvironmentId: dEnvID, AppId: "a1", ProjectId: "p1",
			ReleaseId: greenRel,
			Phase:     zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PENDING,
		})

		d, _ := st.Deployment(depID)
		if err := o.reconcile(context.Background(), d); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		got, _ := st.Deployment(depID)
		if got.GetPhase() != zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_SUPERSEDED {
			t.Fatalf("older deployment should be SUPERSEDED, got %v", got.GetPhase())
		}
	})

	t.Run("a newer deployment never reaps an already-promoted one's live green", func(t *testing.T) {
		o, rs, clk := newDeployRig(t)
		st := rs.State()

		// Walk dep1 to DRAINING_OLD: its green (rel2) is now the live release.
		step(t, o, depID) // → PLACING
		step(t, o, depID) // → STARTING (green placed)
		setGreen(st, zatterav1.InstanceState_INSTANCE_STATE_HEALTHY)
		step(t, o, depID) // → HEALTHCHECKING
		step(t, o, depID) // → PROMOTING
		step(t, o, depID) // → DRAINING_OLD (traffic switched)
		phaseIs(t, st, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_DRAINING_OLD)
		liveGreen := len(greenOf(st))
		if liveGreen == 0 {
			t.Fatal("expected a live green set after promotion")
		}

		// A newer deploy arrives while dep1 is still draining (well within the
		// drain window).
		clk.Advance(time.Minute)
		st.PutDeployment(&zatterav1.Deployment{
			Meta:          &zatterav1.Meta{Id: "dep2", CreatedAt: pbTime(clk)},
			EnvironmentId: dEnvID, AppId: "a1", ProjectId: "p1",
			ReleaseId:         greenRel,
			PreviousReleaseId: greenRel,
			Phase:             zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PENDING,
		})

		step(t, o, depID) // dep1: superseded while DRAINING_OLD → complete, touch nothing
		got, _ := st.Deployment(depID)
		if got.GetPhase() != zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_SUCCEEDED {
			t.Fatalf("promoted+superseded deployment should complete, got %v", got.GetPhase())
		}
		if n := len(greenOf(st)); n != liveGreen {
			t.Fatalf("live green must survive supersession, had %d now %d", liveGreen, n)
		}
		// Blue must stay warm too: a rollback taking over may promote exactly
		// that release, so supersession must not stop it.
		if n := runningBlue(st); n != 2 {
			t.Fatalf("blue must stay warm through supersession, got %d still RUN", n)
		}
	})

	t.Run("stateful releases are rejected", func(t *testing.T) {
		o, rs, _ := newDeployRig(t)
		st := rs.State()
		rel, _ := st.Release(greenRel)
		rel.GetService().Stateful = true
		st.PutRelease(rel)

		step(t, o, depID)
		phaseIs(t, st, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_FAILED)
	})
}

// --- rig ------------------------------------------------------------------

func newDeployRig(t *testing.T) (*Orchestrator, *raftstore.Store, *clock.Fake) {
	t.Helper()
	rs := raftstore.NewTestStore(t)
	st := rs.State()
	clk := clock.NewFake()
	o := NewOrchestrator(rs, clk, nil)

	st.PutNode(pnode("n1", "", 1000, 4096))
	st.PutNode(pnode("n2", "", 1000, 4096))

	spec := &zatterav1.ServiceSpec{Replicas: &zatterav1.ReplicaRange{Min: 2}}
	st.PutRelease(&zatterav1.Release{Meta: &zatterav1.Meta{Id: blueRel}, EnvironmentId: dEnvID, ImageRef: "app:v1", Service: spec})
	st.PutRelease(&zatterav1.Release{Meta: &zatterav1.Meta{Id: greenRel}, EnvironmentId: dEnvID, ImageRef: "app:v2", Service: spec})
	st.PutEnvironment(&zatterav1.Environment{
		Meta: &zatterav1.Meta{Id: dEnvID}, ProjectId: "p1", AppId: "a1",
		Service: spec, ActiveReleaseId: blueRel,
	})
	// Blue: two RUN replicas of rel1.
	for i, n := range []string{"n1", "n2"} {
		st.PutAssignment(&zatterav1.Assignment{
			Meta: &zatterav1.Meta{Id: blueID(i)}, NodeId: n,
			EnvironmentId: dEnvID, AppId: "a1", ProjectId: "p1", ReleaseId: blueRel,
			Desired: zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN,
		})
	}
	st.PutDeployment(&zatterav1.Deployment{
		Meta:          &zatterav1.Meta{Id: depID, CreatedAt: pbTime(clk)},
		EnvironmentId: dEnvID, AppId: "a1", ProjectId: "p1",
		ReleaseId:         greenRel,
		PreviousReleaseId: blueRel,
		Phase:             zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PENDING,
	})
	return o, rs, clk
}

func step(t *testing.T, o *Orchestrator, id string) {
	t.Helper()
	d, ok := o.store.State().Deployment(id)
	if !ok {
		t.Fatalf("deployment %s gone", id)
	}
	if err := o.reconcile(context.Background(), d); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
}

func phaseIs(t *testing.T, st *state.Store, want zatterav1.DeploymentPhase) {
	t.Helper()
	d, _ := st.Deployment(depID)
	if d.GetPhase() != want {
		t.Fatalf("phase = %v, want %v (err=%q)", d.GetPhase(), want, d.GetError())
	}
}

func greenOf(st *state.Store) []*zatterav1.Assignment {
	d, _ := st.Deployment(depID)
	return greenAssignments(st, d)
}

func setGreen(st *state.Store, s zatterav1.InstanceState) {
	for _, a := range greenOf(st) {
		st.SetAssignmentObserved(a.GetNodeId(), map[string]*zatterav1.AssignmentObserved{
			a.GetMeta().GetId(): {State: s},
		})
	}
}

func runningBlue(st *state.Store) int {
	n := 0
	for _, a := range st.ListAssignments(dEnvID) {
		if a.GetReleaseId() == blueRel && a.GetDesired() == zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN {
			n++
		}
	}
	return n
}

func markBlueHealthy(st *state.Store) {
	for _, a := range st.ListAssignments(dEnvID) {
		if a.GetReleaseId() == blueRel {
			st.SetAssignmentObserved(a.GetNodeId(), map[string]*zatterav1.AssignmentObserved{
				a.GetMeta().GetId(): {State: zatterav1.InstanceState_INSTANCE_STATE_HEALTHY},
			})
		}
	}
}

func blueID(i int) string {
	if i == 0 {
		return "blue-0"
	}
	return "blue-1"
}

func pbTime(clk *clock.Fake) *timestamppb.Timestamp { return timestamppb.New(clk.Now()) }
