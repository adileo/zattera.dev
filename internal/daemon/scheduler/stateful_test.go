package scheduler

import (
	"testing"
	"time"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/state"
)

// newStatefulRig sets up a stateful env (single volume-pinned instance) mid
// update: blue (rel1) running on n1 with volume "data" pinned there, and a
// green (rel2) deployment PENDING. Reuses the shared depID/dEnvID/rel ids so the
// deployment_test helpers (greenOf/step/phaseIs) apply.
func newStatefulRig(t *testing.T) (*Orchestrator, *raftstore.Store, *clock.Fake) {
	t.Helper()
	rs := raftstore.NewTestStore(t)
	st := rs.State()
	clk := clock.NewFake()
	o := NewOrchestrator(rs, clk, nil)

	st.PutNode(pnode("n1", "", 1000, 4096))
	st.PutNode(pnode("n2", "", 1000, 4096))

	spec := &zatterav1.ServiceSpec{
		Replicas: &zatterav1.ReplicaRange{Min: 1, Max: 1},
		Stateful: true,
		Volumes:  []*zatterav1.VolumeMount{{VolumeName: "data", MountPath: "/data"}},
	}
	st.PutRelease(&zatterav1.Release{Meta: &zatterav1.Meta{Id: blueRel}, EnvironmentId: dEnvID, ImageRef: "db:v1", Service: spec})
	st.PutRelease(&zatterav1.Release{Meta: &zatterav1.Meta{Id: greenRel}, EnvironmentId: dEnvID, ImageRef: "db:v2", Service: spec})
	st.PutEnvironment(&zatterav1.Environment{
		Meta: &zatterav1.Meta{Id: dEnvID}, ProjectId: "p1", AppId: "a1",
		Service: spec, ActiveReleaseId: blueRel,
	})
	st.PutVolume(&zatterav1.Volume{
		Meta: &zatterav1.Meta{Id: "vol-data"}, ProjectId: "p1", EnvironmentId: dEnvID,
		Name: "data", NodeId: "n1", Status: zatterav1.VolumeStatus_VOLUME_STATUS_ACTIVE,
	})
	// Blue: one RUN replica of rel1 on n1, healthy.
	st.PutAssignment(&zatterav1.Assignment{
		Meta: &zatterav1.Meta{Id: "blue-0"}, NodeId: "n1",
		EnvironmentId: dEnvID, AppId: "a1", ProjectId: "p1", ReleaseId: blueRel,
		Desired:  zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN,
		Observed: &zatterav1.AssignmentObserved{State: zatterav1.InstanceState_INSTANCE_STATE_HEALTHY},
	})
	st.PutDeployment(&zatterav1.Deployment{
		Meta:          &zatterav1.Meta{Id: depID, CreatedAt: pbTime(clk)},
		EnvironmentId: dEnvID, AppId: "a1", ProjectId: "p1",
		ReleaseId:         greenRel,
		PreviousReleaseId: blueRel,
		Phase:             zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PENDING,
	})
	return o, rs, clk
}

// runningStateful counts RUN assignments for the env — the double-run guard.
func runningStateful(st *state.Store) int {
	n := 0
	for _, a := range st.ListAssignments(dEnvID) {
		if a.GetDesired() == zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN {
			n++
		}
	}
	return n
}

func setBlueObserved(st *state.Store, s zatterav1.InstanceState) {
	st.SetAssignmentObserved("n1", map[string]*zatterav1.AssignmentObserved{
		"blue-0": {State: s},
	})
}

// stepSafe runs one reconcile step and asserts the single-writer invariant holds
// immediately after: never two RUN assignments for the volume.
func stepSafe(t *testing.T, o *Orchestrator, st *state.Store) {
	t.Helper()
	step(t, o, depID)
	if n := runningStateful(st); n > 1 {
		t.Fatalf("double-run: %d RUN assignments for the stateful volume", n)
	}
}

func TestStateful(t *testing.T) {
	t.Run("stop-then-start walks to SUCCEEDED without a double-run", func(t *testing.T) {
		o, rs, _ := newStatefulRig(t)
		st := rs.State()

		stepSafe(t, o, st) // PENDING → STOPPING_OLD
		phaseIs(t, st, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_STOPPING_OLD)

		stepSafe(t, o, st) // STOPPING_OLD: flip blue to STOP, wait
		phaseIs(t, st, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_STOPPING_OLD)
		if runningStateful(st) != 0 {
			t.Fatal("blue should be flipped to STOP")
		}

		// Blue still has a container until the agent reports STOPPED.
		stepSafe(t, o, st) // still STOPPING_OLD (blue observed HEALTHY)
		phaseIs(t, st, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_STOPPING_OLD)

		setBlueObserved(st, zatterav1.InstanceState_INSTANCE_STATE_STOPPED)
		stepSafe(t, o, st) // STOPPING_OLD → STARTING
		phaseIs(t, st, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_STARTING)

		stepSafe(t, o, st) // STARTING: place green on the pinned node
		green := greenOf(st)
		if len(green) != 1 || green[0].GetNodeId() != "n1" {
			t.Fatalf("want 1 green on n1 (the volume's node), got %+v", green)
		}

		setGreen(st, zatterav1.InstanceState_INSTANCE_STATE_RUNNING)
		stepSafe(t, o, st) // STARTING → HEALTHCHECKING
		phaseIs(t, st, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_HEALTHCHECKING)

		setGreen(st, zatterav1.InstanceState_INSTANCE_STATE_HEALTHY)
		stepSafe(t, o, st) // HEALTHCHECKING → PROMOTING
		phaseIs(t, st, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PROMOTING)

		stepSafe(t, o, st) // PROMOTING → SUCCEEDED (+ traffic switch)
		phaseIs(t, st, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_SUCCEEDED)
		env, _ := st.Environment(dEnvID)
		if env.GetActiveReleaseId() != greenRel {
			t.Fatalf("promote should switch active release to green, got %s", env.GetActiveReleaseId())
		}
		if runningStateful(st) != 1 {
			t.Fatalf("exactly one instance should run after success, got %d", runningStateful(st))
		}
	})

	t.Run("first deploy (no previous instance) auto-creates the volume", func(t *testing.T) {
		o, rs, _ := newStatefulRig(t)
		st := rs.State()
		// Make it a first deploy: no blue, no volume, no previous release.
		st.DeleteAssignments([]string{"blue-0"})
		st.DeleteVolume("vol-data")
		d, _ := st.Deployment(depID)
		d.PreviousReleaseId = ""
		env, _ := st.Environment(dEnvID)
		env.ActiveReleaseId = ""
		st.PutEnvironment(env)
		st.PutDeployment(d)

		stepSafe(t, o, st) // PENDING → STOPPING_OLD
		stepSafe(t, o, st) // STOPPING_OLD → STARTING (nothing to stop)
		phaseIs(t, st, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_STARTING)
		stepSafe(t, o, st) // STARTING: create volume + place green
		vols := st.ListVolumes("")
		if len(vols) != 1 || vols[0].GetName() != "data" {
			t.Fatalf("first deploy should auto-create the volume, got %+v", vols)
		}
		green := greenOf(st)
		if len(green) != 1 || green[0].GetNodeId() != vols[0].GetNodeId() {
			t.Fatalf("green must land on the new volume's node: green=%+v vol=%+v", green, vols[0])
		}
	})

	t.Run("failure after stopping the old instance restarts it and fails", func(t *testing.T) {
		o, rs, _ := newStatefulRig(t)
		st := rs.State()

		stepSafe(t, o, st) // → STOPPING_OLD
		stepSafe(t, o, st) // flip blue to STOP
		setBlueObserved(st, zatterav1.InstanceState_INSTANCE_STATE_STOPPED)
		stepSafe(t, o, st) // → STARTING
		stepSafe(t, o, st) // place green
		green := greenOf(st)
		if len(green) != 1 {
			t.Fatalf("expected a green instance, got %d", len(green))
		}

		// The new instance fails.
		st.SetAssignmentObserved(green[0].GetNodeId(), map[string]*zatterav1.AssignmentObserved{
			green[0].GetMeta().GetId(): {State: zatterav1.InstanceState_INSTANCE_STATE_FAILED},
		})
		stepSafe(t, o, st) // STARTING → FAILED (reap green, restart old)

		phaseIs(t, st, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_FAILED)
		if len(greenOf(st)) != 0 {
			t.Fatal("green should be reaped on failure")
		}
		// The old instance is restored (flipped back to RUN).
		blue, _ := st.Assignment("blue-0")
		if blue.GetDesired() != zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN {
			t.Fatalf("old instance should be restarted, desired = %v", blue.GetDesired())
		}
		env, _ := st.Environment(dEnvID)
		if env.GetActiveReleaseId() != blueRel {
			t.Fatalf("traffic must not have moved, active = %s", env.GetActiveReleaseId())
		}
	})

	t.Run("health-check timeout restarts the old instance", func(t *testing.T) {
		o, rs, clk := newStatefulRig(t)
		st := rs.State()

		stepSafe(t, o, st) // → STOPPING_OLD
		stepSafe(t, o, st) // flip blue to STOP
		setBlueObserved(st, zatterav1.InstanceState_INSTANCE_STATE_STOPPED)
		stepSafe(t, o, st) // → STARTING
		stepSafe(t, o, st) // place green
		setGreen(st, zatterav1.InstanceState_INSTANCE_STATE_RUNNING)
		stepSafe(t, o, st) // → HEALTHCHECKING

		// Never becomes healthy; the deadline passes.
		rel, _ := st.Release(greenRel)
		clk.Advance(healthDeadline(rel) + time.Minute)
		stepSafe(t, o, st) // HEALTHCHECKING → FAILED

		phaseIs(t, st, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_FAILED)
		blue, _ := st.Assignment("blue-0")
		if blue.GetDesired() != zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN {
			t.Fatal("old instance should be restarted after a health-check timeout")
		}
	})
}
