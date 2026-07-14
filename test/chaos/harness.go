//go:build chaos

// Package chaos runs failover/partition chaos tests against a real 3-node
// in-process control plane (simcluster) with the scheduler + red/green
// orchestrator running on whichever node is leader, plus a fake agent that
// drives instance health. Slow by design; gated behind the `chaos` build tag.
package chaos

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/scheduler"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
	"github.com/zattera-dev/zattera/internal/testutil/simcluster"
)

const (
	cEnvID     = "cenv"
	cBlueRel   = "crel-blue"
	cGreenRel  = "crel-green"
	chaosDrain = time.Second
)

// gate controls how far the fake agent lets green instances progress.
type gate struct {
	running atomic.Bool
	healthy atomic.Bool
}

// Harness is a running chaos cluster.
type Harness struct {
	C    *simcluster.Cluster
	gate *gate
}

// New boots a 3-node cluster, starts the control loops on every node (leader
// gated, as production), starts the fake agent, and seeds nodes + a blue
// release.
func New(t *testing.T) *Harness {
	c := simcluster.New(t, 3)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	for _, n := range c.Nodes {
		go scheduler.New(n.Store, clock.Real{}, log).Run(ctx)
		orch := scheduler.NewOrchestrator(n.Store, clock.Real{}, log)
		orch.SetDrainWindow(chaosDrain)
		go orch.Run(ctx)
	}

	h := &Harness{C: c, gate: &gate{}}
	go h.runAgent(ctx)
	h.seed(t)
	return h
}

// seed registers three worker nodes, the environment, blue+green releases and a
// running blue replica set (the outgoing version).
func (h *Harness) seed(t *testing.T) {
	spec := &zatterav1.ServiceSpec{Replicas: &zatterav1.ReplicaRange{Min: 2}}
	cmds := []*clusterv1.Command{
		putNode("w1"), putNode("w2"), putNode("w3"),
		{Mutation: &clusterv1.Command_PutRelease{PutRelease: &clusterv1.PutRelease{Release: &zatterav1.Release{Meta: metaID(cBlueRel), EnvironmentId: cEnvID, ImageRef: "app:v1", ConfigHash: "h1", Service: spec}}}},
		{Mutation: &clusterv1.Command_PutRelease{PutRelease: &clusterv1.PutRelease{Release: &zatterav1.Release{Meta: metaID(cGreenRel), EnvironmentId: cEnvID, ImageRef: "app:v2", ConfigHash: "h2", Service: spec}}}},
		{Mutation: &clusterv1.Command_PutEnvironment{PutEnvironment: &clusterv1.PutEnvironment{Environment: &zatterav1.Environment{Meta: metaID(cEnvID), ProjectId: "p", AppId: "a", Name: "production", Service: spec, ActiveReleaseId: cBlueRel}}}},
		putBlue("blue-1", "w1"), putBlue("blue-2", "w2"),
	}
	for _, cmd := range cmds {
		if err := h.C.Apply(withMeta(cmd)); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
}

// Deploy creates a PENDING green deployment for the env and returns its id.
func (h *Harness) Deploy(t *testing.T) string {
	depID := ids.New()
	cmd := withMeta(&clusterv1.Command{Mutation: &clusterv1.Command_PutDeployment{PutDeployment: &clusterv1.PutDeployment{Deployment: &zatterav1.Deployment{
		Meta:          metaID(depID),
		EnvironmentId: cEnvID, AppId: "a", ProjectId: "p",
		ReleaseId:         cGreenRel,
		PreviousReleaseId: cBlueRel,
		Phase:             zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PENDING,
	}}}})
	if err := h.C.Apply(cmd); err != nil {
		t.Fatalf("deploy: %v", err)
	}
	return depID
}

// runAgent marks blue instances HEALTHY and green instances up to the gate,
// replicating observed status through raft so it survives failover.
func (h *Harness) runAgent(ctx context.Context) {
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			st := h.leaderState()
			if st == nil {
				continue
			}
			byNode := map[string]map[string]*zatterav1.AssignmentObserved{}
			for _, a := range st.ListAssignments("") {
				if a.GetDesired() != zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN {
					continue
				}
				target := h.targetState(a)
				if target == zatterav1.InstanceState_INSTANCE_STATE_UNSPECIFIED || a.GetObserved().GetState() == target {
					continue
				}
				if byNode[a.GetNodeId()] == nil {
					byNode[a.GetNodeId()] = map[string]*zatterav1.AssignmentObserved{}
				}
				byNode[a.GetNodeId()][a.GetMeta().GetId()] = &zatterav1.AssignmentObserved{State: target, ContainerId: "fake"}
			}
			for node, obs := range byNode {
				_ = h.C.Apply(withMeta(&clusterv1.Command{Mutation: &clusterv1.Command_SetAssignmentsObserved{SetAssignmentsObserved: &clusterv1.SetAssignmentsObserved{NodeId: node, Observed: obs}}}))
			}
		}
	}
}

func (h *Harness) targetState(a *zatterav1.Assignment) zatterav1.InstanceState {
	if a.GetDeploymentId() == "" {
		return zatterav1.InstanceState_INSTANCE_STATE_HEALTHY // blue: always up
	}
	switch {
	case h.gate.healthy.Load():
		return zatterav1.InstanceState_INSTANCE_STATE_HEALTHY
	case h.gate.running.Load():
		return zatterav1.InstanceState_INSTANCE_STATE_RUNNING
	default:
		return zatterav1.InstanceState_INSTANCE_STATE_UNSPECIFIED
	}
}

// --- helpers --------------------------------------------------------------

func (h *Harness) allowRunning() { h.gate.running.Store(true) }
func (h *Harness) allowHealthy() { h.gate.running.Store(true); h.gate.healthy.Store(true) }

// leaderState returns the leader's state store, or nil during an election.
func (h *Harness) leaderState() *state.Store {
	if l := h.C.Leader(); l != nil {
		return l.State
	}
	return nil
}

// deploymentPhase reads a deployment's current phase from the leader.
func (h *Harness) deploymentPhase(id string) zatterav1.DeploymentPhase {
	st := h.leaderState()
	if st == nil {
		return zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_UNSPECIFIED
	}
	d, _ := st.Deployment(id)
	return d.GetPhase()
}

// waitPhaseAtLeast blocks until the deployment reaches at least `target` in the
// forward phase order (fails fast if it aborts first).
func (h *Harness) waitPhaseAtLeast(t *testing.T, id string, target zatterav1.DeploymentPhase, timeout time.Duration) {
	t.Helper()
	want := phaseRank(target)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		got := phaseRank(h.deploymentPhase(id))
		if got == rankFailed {
			t.Fatalf("deployment aborted before reaching %v", target)
		}
		if got >= want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for deployment to reach %v (now %v)", target, h.deploymentPhase(id))
}

// waitPhase blocks until the deployment is exactly `want`.
func (h *Harness) waitPhase(t *testing.T, id string, want zatterav1.DeploymentPhase, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if h.deploymentPhase(id) == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for deployment phase %v (now %v)", want, h.deploymentPhase(id))
}

// checkInvariants asserts the cluster-wide safety properties.
func (h *Harness) checkInvariants(t *testing.T) {
	t.Helper()
	st := h.leaderState()
	if st == nil {
		t.Fatal("no leader for invariant check")
	}
	// Every environment's active release exists (at most one, structurally).
	for _, env := range st.ListEnvironments("", "") {
		if r := env.GetActiveReleaseId(); r != "" {
			if _, ok := st.Release(r); !ok {
				t.Fatalf("env %s active release %s does not exist", env.GetMeta().GetId(), r)
			}
		}
	}
	// Every RUN assignment references an existing release + node; no stateful
	// service has two RUN replicas.
	statefulRun := map[string]int{}
	for _, a := range st.ListAssignments("") {
		if a.GetDesired() != zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN {
			continue
		}
		rel, ok := st.Release(a.GetReleaseId())
		if !ok {
			t.Fatalf("RUN assignment %s references missing release %s", a.GetMeta().GetId(), a.GetReleaseId())
		}
		if _, ok := st.Node(a.GetNodeId()); !ok {
			t.Fatalf("RUN assignment %s references missing node %s", a.GetMeta().GetId(), a.GetNodeId())
		}
		if rel.GetService().GetStateful() {
			statefulRun[a.GetEnvironmentId()]++
		}
	}
	for env, n := range statefulRun {
		if n > 1 {
			t.Fatalf("env %s has %d RUN replicas of a stateful service (fencing violated)", env, n)
		}
	}
}

// checkDeploymentConsistent asserts a terminal deployment left a coherent state:
// traffic switched iff it promoted, and an aborted deploy leaves no green.
func (h *Harness) checkDeploymentConsistent(t *testing.T, depID string) {
	t.Helper()
	st := h.leaderState()
	d, _ := st.Deployment(depID)
	env, _ := st.Environment(d.GetEnvironmentId())

	green := 0
	for _, a := range st.ListAssignments(d.GetEnvironmentId()) {
		if a.GetDeploymentId() == depID && a.GetDesired() == zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN {
			green++
		}
	}
	promoted := d.GetPhase() == zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_DRAINING_OLD ||
		d.GetPhase() == zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_SUCCEEDED
	switch {
	case promoted && env.GetActiveReleaseId() != d.GetReleaseId():
		t.Fatalf("deployment promoted but active release is %s, not %s", env.GetActiveReleaseId(), d.GetReleaseId())
	case !promoted && env.GetActiveReleaseId() == d.GetReleaseId():
		t.Fatalf("traffic switched to %s without promotion (phase %v)", d.GetReleaseId(), d.GetPhase())
	}
	if d.GetPhase() == zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_FAILED && green > 0 {
		t.Fatalf("aborted deployment left %d orphan green replicas", green)
	}
}

const (
	rankFailed = -1
)

// phaseRank orders the forward phases; failure phases sort to rankFailed.
func phaseRank(p zatterav1.DeploymentPhase) int {
	switch p {
	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PENDING:
		return 1
	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PLACING:
		return 2
	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_STARTING:
		return 3
	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_HEALTHCHECKING:
		return 4
	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PROMOTING:
		return 5
	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_DRAINING_OLD:
		return 6
	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_SUCCEEDED:
		return 7
	default:
		return rankFailed
	}
}

func putNode(id string) *clusterv1.Command {
	return &clusterv1.Command{Mutation: &clusterv1.Command_PutNode{PutNode: &clusterv1.PutNode{Node: &zatterav1.Node{
		Meta:        metaID(id),
		Name:        id,
		Roles:       []zatterav1.NodeRole{zatterav1.NodeRole_NODE_ROLE_WORKER},
		Status:      zatterav1.NodeStatus_NODE_STATUS_ALIVE,
		Schedulable: true,
		Capacity:    &zatterav1.ResourceLimits{CpuMillis: 8000, MemoryMb: 16384},
	}}}}
}

func putBlue(id, node string) *clusterv1.Command {
	return &clusterv1.Command{Mutation: &clusterv1.Command_PutAssignments{PutAssignments: &clusterv1.PutAssignments{Assignments: []*zatterav1.Assignment{{
		Meta:          metaID(id),
		NodeId:        node,
		EnvironmentId: cEnvID, AppId: "a", ProjectId: "p",
		ReleaseId: cBlueRel,
		Desired:   zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN,
		Observed:  &zatterav1.AssignmentObserved{State: zatterav1.InstanceState_INSTANCE_STATE_HEALTHY},
	}}}}}
}

func metaID(id string) *zatterav1.Meta {
	return &zatterav1.Meta{Id: id, CreatedAt: timestamppb.Now(), UpdatedAt: timestamppb.Now()}
}

func withMeta(cmd *clusterv1.Command) *clusterv1.Command {
	cmd.RequestId = ids.New()
	cmd.Actor = "chaos"
	cmd.Time = timestamppb.Now()
	return cmd
}
