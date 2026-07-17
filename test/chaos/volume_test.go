//go:build chaos

package chaos

import (
	"testing"
	"time"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/state"
)

const cVolEnvID = "senv"

// TestVolumeFencing checks the stateful-volume safety invariants under a node
// loss: the volume auto-creates and pins, its fencing lease is held by the
// pinned node, and when that node dies the volume goes NODE_LOST with NO second
// replica placed elsewhere — the no-double-run guarantee (spec §9.1). The
// agent-side half (a node refuses to start a container without a matching lease)
// is covered by agent.TestExecutorVolumeFencing.
func TestVolumeFencing(t *testing.T) {
	h := New(t)
	h.seedStatefulVolume(t)

	// The scheduler auto-creates the volume, pins it, places one replica and
	// leases it.
	var pinned string
	waitUntil(t, 30*time.Second, func() bool {
		st := h.leaderState()
		if st == nil {
			return false
		}
		v := onlyVolume(st)
		if v == nil || v.GetNodeId() == "" || v.GetStatus() != zatterav1.VolumeStatus_VOLUME_STATUS_ACTIVE {
			return false
		}
		run := runReplicas(st, cVolEnvID)
		if len(run) != 1 || run[0].GetNodeId() != v.GetNodeId() {
			return false
		}
		l := v.GetLease()
		if l == nil || l.GetNodeId() != v.GetNodeId() || l.GetAssignmentId() != run[0].GetMeta().GetId() {
			return false
		}
		pinned = v.GetNodeId()
		return true
	})

	// The pinned node dies.
	if err := h.C.Apply(withMeta(&clusterv1.Command{Mutation: &clusterv1.Command_SetNodeStatus{SetNodeStatus: &clusterv1.SetNodeStatus{
		NodeId: pinned, Status: zatterav1.NodeStatus_NODE_STATUS_DOWN,
	}}})); err != nil {
		t.Fatalf("mark node down: %v", err)
	}

	// The volume goes NODE_LOST and the service is NOT rescheduled elsewhere.
	waitUntil(t, 30*time.Second, func() bool {
		st := h.leaderState()
		return st != nil && onlyVolume(st).GetStatus() == zatterav1.VolumeStatus_VOLUME_STATUS_NODE_LOST
	})

	// Hold the invariant for a while: never a second RUN replica, and never one
	// on a live node (which would double-mount the volume's data).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		st := h.leaderState()
		if st == nil {
			continue
		}
		run := runReplicas(st, cVolEnvID)
		if len(run) != 1 {
			t.Fatalf("stateful volume double-run: %d RUN replicas", len(run))
		}
		if run[0].GetNodeId() != pinned {
			t.Fatalf("replica migrated off the pinned (dead) node to %s", run[0].GetNodeId())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// seedStatefulVolume registers a stateful service (one volume) as a fresh env.
func (h *Harness) seedStatefulVolume(t *testing.T) {
	t.Helper()
	spec := &zatterav1.ServiceSpec{
		Replicas: &zatterav1.ReplicaRange{Min: 1, Max: 1},
		Stateful: true,
		Volumes:  []*zatterav1.VolumeMount{{VolumeName: "data", MountPath: "/data"}},
	}
	rel := "srel"
	cmds := []*clusterv1.Command{
		{Mutation: &clusterv1.Command_PutRelease{PutRelease: &clusterv1.PutRelease{Release: &zatterav1.Release{
			Meta: metaID(rel), EnvironmentId: cVolEnvID, ImageRef: "db:v1", ConfigHash: "sh1", Service: spec,
		}}}},
		{Mutation: &clusterv1.Command_PutEnvironment{PutEnvironment: &clusterv1.PutEnvironment{Environment: &zatterav1.Environment{
			Meta: metaID(cVolEnvID), ProjectId: "p", AppId: "a", Name: "db", Service: spec, ActiveReleaseId: rel,
		}}}},
	}
	for _, cmd := range cmds {
		if err := h.C.Apply(withMeta(cmd)); err != nil {
			t.Fatalf("seed stateful: %v", err)
		}
	}
}

func onlyVolume(st *state.Store) *zatterav1.Volume {
	for _, v := range st.ListVolumes("") {
		if v.GetEnvironmentId() == cVolEnvID {
			return v
		}
	}
	return nil
}

func runReplicas(st *state.Store, envID string) []*zatterav1.Assignment {
	var out []*zatterav1.Assignment
	for _, a := range st.ListAssignments(envID) {
		if a.GetDesired() == zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN {
			out = append(out, a)
		}
	}
	return out
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}
