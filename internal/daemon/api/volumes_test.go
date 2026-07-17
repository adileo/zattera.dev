package api

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/state"
)

// fakeVolumeDialer records RemoveVolume calls and can be made to fail.
type fakeVolumeDialer struct {
	calls []string // "node/env/name"
	err   error
}

func (f *fakeVolumeDialer) RemoveVolume(_ context.Context, node *zatterav1.Node, envID, name string) error {
	f.calls = append(f.calls, node.GetMeta().GetId()+"/"+envID+"/"+name)
	return f.err
}

func newVolumeHarness(t *testing.T) (*VolumeServer, *state.Store, context.Context) {
	t.Helper()
	rs := raftstore.NewTestStore(t)
	st := rs.State()
	st.PutNode(&zatterav1.Node{
		Meta: &zatterav1.Meta{Id: "n1"}, Status: zatterav1.NodeStatus_NODE_STATUS_ALIVE, Schedulable: true,
		Roles: []zatterav1.NodeRole{zatterav1.NodeRole_NODE_ROLE_WORKER},
	})
	st.PutApp(&zatterav1.App{Meta: &zatterav1.Meta{Id: "app1"}, Name: "web", ProjectId: "p1"})
	st.PutEnvironment(&zatterav1.Environment{Meta: &zatterav1.Meta{Id: "e1"}, AppId: "app1", ProjectId: "p1", Name: "production"})
	srv := NewVolumeServer(st, rs, nil, clock.NewFake(), nil)
	ctx := withIdentity(context.Background(), Identity{UserID: "u1"})
	return srv, st, ctx
}

func statusCode(err error) codes.Code { return status.Code(err) }

func TestVolumeServerCRUD(t *testing.T) {
	srv, _, ctx := newVolumeHarness(t)

	v, err := srv.CreateVolume(ctx, &zatterav1.CreateVolumeRequest{ProjectId: "p1", EnvironmentId: "e1", Name: "data"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if v.GetNodeId() != "n1" {
		t.Fatalf("volume pinned to %q, want n1 (only ALIVE worker)", v.GetNodeId())
	}
	if v.GetStatus() != zatterav1.VolumeStatus_VOLUME_STATUS_ACTIVE {
		t.Fatalf("status = %v, want ACTIVE", v.GetStatus())
	}

	list, err := srv.ListVolumes(ctx, &zatterav1.ListVolumesRequest{ProjectId: "p1"})
	if err != nil || len(list.GetVolumes()) != 1 {
		t.Fatalf("list = %v (err %v), want 1 volume", list.GetVolumes(), err)
	}

	// Duplicate name in the same env is rejected.
	_, err = srv.CreateVolume(ctx, &zatterav1.CreateVolumeRequest{ProjectId: "p1", EnvironmentId: "e1", Name: "data"})
	if statusCode(err) != codes.AlreadyExists {
		t.Fatalf("duplicate create = %v, want AlreadyExists", err)
	}

	// Delete removes it.
	if _, err := srv.DeleteVolume(ctx, &zatterav1.DeleteVolumeRequest{ProjectId: "p1", VolumeId: v.GetMeta().GetId()}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if list, _ := srv.ListVolumes(ctx, &zatterav1.ListVolumesRequest{ProjectId: "p1"}); len(list.GetVolumes()) != 0 {
		t.Fatalf("volume not deleted: %d remain", len(list.GetVolumes()))
	}
}

func TestVolumeServerDeleteRefusesWhileMounted(t *testing.T) {
	srv, st, ctx := newVolumeHarness(t)
	v, err := srv.CreateVolume(ctx, &zatterav1.CreateVolumeRequest{ProjectId: "p1", EnvironmentId: "e1", Name: "data"})
	if err != nil {
		t.Fatal(err)
	}
	// A running instance on the volume's node makes it in-use.
	st.PutAssignment(&zatterav1.Assignment{
		Meta: &zatterav1.Meta{Id: "a1"}, EnvironmentId: "e1", NodeId: "n1",
		Desired: zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN,
	})

	_, err = srv.DeleteVolume(ctx, &zatterav1.DeleteVolumeRequest{ProjectId: "p1", VolumeId: v.GetMeta().GetId()})
	if statusCode(err) != codes.FailedPrecondition {
		t.Fatalf("delete while mounted = %v, want FailedPrecondition", err)
	}

	// Stop the instance → delete succeeds.
	st.DeleteAssignments([]string{"a1"})
	if _, err := srv.DeleteVolume(ctx, &zatterav1.DeleteVolumeRequest{ProjectId: "p1", VolumeId: v.GetMeta().GetId()}); err != nil {
		t.Fatalf("delete after stop: %v", err)
	}
}

func TestVolumeServerDeleteCleansDockerVolume(t *testing.T) {
	rs := raftstore.NewTestStore(t)
	st := rs.State()
	st.PutNode(&zatterav1.Node{Meta: &zatterav1.Meta{Id: "n1"}, Status: zatterav1.NodeStatus_NODE_STATUS_ALIVE, Schedulable: true})
	st.PutApp(&zatterav1.App{Meta: &zatterav1.Meta{Id: "app1"}, Name: "web", ProjectId: "p1"})
	st.PutEnvironment(&zatterav1.Environment{Meta: &zatterav1.Meta{Id: "e1"}, AppId: "app1", ProjectId: "p1", Name: "production"})
	dialer := &fakeVolumeDialer{}
	srv := NewVolumeServer(st, rs, dialer, clock.NewFake(), nil)
	ctx := withIdentity(context.Background(), Identity{UserID: "u1"})

	v, err := srv.CreateVolume(ctx, &zatterav1.CreateVolumeRequest{ProjectId: "p1", EnvironmentId: "e1", Name: "data"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := srv.DeleteVolume(ctx, &zatterav1.DeleteVolumeRequest{ProjectId: "p1", VolumeId: v.GetMeta().GetId()}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if len(dialer.calls) != 1 || dialer.calls[0] != "n1/e1/data" {
		t.Fatalf("docker cleanup calls = %v, want [n1/e1/data]", dialer.calls)
	}

	// A cleanup failure must not fail the delete (best effort).
	v2, _ := srv.CreateVolume(ctx, &zatterav1.CreateVolumeRequest{ProjectId: "p1", EnvironmentId: "e1", Name: "data2"})
	dialer.err = errors.New("node unreachable")
	if _, err := srv.DeleteVolume(ctx, &zatterav1.DeleteVolumeRequest{ProjectId: "p1", VolumeId: v2.GetMeta().GetId()}); err != nil {
		t.Fatalf("delete should tolerate cleanup failure, got: %v", err)
	}
	if _, ok := st.Volume(v2.GetMeta().GetId()); ok {
		t.Fatal("volume not deleted despite cleanup error")
	}
}

func TestVolumeServerCreateValidation(t *testing.T) {
	srv, _, ctx := newVolumeHarness(t)

	if _, err := srv.CreateVolume(ctx, &zatterav1.CreateVolumeRequest{ProjectId: "p1", EnvironmentId: "nope", Name: "data"}); statusCode(err) != codes.NotFound {
		t.Fatalf("unknown env = %v, want NotFound", err)
	}
	if _, err := srv.CreateVolume(ctx, &zatterav1.CreateVolumeRequest{ProjectId: "p1", EnvironmentId: "e1", Name: "Bad Name"}); statusCode(err) != codes.InvalidArgument {
		t.Fatalf("bad name = %v, want InvalidArgument", err)
	}
}
