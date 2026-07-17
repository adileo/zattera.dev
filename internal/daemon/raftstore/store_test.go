package raftstore

import (
	"context"
	"errors"
	"testing"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/state"
)

func cmdPutProject(requestID, id, name string) *clusterv1.Command {
	return &clusterv1.Command{
		RequestId: requestID,
		Actor:     "test",
		Time:      timestamppb.Now(),
		Mutation: &clusterv1.Command_PutProject{PutProject: &clusterv1.PutProject{
			Project: &zatterav1.Project{Meta: &zatterav1.Meta{Id: id}, Name: name},
		}},
	}
}

func TestApplyRoundTrip(t *testing.T) {
	s := NewTestStore(t)
	ctx := context.Background()

	if err := s.Apply(ctx, cmdPutProject("req-1", "p1", "demo")); err != nil {
		t.Fatalf("apply: %v", err)
	}
	p, ok := s.State().Project("p1")
	if !ok || p.GetName() != "demo" {
		t.Fatalf("project not applied: %v %v", p, ok)
	}
}

func TestApplyIdempotency(t *testing.T) {
	s := NewTestStore(t)
	ctx := context.Background()

	if err := s.Apply(ctx, cmdPutProject("req-1", "p1", "first")); err != nil {
		t.Fatal(err)
	}
	// Same request_id: the mutation must be skipped.
	if err := s.Apply(ctx, cmdPutProject("req-1", "p1", "second")); err != nil {
		t.Fatal(err)
	}
	p, _ := s.State().Project("p1")
	if p.GetName() != "first" {
		t.Fatalf("duplicate request mutated state: %q", p.GetName())
	}
}

func TestApplyBusinessError(t *testing.T) {
	s := NewTestStore(t)
	ctx := context.Background()

	put := func(reqID string, expected int64) error {
		return s.Apply(ctx, &clusterv1.Command{
			RequestId: reqID,
			Time:      timestamppb.Now(),
			Mutation: &clusterv1.Command_PutKv{PutKv: &clusterv1.PutKV{
				Key:             "locks/x",
				Value:           []byte("v"),
				ExpectedVersion: expected,
			}},
		})
	}
	if err := put("r1", 0); err != nil {
		t.Fatalf("create: %v", err)
	}
	err := put("r2", 0)
	if !errors.Is(err, state.ErrKVConflict) {
		t.Fatalf("expected CAS conflict through raft, got %v", err)
	}
}

func TestPromoteReleaseBumpsRouteGeneration(t *testing.T) {
	s := NewTestStore(t)
	ctx := context.Background()

	apply := func(m any) {
		t.Helper()
		cmd := &clusterv1.Command{Time: timestamppb.Now()}
		switch v := m.(type) {
		case *clusterv1.PutEnvironment:
			cmd.Mutation = &clusterv1.Command_PutEnvironment{PutEnvironment: v}
		case *clusterv1.PutRelease:
			cmd.Mutation = &clusterv1.Command_PutRelease{PutRelease: v}
		case *clusterv1.PromoteRelease:
			cmd.Mutation = &clusterv1.Command_PromoteRelease{PromoteRelease: v}
		default:
			t.Fatalf("unhandled %T", m)
		}
		if err := s.Apply(ctx, cmd); err != nil {
			t.Fatalf("apply %T: %v", m, err)
		}
	}

	apply(&clusterv1.PutEnvironment{Environment: &zatterav1.Environment{
		Meta: &zatterav1.Meta{Id: "e1"}, Name: "production",
	}})
	apply(&clusterv1.PutRelease{Release: &zatterav1.Release{
		Meta: &zatterav1.Meta{Id: "r1"}, EnvironmentId: "e1", Version: 1,
	}})
	apply(&clusterv1.PromoteRelease{EnvironmentId: "e1", ReleaseId: "r1"})

	env, _ := s.State().Environment("e1")
	if env.GetActiveReleaseId() != "r1" || env.GetRouteGeneration() != 1 {
		t.Fatalf("promote: active=%s gen=%d", env.GetActiveReleaseId(), env.GetRouteGeneration())
	}

	// Promoting a missing release must fail without touching state.
	err := s.Apply(ctx, &clusterv1.Command{
		Time: timestamppb.Now(),
		Mutation: &clusterv1.Command_PromoteRelease{PromoteRelease: &clusterv1.PromoteRelease{
			EnvironmentId: "e1", ReleaseId: "missing",
		}},
	})
	if err == nil {
		t.Fatal("promote of missing release succeeded")
	}
	env, _ = s.State().Environment("e1")
	if env.GetActiveReleaseId() != "r1" {
		t.Fatal("failed promote mutated state")
	}
}

func TestSnapshotRestoreThroughRaft(t *testing.T) {
	s := NewTestStore(t)
	ctx := context.Background()

	for _, id := range []string{"p1", "p2", "p3"} {
		if err := s.Apply(ctx, cmdPutProject("req-"+id, id, id)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Raft().Snapshot().Error(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// FSM state content survival is exercised by the state-level round-trip
	// tests; here we assert the raft snapshot completed and the store still
	// serves reads afterwards.
	if got := len(s.State().ListProjects()); got != 3 {
		t.Fatalf("projects after snapshot: %d", got)
	}
}

func TestApplyOnNonLeaderFails(t *testing.T) {
	// A node that never bootstraps has no leader: Apply must refuse.
	s := NewTestNode(t, "lonely", false, nil)
	err := s.Apply(context.Background(), cmdPutProject("r", "p", "n"))
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
}

// TestShutdownReleasesBoltLock reopens the same on-disk data dir after Shutdown.
// raft.Shutdown does not close the bbolt log/stable store it was handed, so
// unless Shutdown closes it explicitly the file lock leaks and the reopen's
// bbolt.Open blocks forever — the deadlock that hung the disaster-recovery
// restore-then-verify path. A leaked lock makes this test time out.
func TestShutdownReleasesBoltLock(t *testing.T) {
	dir := t.TempDir()
	_, tr := raft.NewInmemTransport("reopen")
	open := func() *Store {
		s, err := New(Config{NodeID: "reopen", DataDir: dir, Transport: tr}, state.New())
		if err != nil {
			t.Fatalf("raftstore.New: %v", err)
		}
		return s
	}

	first := open()
	if err := first.Shutdown(); err != nil {
		t.Fatalf("first shutdown: %v", err)
	}
	// Would deadlock on the leaked flock before the fix.
	second := open()
	if err := second.Shutdown(); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}
}
