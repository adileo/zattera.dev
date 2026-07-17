package api

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

func TestActivatorWakesEnv(t *testing.T) {
	rs := raftstore.NewTestStore(t)
	st := rs.State()
	st.PutEnvironment(&zatterav1.Environment{
		Meta:              &zatterav1.Meta{Id: "e1"},
		Name:              "production",
		ActiveReleaseId:   "rel1",
		EffectiveReplicas: 0, // cold
		Service:           &zatterav1.ServiceSpec{ScaleToZero: true, Replicas: &zatterav1.ReplicaRange{Min: 2, Max: 5}},
	})
	srv := NewActivatorServer(st, rs, clock.NewFake(), nil)
	ctx := context.Background()

	resp, err := srv.Activate(ctx, &clusterv1.ActivateRequest{EnvironmentId: "e1", NodeId: "n1"})
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	if !resp.GetAccepted() {
		t.Fatal("activation not accepted")
	}
	env, _ := st.Environment("e1")
	if env.GetEffectiveReplicas() != 2 {
		t.Fatalf("effective_replicas = %d, want 2 (min)", env.GetEffectiveReplicas())
	}

	// Idempotent: a second call while already warm is accepted, no change.
	if _, err := srv.Activate(ctx, &clusterv1.ActivateRequest{EnvironmentId: "e1"}); err != nil {
		t.Fatalf("second activate: %v", err)
	}
	env, _ = st.Environment("e1")
	if env.GetEffectiveReplicas() != 2 {
		t.Fatalf("effective_replicas changed on idempotent call: %d", env.GetEffectiveReplicas())
	}
}

func TestActivatorRejectsUnknownAndNonScaleToZero(t *testing.T) {
	rs := raftstore.NewTestStore(t)
	st := rs.State()
	st.PutEnvironment(&zatterav1.Environment{
		Meta: &zatterav1.Meta{Id: "e2"}, Name: "production",
		Service: &zatterav1.ServiceSpec{Replicas: &zatterav1.ReplicaRange{Min: 1}}, // not scale-to-zero
	})
	srv := NewActivatorServer(st, rs, clock.NewFake(), nil)
	ctx := context.Background()

	if _, err := srv.Activate(ctx, &clusterv1.ActivateRequest{EnvironmentId: "missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("unknown env code = %v, want NotFound", status.Code(err))
	}
	resp, err := srv.Activate(ctx, &clusterv1.ActivateRequest{EnvironmentId: "e2"})
	if err != nil {
		t.Fatalf("non-scale-to-zero activate: %v", err)
	}
	if resp.GetAccepted() {
		t.Fatal("non-scale-to-zero env should not be accepted for activation")
	}
	env, _ := st.Environment("e2")
	if env.GetEffectiveReplicas() != 0 {
		t.Fatalf("non-scale-to-zero env effective changed: %d", env.GetEffectiveReplicas())
	}
}
