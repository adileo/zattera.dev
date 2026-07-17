package scheduler

import (
	"context"
	"testing"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/livestate"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/state"
)

func newServerless(t *testing.T) (*Serverless, *raftstore.Store, *clock.Fake, *livestate.Registry) {
	t.Helper()
	rs := raftstore.NewTestStore(t)
	clk := clock.NewFake()
	live := livestate.New(clk)
	return NewServerless(rs, live, clk, nil), rs, clk, live
}

// addServerlessEnv seeds a max_concurrency env with [min,max] and an initial
// effective count.
func addServerlessEnv(st *state.Store, maxConc, min, max, eff uint32, scaleToZero bool) {
	spec := &zatterav1.ServiceSpec{
		Replicas:       &zatterav1.ReplicaRange{Min: min, Max: max},
		MaxConcurrency: maxConc,
		ScaleToZero:    scaleToZero,
	}
	st.PutRelease(&zatterav1.Release{Meta: &zatterav1.Meta{Id: relID}, EnvironmentId: envID, ConfigHash: "h", Service: spec})
	st.PutEnvironment(&zatterav1.Environment{
		Meta: &zatterav1.Meta{Id: envID}, Name: "production", ActiveReleaseId: relID,
		Service: spec, EffectiveReplicas: eff,
	})
}

// setInflight pushes a heartbeat carrying the env's total in-flight count.
func setInflight(live *livestate.Registry, clk *clock.Fake, inflight uint32) {
	live.Heartbeat("n1", &clusterv1.Heartbeat{
		Proxy: map[string]*clusterv1.ProxySample{envID: {Inflight: inflight}},
	})
}

func TestServerless(t *testing.T) {
	// desired = ceil(inflight / max_concurrency), clamped to [floor, max].
	cases := []struct {
		name        string
		maxConc     uint32
		min, max    uint32
		scaleToZero bool
		inflight    uint32
		want        uint32
	}{
		{"idle_holds_min", 10, 1, 5, false, 0, 1},
		{"idle_scales_to_zero", 10, 0, 5, true, 0, 0},
		{"one_replica_per_cap", 10, 1, 5, false, 10, 1},
		{"rounds_up", 10, 1, 5, false, 11, 2},
		{"scales_with_load", 10, 1, 5, false, 35, 4},
		{"clamped_at_max", 10, 1, 5, false, 200, 5},
		{"cap_one_tracks_inflight", 1, 0, 8, true, 6, 6},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			s, rs, clk, live := newServerless(t)
			st := rs.State()
			addServerlessEnv(st, tc.maxConc, tc.min, tc.max, tc.min, tc.scaleToZero)
			setInflight(live, clk, tc.inflight)

			if err := s.evaluate(context.Background()); err != nil {
				t.Fatalf("evaluate: %v", err)
			}
			if got := effectiveOf(t, st); got != int(tc.want) {
				t.Fatalf("inflight=%d cap=%d → effective=%d, want %d", tc.inflight, tc.maxConc, got, tc.want)
			}
		})
	}
}

// TestServerlessFreezesWithoutData: no live proxy data → no scaling decision.
func TestServerlessFreezesWithoutData(t *testing.T) {
	s, rs, _, _ := newServerless(t)
	st := rs.State()
	addServerlessEnv(st, 10, 1, 5, 3, false) // effective=3, no heartbeats

	if err := s.evaluate(context.Background()); err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if got := effectiveOf(t, st); got != 3 {
		t.Fatalf("froze wrong: effective=%d, want 3 (unchanged)", got)
	}
}

// TestServerlessSkipsDuringDeployment: a running deployment owns placement.
func TestServerlessSkipsDuringDeployment(t *testing.T) {
	s, rs, clk, live := newServerless(t)
	st := rs.State()
	addServerlessEnv(st, 10, 1, 5, 2, false)
	st.PutDeployment(&zatterav1.Deployment{
		Meta: &zatterav1.Meta{Id: "d1"}, EnvironmentId: envID,
		Phase: zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_STARTING,
	})
	setInflight(live, clk, 100) // would otherwise scale to max

	if err := s.evaluate(context.Background()); err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if got := effectiveOf(t, st); got != 2 {
		t.Fatalf("scaled during a deployment: effective=%d, want 2", got)
	}
}
