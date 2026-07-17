package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/leaderrunner"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

// serverlessTick is the (tight) concurrency re-evaluation cadence: request-rate
// driven scaling reacts faster than the resource autoscaler.
const serverlessTick = 5 * time.Second

// Serverless autoscales max_concurrency environments off in-flight request
// counts (T-71): desired = ceil(total_inflight / max_concurrency), clamped to
// [floor, max] where floor is 0 for scale_to_zero envs (idle cools to zero, the
// activator wakes them) else replicas.min. It owns these envs instead of the
// resource autoscaler (T-61) and the scale-to-zero loop (T-69).
type Serverless struct {
	store *raftstore.Store
	live  LiveView
	clock clock.Clock
	log   *slog.Logger
}

// NewServerless builds the loop.
func NewServerless(store *raftstore.Store, live LiveView, clk clock.Clock, log *slog.Logger) *Serverless {
	if log == nil {
		log = slog.Default()
	}
	if clk == nil {
		clk = clock.Real{}
	}
	return &Serverless{store: store, live: live, clock: clk, log: log}
}

// Run evaluates while this node leads.
func (s *Serverless) Run(ctx context.Context) {
	leaderrunner.Run(ctx, s.store, s.clock, s.leaderLoop)
}

func (s *Serverless) leaderLoop(ctx context.Context) {
	tick := s.clock.NewTicker(serverlessTick)
	defer tick.Stop()
	for {
		if err := s.evaluate(ctx); errors.Is(err, raftstore.ErrNotLeader) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-s.store.LeaderCh():
			if !s.store.IsLeader() {
				return
			}
		case <-tick.C():
		}
	}
}

func (s *Serverless) evaluate(ctx context.Context) error {
	if !s.store.IsLeader() {
		return raftstore.ErrNotLeader
	}
	st := s.store.State()
	for _, env := range st.ListEnvironments("", "") {
		if err := s.evaluateEnv(ctx, st, env); err != nil {
			return err
		}
	}
	return nil
}

// evaluateEnv sets one env's replica count from its in-flight load.
func (s *Serverless) evaluateEnv(ctx context.Context, st *state.Store, env *zatterav1.Environment) error {
	spec := env.GetService()
	maxConc := spec.GetMaxConcurrency()
	if maxConc == 0 || env.GetActiveReleaseId() == "" {
		return nil // not a serverless env
	}
	if s.deploymentActive(st, env.GetMeta().GetId()) {
		return nil // a deployment owns placement; don't fight it
	}

	inflight, ok := s.totalInflight(env.GetMeta().GetId())
	if !ok {
		return nil // no live proxy data → freeze rather than scale on nothing
	}

	floor := int(spec.GetReplicas().GetMin())
	if spec.GetScaleToZero() {
		floor = 0
	} else if floor < 1 {
		floor = 1
	}
	maxRep := int(spec.GetReplicas().GetMax())
	if maxRep < floor {
		maxRep = floor
	}

	desired := int(math.Ceil(float64(inflight) / float64(maxConc)))
	if desired < floor {
		desired = floor
	}
	if desired > maxRep {
		desired = maxRep
	}

	if desired == desiredReplicas(env) {
		return nil
	}
	return s.applyScale(ctx, st, env, desired)
}

// totalInflight sums the env's in-flight requests across live proxies. ok is
// false when no live node is reporting (freeze).
func (s *Serverless) totalInflight(envID string) (uint32, bool) {
	var total uint32
	var have bool
	for _, ns := range s.live.Snapshot() {
		hb := ns.Heartbeat
		if hb == nil {
			continue
		}
		have = true
		if ps, ok := hb.GetProxy()[envID]; ok {
			total += ps.GetInflight()
		}
	}
	return total, have
}

// applyScale writes effective_replicas (re-reading to avoid clobbering).
func (s *Serverless) applyScale(ctx context.Context, st *state.Store, env *zatterav1.Environment, to int) error {
	cur, ok := st.Environment(env.GetMeta().GetId())
	if !ok {
		return nil
	}
	from := desiredReplicas(cur)
	cur = proto.Clone(cur).(*zatterav1.Environment)
	cur.EffectiveReplicas = uint32(to)
	cur.GetMeta().UpdatedAt = timestamppb.New(s.clock.Now())
	if err := s.apply(ctx, &clusterv1.Command{
		Mutation: &clusterv1.Command_PutEnvironment{PutEnvironment: &clusterv1.PutEnvironment{Environment: cur}},
	}); err != nil {
		return err
	}
	s.log.Info("serverless scaled env", "env", env.GetMeta().GetId(), "from", from, "to", to)
	return nil
}

func (s *Serverless) deploymentActive(st *state.Store, envID string) bool {
	for _, d := range st.ListDeployments(envID) {
		if !isTerminalPhase(d.GetPhase()) {
			return true
		}
	}
	return false
}

func (s *Serverless) apply(ctx context.Context, cmd *clusterv1.Command) error {
	cmd.RequestId = ids.New()
	cmd.Actor = "system:serverless"
	cmd.Time = timestamppb.New(s.clock.Now())
	err := s.store.Apply(ctx, cmd)
	if errors.Is(err, raftstore.ErrNotLeader) {
		return err
	}
	if err != nil {
		s.log.Warn("serverless apply failed", "err", err)
	}
	return nil
}
