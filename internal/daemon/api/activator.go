package api

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

// activationDebounce coalesces repeated Activate calls for the same env: once a
// wake is issued, further calls within this window are accepted without
// re-applying (the scheduler is already bringing replicas up).
const activationDebounce = 10 * time.Second

// ActivatorServer wakes a scaled-to-zero environment on demand (T-70). Proxies
// call Activate when a request arrives for an env with no healthy endpoints; it
// sets effective_replicas back to max(1, min) so the scheduler places a replica.
// Idempotent and singleflighted per env.
type ActivatorServer struct {
	clusterv1.UnimplementedActivatorServiceServer

	store *state.Store
	raft  Applier
	clk   clock.Clock
	log   *slog.Logger

	mu       sync.Mutex
	lastWake map[string]time.Time
}

// NewActivatorServer builds the activator.
func NewActivatorServer(store *state.Store, raft Applier, clk clock.Clock, log *slog.Logger) *ActivatorServer {
	if log == nil {
		log = slog.Default()
	}
	if clk == nil {
		clk = clock.Real{}
	}
	return &ActivatorServer{store: store, raft: raft, clk: clk, log: log, lastWake: map[string]time.Time{}}
}

// Activate brings a scaled-to-zero env back up to its warm replica count. It is
// idempotent: an already-warm env, or one woken moments ago, returns accepted
// without a redundant write.
func (s *ActivatorServer) Activate(ctx context.Context, req *clusterv1.ActivateRequest) (*clusterv1.ActivateResponse, error) {
	env, ok := s.store.Environment(req.GetEnvironmentId())
	if !ok {
		return nil, status.Error(codes.NotFound, "environment not found")
	}
	if !env.GetService().GetScaleToZero() {
		// Not a scale-to-zero env: nothing to wake.
		return &clusterv1.ActivateResponse{Accepted: false}, nil
	}

	target := env.GetService().GetReplicas().GetMin()
	if target < 1 {
		target = 1
	}
	// Already warm: idempotent success.
	if env.GetEffectiveReplicas() >= target {
		return &clusterv1.ActivateResponse{Accepted: true}, nil
	}

	// Singleflight/debounce: skip a redundant apply while a recent wake is still
	// being converged by the scheduler.
	s.mu.Lock()
	if t, ok := s.lastWake[req.GetEnvironmentId()]; ok && s.clk.Now().Sub(t) < activationDebounce {
		s.mu.Unlock()
		return &clusterv1.ActivateResponse{Accepted: true}, nil
	}
	s.mu.Unlock()

	cur := proto.Clone(env).(*zatterav1.Environment)
	cur.EffectiveReplicas = target
	cur.GetMeta().UpdatedAt = timestamppb.New(s.clk.Now())
	cmd := &clusterv1.Command{
		RequestId: ids.New(),
		Actor:     "system:activator",
		Time:      timestamppb.New(s.clk.Now()),
		Mutation:  &clusterv1.Command_PutEnvironment{PutEnvironment: &clusterv1.PutEnvironment{Environment: cur}},
	}
	if err := s.raft.Apply(ctx, cmd); err != nil {
		return nil, toStatus(err)
	}
	s.mu.Lock()
	s.lastWake[req.GetEnvironmentId()] = s.clk.Now()
	s.mu.Unlock()
	s.log.Info("activated env", "env", req.GetEnvironmentId(), "to", target, "by_node", req.GetNodeId())
	return &clusterv1.ActivateResponse{Accepted: true}, nil
}
