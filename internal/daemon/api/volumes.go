package api

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

// VolumeServer implements the CRUD subset of VolumeService (T-62). Snapshot,
// restore and file operations land in later tasks (T-64/T-65/T-77).
type VolumeServer struct {
	zatterav1.UnimplementedVolumeServiceServer
	store *state.Store
	raft  Applier
	clock clock.Clock
}

// NewVolumeServer builds the volume service.
func NewVolumeServer(store *state.Store, raft Applier, clk clock.Clock) *VolumeServer {
	if clk == nil {
		clk = clock.Real{}
	}
	return &VolumeServer{store: store, raft: raft, clock: clk}
}

// CreateVolume creates a named volume pinned to a node. When node_id is empty
// the least-used ALIVE worker is chosen. Names are unique within (project, env).
func (s *VolumeServer) CreateVolume(ctx context.Context, req *zatterav1.CreateVolumeRequest) (*zatterav1.Volume, error) {
	if !validDNSName(req.GetName()) {
		return nil, status.Error(codes.InvalidArgument, "name must be DNS-safe: [a-z0-9-], 1-40 chars")
	}
	env, ok := s.store.Environment(req.GetEnvironmentId())
	if !ok || env.GetProjectId() != req.GetProjectId() {
		return nil, status.Errorf(codes.NotFound, "environment %q not found", req.GetEnvironmentId())
	}
	if _, exists := s.store.VolumeByName(req.GetProjectId(), req.GetEnvironmentId(), req.GetName()); exists {
		return nil, status.Errorf(codes.AlreadyExists, "volume %q already exists in this environment", req.GetName())
	}

	node := req.GetNodeId()
	if node == "" {
		node = leastUsedVolumeNode(s.store)
	} else if _, ok := s.store.Node(node); !ok {
		return nil, status.Errorf(codes.InvalidArgument, "node %q not found", node)
	}
	if node == "" {
		return nil, status.Error(codes.FailedPrecondition, "no schedulable node available for the volume")
	}

	now := timestamppb.New(s.clock.Now())
	v := &zatterav1.Volume{
		Meta:           &zatterav1.Meta{Id: ids.New(), CreatedAt: now, UpdatedAt: now},
		ProjectId:      req.GetProjectId(),
		EnvironmentId:  req.GetEnvironmentId(),
		Name:           req.GetName(),
		NodeId:         node,
		Status:         zatterav1.VolumeStatus_VOLUME_STATUS_ACTIVE,
		SnapshotPolicy: req.GetSnapshotPolicy(),
	}
	if err := s.apply(ctx, &clusterv1.Command{
		Mutation: &clusterv1.Command_PutVolume{PutVolume: &clusterv1.PutVolume{Volume: v}},
	}); err != nil {
		return nil, toStatus(err)
	}
	return v, nil
}

// ListVolumes returns the project's volumes.
func (s *VolumeServer) ListVolumes(_ context.Context, req *zatterav1.ListVolumesRequest) (*zatterav1.ListVolumesResponse, error) {
	return &zatterav1.ListVolumesResponse{Volumes: s.store.ListVolumes(req.GetProjectId())}, nil
}

// DeleteVolume removes a volume. It refuses while the volume is mounted (a live
// fencing lease or a running instance on its node).
func (s *VolumeServer) DeleteVolume(ctx context.Context, req *zatterav1.DeleteVolumeRequest) (*emptypb.Empty, error) {
	v, ok := s.store.Volume(req.GetVolumeId())
	if !ok || v.GetProjectId() != req.GetProjectId() {
		return nil, status.Errorf(codes.NotFound, "volume %q not found", req.GetVolumeId())
	}
	if s.volumeMounted(v) {
		return nil, status.Error(codes.FailedPrecondition, "volume is in use; stop the service before deleting it")
	}
	if err := s.apply(ctx, &clusterv1.Command{
		Mutation: &clusterv1.Command_DeleteVolume{DeleteVolume: &clusterv1.DeleteByID{Id: req.GetVolumeId()}},
	}); err != nil {
		return nil, toStatus(err)
	}
	return &emptypb.Empty{}, nil
}

// volumeMounted reports whether the volume is currently in use: an unexpired
// fencing lease, or a running (non-job) instance on its pinned node.
func (s *VolumeServer) volumeMounted(v *zatterav1.Volume) bool {
	if l := v.GetLease(); l != nil && l.GetExpiresAt() != nil && s.clock.Now().Before(l.GetExpiresAt().AsTime()) {
		return true
	}
	for _, a := range s.store.ListAssignments(v.GetEnvironmentId()) {
		if a.GetJobId() == "" &&
			a.GetDesired() == zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN &&
			a.GetNodeId() == v.GetNodeId() {
			return true
		}
	}
	return false
}

func (s *VolumeServer) apply(ctx context.Context, cmd *clusterv1.Command) error {
	cmd.RequestId = ids.New()
	id, _ := IdentityFrom(ctx)
	cmd.Actor = "user:" + id.UserID
	cmd.Time = timestamppb.New(s.clock.Now())
	return s.raft.Apply(ctx, cmd)
}

// leastUsedVolumeNode picks the ALIVE schedulable worker hosting the fewest
// volumes (ties broken by node id). Shared shape with the scheduler's picker.
func leastUsedVolumeNode(st *state.Store) string {
	counts := map[string]int{}
	for _, v := range st.ListVolumes("") {
		counts[v.GetNodeId()]++
	}
	best, bestCount := "", 0
	for _, n := range st.ListNodes() {
		id := n.GetMeta().GetId()
		if n.GetStatus() != zatterav1.NodeStatus_NODE_STATUS_ALIVE || !n.GetSchedulable() {
			continue
		}
		c := counts[id]
		if best == "" || c < bestCount || (c == bestCount && id < best) {
			best, bestCount = id, c
		}
	}
	return best
}
