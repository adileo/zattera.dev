package api

import (
	"context"
	"regexp"

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

// dnsNameRe matches a DNS-safe project/app/env name: lowercase alphanumeric and
// hyphens, 1–40 chars, no leading/trailing hyphen.
var dnsNameRe = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?$`)

func validDNSName(name string) bool { return dnsNameRe.MatchString(name) }

// ProjectServer implements zatterav1.ProjectServiceServer.
type ProjectServer struct {
	zatterav1.UnimplementedProjectServiceServer
	store *state.Store
	raft  Applier
	clock clock.Clock
	rbac  *RBAC
}

// NewProjectServer builds the project service.
func NewProjectServer(store *state.Store, raft Applier, clk clock.Clock, rbac *RBAC) *ProjectServer {
	return &ProjectServer{store: store, raft: raft, clock: clk, rbac: rbac}
}

// CreateProject creates a project; the creator becomes its OWNER member.
func (s *ProjectServer) CreateProject(ctx context.Context, req *zatterav1.CreateProjectRequest) (*zatterav1.Project, error) {
	id, ok := IdentityFrom(ctx)
	if !ok || id.UserID == "" {
		return nil, status.Error(codes.Unauthenticated, "a user identity is required")
	}
	if !validDNSName(req.GetName()) {
		return nil, status.Error(codes.InvalidArgument, "name must be DNS-safe: [a-z0-9-], 1-40 chars, no leading/trailing hyphen")
	}
	if _, exists := s.store.ProjectByName(req.GetName()); exists {
		return nil, status.Errorf(codes.AlreadyExists, "project %q already exists", req.GetName())
	}
	org, _ := s.store.Org()
	now := s.clock.Now()
	proj := &zatterav1.Project{
		Meta:  newMeta(ids.New(), now),
		OrgId: org.GetMeta().GetId(),
		Name:  req.GetName(),
	}
	if err := s.apply(ctx, id.UserID, &clusterv1.Command{Mutation: &clusterv1.Command_PutProject{PutProject: &clusterv1.PutProject{Project: proj}}}); err != nil {
		return nil, toStatus(err)
	}
	member := &zatterav1.ProjectMember{ProjectId: proj.GetMeta().GetId(), UserId: id.UserID, Role: zatterav1.Role_ROLE_OWNER}
	if err := s.apply(ctx, id.UserID, &clusterv1.Command{Mutation: &clusterv1.Command_PutProjectMember{PutProjectMember: &clusterv1.PutProjectMember{Member: member}}}); err != nil {
		return nil, toStatus(err)
	}
	return proj, nil
}

// ListProjects lists the caller's projects (all, for org admins).
func (s *ProjectServer) ListProjects(ctx context.Context, _ *emptypb.Empty) (*zatterav1.ListProjectsResponse, error) {
	id, ok := IdentityFrom(ctx)
	if !ok || id.UserID == "" {
		return nil, status.Error(codes.Unauthenticated, "a user identity is required")
	}
	if s.rbac.isOrgAdmin(id.UserID) {
		return &zatterav1.ListProjectsResponse{Projects: s.store.ListProjects()}, nil
	}
	var out []*zatterav1.Project
	for _, m := range s.store.ListMembershipsOfUser(id.UserID) {
		if p, ok := s.store.Project(m.GetProjectId()); ok {
			out = append(out, p)
		}
	}
	return &zatterav1.ListProjectsResponse{Projects: out}, nil
}

// GetProject returns one project (project_id resolved by RBAC).
func (s *ProjectServer) GetProject(_ context.Context, req *zatterav1.GetProjectRequest) (*zatterav1.Project, error) {
	p, ok := s.store.Project(req.GetProjectId())
	if !ok {
		return nil, status.Error(codes.NotFound, "project not found")
	}
	return p, nil
}

// DeleteProject cascades: apps, environments (+ env vars), domains, volumes,
// then the project itself.
func (s *ProjectServer) DeleteProject(ctx context.Context, req *zatterav1.DeleteProjectRequest) (*emptypb.Empty, error) {
	pid := req.GetProjectId()
	if _, ok := s.store.Project(pid); !ok {
		return nil, status.Error(codes.NotFound, "project not found")
	}
	id, _ := IdentityFrom(ctx)

	// TODO(T-27): stop running instances first; the scheduler reconciles
	// assignments away once environments vanish.
	for _, env := range s.store.ListEnvironments(pid, "") {
		if err := s.apply(ctx, id.UserID, &clusterv1.Command{Mutation: &clusterv1.Command_DeleteEnvironment{DeleteEnvironment: &clusterv1.DeleteByID{Id: env.GetMeta().GetId()}}}); err != nil {
			return nil, toStatus(err)
		}
	}
	for _, app := range s.store.ListApps(pid) {
		if err := s.apply(ctx, id.UserID, &clusterv1.Command{Mutation: &clusterv1.Command_DeleteApp{DeleteApp: &clusterv1.DeleteByID{Id: app.GetMeta().GetId()}}}); err != nil {
			return nil, toStatus(err)
		}
	}
	for _, d := range s.store.ListDomains(pid) {
		if err := s.apply(ctx, id.UserID, &clusterv1.Command{Mutation: &clusterv1.Command_DeleteDomain{DeleteDomain: &clusterv1.DeleteByID{Id: d.GetMeta().GetId()}}}); err != nil {
			return nil, toStatus(err)
		}
	}
	for _, v := range s.store.ListVolumes(pid) {
		if err := s.apply(ctx, id.UserID, &clusterv1.Command{Mutation: &clusterv1.Command_DeleteVolume{DeleteVolume: &clusterv1.DeleteByID{Id: v.GetMeta().GetId()}}}); err != nil {
			return nil, toStatus(err)
		}
	}
	if err := s.apply(ctx, id.UserID, &clusterv1.Command{Mutation: &clusterv1.Command_DeleteProject{DeleteProject: &clusterv1.DeleteByID{Id: pid}}}); err != nil {
		return nil, toStatus(err)
	}
	return &emptypb.Empty{}, nil
}

// AddMember resolves user_email → user and grants a project role.
func (s *ProjectServer) AddMember(ctx context.Context, req *zatterav1.AddMemberRequest) (*zatterav1.ProjectMember, error) {
	user, ok := s.store.UserByEmail(req.GetUserEmail())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "user %q not found", req.GetUserEmail())
	}
	role := req.GetRole()
	if role == zatterav1.Role_ROLE_UNSPECIFIED {
		role = zatterav1.Role_ROLE_DEVELOPER
	}
	id, _ := IdentityFrom(ctx)
	member := &zatterav1.ProjectMember{ProjectId: req.GetProjectId(), UserId: user.GetMeta().GetId(), Role: role}
	if err := s.apply(ctx, id.UserID, &clusterv1.Command{Mutation: &clusterv1.Command_PutProjectMember{PutProjectMember: &clusterv1.PutProjectMember{Member: member}}}); err != nil {
		return nil, toStatus(err)
	}
	return member, nil
}

// RemoveMember revokes a member's project access.
func (s *ProjectServer) RemoveMember(ctx context.Context, req *zatterav1.RemoveMemberRequest) (*emptypb.Empty, error) {
	if _, ok := s.store.ProjectMember(req.GetProjectId(), req.GetUserId()); !ok {
		return nil, status.Error(codes.NotFound, "member not found")
	}
	id, _ := IdentityFrom(ctx)
	cmd := &clusterv1.Command{Mutation: &clusterv1.Command_DeleteProjectMember{DeleteProjectMember: &clusterv1.DeleteProjectMember{
		ProjectId: req.GetProjectId(), UserId: req.GetUserId(),
	}}}
	if err := s.apply(ctx, id.UserID, cmd); err != nil {
		return nil, toStatus(err)
	}
	return &emptypb.Empty{}, nil
}

// ListMembers lists a project's members.
func (s *ProjectServer) ListMembers(_ context.Context, req *zatterav1.ListMembersRequest) (*zatterav1.ListMembersResponse, error) {
	return &zatterav1.ListMembersResponse{Members: s.store.ListProjectMembers(req.GetProjectId())}, nil
}

func (s *ProjectServer) apply(ctx context.Context, actorUser string, cmd *clusterv1.Command) error {
	cmd.RequestId = ids.New()
	cmd.Actor = "user:" + actorUser
	cmd.Time = timestamppb.Now()
	return s.raft.Apply(ctx, cmd)
}
