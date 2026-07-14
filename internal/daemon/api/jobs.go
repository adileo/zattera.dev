package api

import (
	"context"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

// JobServer implements JobService: it enqueues one-shot jobs (T-53). The
// scheduler (reconcileJobs) owns placement, retries and terminal transitions;
// this service only creates work and reports it.
type JobServer struct {
	zatterav1.UnimplementedJobServiceServer
	store *state.Store
	raft  Applier
	clock clock.Clock
}

// NewJobServer builds the job service.
func NewJobServer(store *state.Store, raft Applier, clk clock.Clock) *JobServer {
	if clk == nil {
		clk = clock.Real{}
	}
	return &JobServer{store: store, raft: raft, clock: clk}
}

// RunJob enqueues a one-shot job against an environment's active release.
func (s *JobServer) RunJob(ctx context.Context, req *zatterav1.RunJobRequest) (*zatterav1.Job, error) {
	if strings.TrimSpace(req.GetCommand()) == "" {
		return nil, status.Error(codes.InvalidArgument, "command is required")
	}
	env, ok := s.store.Environment(req.GetEnvironmentId())
	if !ok {
		return nil, status.Error(codes.NotFound, "environment not found")
	}
	if env.GetActiveReleaseId() == "" {
		return nil, status.Error(codes.FailedPrecondition, "environment has no active release to source an image from")
	}
	id, _ := IdentityFrom(ctx)
	now := timestamppb.New(s.clock.Now())
	job := &zatterav1.Job{
		Meta:            &zatterav1.Meta{Id: ids.New(), CreatedAt: now, UpdatedAt: now},
		ProjectId:       env.GetProjectId(),
		AppId:           env.GetAppId(),
		EnvironmentId:   env.GetMeta().GetId(),
		ReleaseId:       env.GetActiveReleaseId(),
		Command:         req.GetCommand(),
		Status:          zatterav1.JobStatus_JOB_STATUS_QUEUED,
		MaxRetries:      req.GetMaxRetries(),
		CreatedByUserId: id.UserID,
	}
	if err := s.apply(ctx, &clusterv1.Command{
		Mutation: &clusterv1.Command_PutJob{PutJob: &clusterv1.PutJob{Job: job}},
	}); err != nil {
		return nil, err
	}
	return job, nil
}

// GetJob returns one job by id.
func (s *JobServer) GetJob(_ context.Context, req *zatterav1.GetJobRequest) (*zatterav1.Job, error) {
	job, ok := s.store.Job(req.GetJobId())
	if !ok || (req.GetProjectId() != "" && job.GetProjectId() != req.GetProjectId()) {
		return nil, status.Error(codes.NotFound, "job not found")
	}
	return job, nil
}

// ListJobs lists jobs for a project, optionally filtered by env/cron.
func (s *JobServer) ListJobs(_ context.Context, req *zatterav1.ListJobsRequest) (*zatterav1.ListJobsResponse, error) {
	var out []*zatterav1.Job
	for _, j := range s.store.ListJobs(req.GetProjectId(), req.GetEnvironmentId()) {
		if req.GetCronName() != "" && j.GetCronName() != req.GetCronName() {
			continue
		}
		out = append(out, j)
	}
	return &zatterav1.ListJobsResponse{Jobs: out}, nil
}

// CancelJob marks a non-terminal job CANCELED and stops its assignment; the
// scheduler reaps the assignment. Terminal jobs are returned unchanged.
func (s *JobServer) CancelJob(ctx context.Context, req *zatterav1.CancelJobRequest) (*zatterav1.Job, error) {
	job, ok := s.store.Job(req.GetJobId())
	if !ok || (req.GetProjectId() != "" && job.GetProjectId() != req.GetProjectId()) {
		return nil, status.Error(codes.NotFound, "job not found")
	}
	if jobTerminal(job.GetStatus()) {
		return job, nil
	}

	// Stop the running assignment (if any) so the agent tears the container down.
	if a, ok := s.jobAssignment(job); ok && a.GetDesired() != zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_STOP {
		a.Desired = zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_STOP
		if err := s.apply(ctx, &clusterv1.Command{
			Mutation: &clusterv1.Command_PutAssignments{PutAssignments: &clusterv1.PutAssignments{Assignments: []*zatterav1.Assignment{a}}},
		}); err != nil {
			return nil, err
		}
	}

	job.Status = zatterav1.JobStatus_JOB_STATUS_CANCELED
	job.FinishedAt = timestamppb.New(s.clock.Now())
	job.GetMeta().UpdatedAt = job.GetFinishedAt()
	if err := s.apply(ctx, &clusterv1.Command{
		Mutation: &clusterv1.Command_PutJob{PutJob: &clusterv1.PutJob{Job: job}},
	}); err != nil {
		return nil, err
	}
	return job, nil
}

func (s *JobServer) jobAssignment(job *zatterav1.Job) (*zatterav1.Assignment, bool) {
	for _, a := range s.store.ListAssignments(job.GetEnvironmentId()) {
		if a.GetJobId() == job.GetMeta().GetId() {
			return a, true
		}
	}
	return nil, false
}

func (s *JobServer) apply(ctx context.Context, cmd *clusterv1.Command) error {
	id, _ := IdentityFrom(ctx)
	cmd.RequestId = ids.New()
	cmd.Actor = id.Actor()
	cmd.Time = timestamppb.Now()
	return toStatus(s.raft.Apply(ctx, cmd))
}

// jobTerminal reports whether a job status is finished.
func jobTerminal(s zatterav1.JobStatus) bool {
	switch s {
	case zatterav1.JobStatus_JOB_STATUS_SUCCEEDED,
		zatterav1.JobStatus_JOB_STATUS_FAILED,
		zatterav1.JobStatus_JOB_STATUS_CANCELED:
		return true
	default:
		return false
	}
}
