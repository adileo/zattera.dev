package scheduler

import (
	"context"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

const (
	// jobRetryBase is the first retry delay; it doubles per attempt up to the cap.
	jobRetryBase = 5 * time.Second
	jobRetryMax  = 5 * time.Minute
)

// reconcileJobs drives one-shot jobs (T-53): it places QUEUED/RETRYING jobs onto
// nodes as assignments carrying job_id, observes their outcome, and marks
// SUCCEEDED or FAILED — retrying with exponential backoff up to max_retries.
// Terminal/canceled jobs get their assignment reaped.
func (s *Scheduler) reconcileJobs(ctx context.Context, st *state.Store) error {
	for _, job := range st.ListJobs("", "") {
		if err := s.reconcileJob(ctx, st, job); err != nil {
			return err
		}
	}
	return nil
}

func (s *Scheduler) reconcileJob(ctx context.Context, st *state.Store, job *zatterav1.Job) error {
	switch job.GetStatus() {
	case zatterav1.JobStatus_JOB_STATUS_QUEUED, zatterav1.JobStatus_JOB_STATUS_RETRYING:
		return s.placeJob(ctx, st, job)
	case zatterav1.JobStatus_JOB_STATUS_RUNNING:
		return s.observeJob(ctx, st, job)
	default:
		// SUCCEEDED / FAILED / CANCELED: ensure the assignment is torn down.
		return s.reapJobAssignment(ctx, st, job)
	}
}

// placeJob assigns a QUEUED/RETRYING job to a node and marks it RUNNING. It
// honors the retry backoff and re-queues (no state change) when capacity or a
// release is unavailable.
func (s *Scheduler) placeJob(ctx context.Context, st *state.Store, job *zatterav1.Job) error {
	if job.GetStatus() == zatterav1.JobStatus_JOB_STATUS_RETRYING {
		if fin := job.GetFinishedAt(); fin != nil {
			ready := fin.AsTime().Add(jobBackoff(job.GetAttempt()))
			if s.clock.Now().Before(ready) {
				return nil // still backing off
			}
		}
	}

	env, ok := st.Environment(job.GetEnvironmentId())
	if !ok {
		return s.finishJob(ctx, job, zatterav1.JobStatus_JOB_STATUS_FAILED, 0, "environment not found")
	}
	rel, ok := s.jobRelease(st, job, env)
	if !ok {
		return nil // no release/image yet; stay queued and retry next tick
	}

	nodes, err := Place(st, rel, env.GetMeta().GetId(), 1, nil)
	if err != nil || len(nodes) == 0 {
		s.emitEvent(ctx, env, "job.no_capacity", "warning", "cannot place job %s: %v", job.GetMeta().GetId(), err)
		return nil // stay queued; the loop retries
	}

	now := timestamppb.New(s.clock.Now())
	attempt := job.GetAttempt() + 1
	a := &zatterav1.Assignment{
		Meta:          &zatterav1.Meta{Id: ids.New(), CreatedAt: now, UpdatedAt: now},
		NodeId:        nodes[0],
		ProjectId:     job.GetProjectId(),
		AppId:         job.GetAppId(),
		EnvironmentId: env.GetMeta().GetId(),
		ReleaseId:     rel.GetMeta().GetId(),
		Desired:       zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN,
		ConfigHash:    rel.GetConfigHash(),
		JobId:         job.GetMeta().GetId(),
	}
	if err := s.apply(ctx, &clusterv1.Command{
		Mutation: &clusterv1.Command_PutAssignments{PutAssignments: &clusterv1.PutAssignments{Assignments: []*zatterav1.Assignment{a}}},
	}); err != nil {
		return err
	}

	job.Status = zatterav1.JobStatus_JOB_STATUS_RUNNING
	job.NodeId = nodes[0]
	job.ReleaseId = rel.GetMeta().GetId()
	job.Attempt = attempt
	job.Error = ""
	job.FinishedAt = nil
	if job.GetStartedAt() == nil {
		job.StartedAt = now
	}
	return s.putJob(ctx, job)
}

// observeJob inspects the running job's assignment and advances the job when its
// container has exited.
func (s *Scheduler) observeJob(ctx context.Context, st *state.Store, job *zatterav1.Job) error {
	a, ok := s.jobAssignment(st, job)
	if !ok {
		// Assignment vanished mid-run (node lost / reaped): treat as a failure.
		return s.jobAttemptFailed(ctx, job, -1, "job assignment lost")
	}
	obs := a.GetObserved()
	switch obs.GetState() {
	case zatterav1.InstanceState_INSTANCE_STATE_STOPPED:
		if obs.GetExitCode() == 0 {
			return s.jobSucceeded(ctx, job, a)
		}
		return s.jobAttemptFailedWith(ctx, job, a, obs.GetExitCode(), obs.GetMessage())
	case zatterav1.InstanceState_INSTANCE_STATE_FAILED:
		return s.jobAttemptFailedWith(ctx, job, a, obs.GetExitCode(), obs.GetMessage())
	default:
		return nil // still pending/running
	}
}

func (s *Scheduler) jobSucceeded(ctx context.Context, job *zatterav1.Job, a *zatterav1.Assignment) error {
	if err := s.deleteAssignment(ctx, a); err != nil {
		return err
	}
	job.Status = zatterav1.JobStatus_JOB_STATUS_SUCCEEDED
	job.ExitCode = 0
	job.Error = ""
	job.FinishedAt = timestamppb.New(s.clock.Now())
	return s.putJob(ctx, job)
}

// jobAttemptFailedWith reaps the failed assignment then applies retry/fail logic.
func (s *Scheduler) jobAttemptFailedWith(ctx context.Context, job *zatterav1.Job, a *zatterav1.Assignment, code int32, msg string) error {
	if err := s.deleteAssignment(ctx, a); err != nil {
		return err
	}
	return s.jobAttemptFailed(ctx, job, code, msg)
}

// jobAttemptFailed records a failed attempt: RETRYING if retries remain
// (attempt <= max_retries), otherwise FAILED.
func (s *Scheduler) jobAttemptFailed(ctx context.Context, job *zatterav1.Job, code int32, msg string) error {
	now := timestamppb.New(s.clock.Now())
	job.ExitCode = code
	job.Error = msg
	job.FinishedAt = now
	if job.GetAttempt() <= job.GetMaxRetries() {
		job.Status = zatterav1.JobStatus_JOB_STATUS_RETRYING
	} else {
		job.Status = zatterav1.JobStatus_JOB_STATUS_FAILED
	}
	return s.putJob(ctx, job)
}

// finishJob forces a terminal status (used for unrecoverable placement errors).
func (s *Scheduler) finishJob(ctx context.Context, job *zatterav1.Job, status zatterav1.JobStatus, code int32, msg string) error {
	job.Status = status
	job.ExitCode = code
	job.Error = msg
	job.FinishedAt = timestamppb.New(s.clock.Now())
	return s.putJob(ctx, job)
}

// reapJobAssignment stops+deletes any lingering assignment for a terminal or
// canceled job (canceled jobs are stopped by the API; this reaps them).
func (s *Scheduler) reapJobAssignment(ctx context.Context, st *state.Store, job *zatterav1.Job) error {
	a, ok := s.jobAssignment(st, job)
	if !ok {
		return nil
	}
	return s.deleteAssignment(ctx, a)
}

func (s *Scheduler) deleteAssignment(ctx context.Context, a *zatterav1.Assignment) error {
	return s.apply(ctx, &clusterv1.Command{
		Mutation: &clusterv1.Command_DeleteAssignments{DeleteAssignments: &clusterv1.DeleteAssignments{AssignmentIds: []string{a.GetMeta().GetId()}}},
	})
}

func (s *Scheduler) putJob(ctx context.Context, job *zatterav1.Job) error {
	job.GetMeta().UpdatedAt = timestamppb.New(s.clock.Now())
	return s.apply(ctx, &clusterv1.Command{
		Mutation: &clusterv1.Command_PutJob{PutJob: &clusterv1.PutJob{Job: job}},
	})
}

// jobAssignment finds the (single) assignment carrying this job's id.
func (s *Scheduler) jobAssignment(st *state.Store, job *zatterav1.Job) (*zatterav1.Assignment, bool) {
	for _, a := range st.ListAssignments(job.GetEnvironmentId()) {
		if a.GetJobId() == job.GetMeta().GetId() {
			return a, true
		}
	}
	return nil, false
}

// jobRelease resolves the image source: the job's pinned release, else the
// environment's active release at run time.
func (s *Scheduler) jobRelease(st *state.Store, job *zatterav1.Job, env *zatterav1.Environment) (*zatterav1.Release, bool) {
	if id := job.GetReleaseId(); id != "" {
		return st.Release(id)
	}
	if id := env.GetActiveReleaseId(); id != "" {
		return st.Release(id)
	}
	return nil, false
}

// jobBackoff is the retry delay for a given (failed) attempt: exponential from
// jobRetryBase, capped at jobRetryMax.
func jobBackoff(attempt uint32) time.Duration {
	d := jobRetryBase
	for i := uint32(1); i < attempt; i++ {
		d *= 2
		if d >= jobRetryMax {
			return jobRetryMax
		}
	}
	return d
}
