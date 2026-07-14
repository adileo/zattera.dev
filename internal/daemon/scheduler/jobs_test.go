package scheduler

import (
	"context"
	"testing"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

// jobHarness builds a scheduler with a fake clock the test controls.
func jobHarness(t *testing.T) (*Scheduler, *state.Store, *clock.Fake) {
	t.Helper()
	rs := raftstore.NewTestStore(t)
	fake := clock.NewFake()
	s := New(rs, fake, nil)
	st := rs.State()
	return s, st, fake
}

// seedJobEnv registers a node, a release and an environment for jobs.
func seedJobEnv(st *state.Store) {
	st.PutNode(&zatterav1.Node{
		Meta: &zatterav1.Meta{Id: "jn1"}, Status: zatterav1.NodeStatus_NODE_STATUS_ALIVE,
		Schedulable: true, Roles: []zatterav1.NodeRole{zatterav1.NodeRole_NODE_ROLE_WORKER},
	})
	spec := &zatterav1.ServiceSpec{Replicas: &zatterav1.ReplicaRange{Min: 1, Max: 3}}
	st.PutRelease(&zatterav1.Release{
		Meta: &zatterav1.Meta{Id: "reljob"}, EnvironmentId: "ejob", ConfigHash: "h1", Service: spec,
	})
	st.PutEnvironment(&zatterav1.Environment{
		Meta: &zatterav1.Meta{Id: "ejob"}, ProjectId: "p1", AppId: "app1",
		Name: "production", ActiveReleaseId: "reljob", Service: spec,
	})
}

func queueJob(st *state.Store, id string, maxRetries uint32) {
	st.PutJob(&zatterav1.Job{
		Meta: &zatterav1.Meta{Id: id}, ProjectId: "p1", AppId: "app1", EnvironmentId: "ejob",
		Command: "echo hi", Status: zatterav1.JobStatus_JOB_STATUS_QUEUED, MaxRetries: maxRetries,
	})
}

// observeJobAssignment sets the observed state of the job's live assignment.
func observeJobAssignment(t *testing.T, st *state.Store, jobID string, stateVal zatterav1.InstanceState, code int32) {
	t.Helper()
	for _, a := range st.ListAssignments("ejob") {
		if a.GetJobId() == jobID {
			a.Observed = &zatterav1.AssignmentObserved{State: stateVal, ExitCode: code, ContainerId: "c1"}
			st.PutAssignment(a)
			return
		}
	}
	t.Fatalf("no assignment for job %s", jobID)
}

func mustReconcileJobs(t *testing.T, s *Scheduler) {
	t.Helper()
	if err := s.reconcileJobs(context.Background(), s.store.State()); err != nil {
		t.Fatalf("reconcileJobs: %v", err)
	}
}

func TestJobsRunSucceeds(t *testing.T) {
	s, st, _ := jobHarness(t)
	seedJobEnv(st)
	queueJob(st, "job1", 0)

	// Place it → RUNNING with an assignment carrying job_id.
	mustReconcileJobs(t, s)
	job, _ := st.Job("job1")
	if job.GetStatus() != zatterav1.JobStatus_JOB_STATUS_RUNNING {
		t.Fatalf("status = %v, want RUNNING", job.GetStatus())
	}
	if job.GetAttempt() != 1 || job.GetNodeId() != "jn1" {
		t.Fatalf("attempt=%d node=%q", job.GetAttempt(), job.GetNodeId())
	}
	var jobAssigns int
	for _, a := range st.ListAssignments("ejob") {
		if a.GetJobId() == "job1" {
			jobAssigns++
		}
	}
	if jobAssigns != 1 {
		t.Fatalf("job assignments = %d, want 1", jobAssigns)
	}

	// Agent reports a clean exit → SUCCEEDED and the assignment is reaped.
	observeJobAssignment(t, st, "job1", zatterav1.InstanceState_INSTANCE_STATE_STOPPED, 0)
	mustReconcileJobs(t, s)
	job, _ = st.Job("job1")
	if job.GetStatus() != zatterav1.JobStatus_JOB_STATUS_SUCCEEDED {
		t.Fatalf("status = %v, want SUCCEEDED", job.GetStatus())
	}
	if job.GetExitCode() != 0 || job.GetFinishedAt() == nil {
		t.Fatalf("exit=%d finished=%v", job.GetExitCode(), job.GetFinishedAt())
	}
	for _, a := range st.ListAssignments("ejob") {
		if a.GetJobId() == "job1" {
			t.Fatal("job assignment should be reaped after success")
		}
	}
}

func TestJobsRetryThenFail(t *testing.T) {
	s, st, fake := jobHarness(t)
	seedJobEnv(st)
	queueJob(st, "job2", 2) // 1 initial + 2 retries = 3 attempts

	// Attempt 1: place, fail.
	mustReconcileJobs(t, s)
	observeJobAssignment(t, st, "job2", zatterav1.InstanceState_INSTANCE_STATE_FAILED, 3)
	mustReconcileJobs(t, s)
	if j, _ := st.Job("job2"); j.GetStatus() != zatterav1.JobStatus_JOB_STATUS_RETRYING {
		t.Fatalf("after attempt 1: %v, want RETRYING", j.GetStatus())
	}

	// Backoff gate: without advancing the clock, no new placement happens.
	mustReconcileJobs(t, s)
	if j, _ := st.Job("job2"); j.GetStatus() != zatterav1.JobStatus_JOB_STATUS_RETRYING || j.GetAttempt() != 1 {
		t.Fatalf("backoff not honored: status=%v attempt=%d", j.GetStatus(), j.GetAttempt())
	}

	// Attempt 2 after backoff.
	fake.Advance(jobBackoff(1))
	mustReconcileJobs(t, s)
	if j, _ := st.Job("job2"); j.GetStatus() != zatterav1.JobStatus_JOB_STATUS_RUNNING || j.GetAttempt() != 2 {
		t.Fatalf("attempt 2 not placed: status=%v attempt=%d", j.GetStatus(), j.GetAttempt())
	}
	observeJobAssignment(t, st, "job2", zatterav1.InstanceState_INSTANCE_STATE_FAILED, 3)
	mustReconcileJobs(t, s)

	// Attempt 3 (last) after backoff, then fail → FAILED terminal.
	fake.Advance(jobBackoff(2))
	mustReconcileJobs(t, s)
	if j, _ := st.Job("job2"); j.GetAttempt() != 3 {
		t.Fatalf("attempt 3 not placed: attempt=%d", j.GetAttempt())
	}
	observeJobAssignment(t, st, "job2", zatterav1.InstanceState_INSTANCE_STATE_FAILED, 3)
	mustReconcileJobs(t, s)

	j, _ := st.Job("job2")
	if j.GetStatus() != zatterav1.JobStatus_JOB_STATUS_FAILED {
		t.Fatalf("final status = %v, want FAILED", j.GetStatus())
	}
	if j.GetExitCode() != 3 {
		t.Fatalf("exit code = %d, want 3", j.GetExitCode())
	}
	for _, a := range st.ListAssignments("ejob") {
		if a.GetJobId() == "job2" {
			t.Fatal("failed job assignment should be reaped")
		}
	}
}

func TestJobsCancel(t *testing.T) {
	s, st, _ := jobHarness(t)
	seedJobEnv(st)
	queueJob(st, "job3", 0)
	mustReconcileJobs(t, s)

	// Simulate the API cancel: mark CANCELED + stop the assignment.
	job, _ := st.Job("job3")
	job.Status = zatterav1.JobStatus_JOB_STATUS_CANCELED
	st.PutJob(job)
	for _, a := range st.ListAssignments("ejob") {
		if a.GetJobId() == "job3" {
			a.Desired = zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_STOP
			st.PutAssignment(a)
		}
	}

	// Scheduler reaps the canceled job's assignment.
	mustReconcileJobs(t, s)
	for _, a := range st.ListAssignments("ejob") {
		if a.GetJobId() == "job3" {
			t.Fatal("canceled job assignment should be reaped")
		}
	}
	if j, _ := st.Job("job3"); j.GetStatus() != zatterav1.JobStatus_JOB_STATUS_CANCELED {
		t.Fatalf("status = %v, want CANCELED", j.GetStatus())
	}
}

// TestJobsIgnoredByReplicaMath asserts the T-23 evaluator does not count a job
// assignment as a service replica.
func TestJobsIgnoredByReplicaMath(t *testing.T) {
	s, st, _ := jobHarness(t)
	seedJobEnv(st) // env min=1

	// A pre-existing job assignment (RUN) that must not be mistaken for the
	// service replica.
	st.PutAssignment(&zatterav1.Assignment{
		Meta: &zatterav1.Meta{Id: ids.New()}, NodeId: "jn1", ProjectId: "p1", AppId: "app1",
		EnvironmentId: "ejob", ReleaseId: "reljob", JobId: "jobX",
		Desired:    zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN,
		ConfigHash: "h1",
	})

	env, _ := st.Environment("ejob")
	if err := s.evaluateEnv(context.Background(), st, env); err != nil {
		t.Fatalf("evaluateEnv: %v", err)
	}

	// The scheduler must have placed exactly one *service* replica (job_id == "").
	var svc int
	for _, a := range st.ListAssignments("ejob") {
		if a.GetJobId() == "" && a.GetDesired() == zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN {
			svc++
		}
	}
	if svc != 1 {
		t.Fatalf("service replicas = %d, want 1 (job assignment must not count)", svc)
	}
}
