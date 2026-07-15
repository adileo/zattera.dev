package api

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

func TestDeploy(t *testing.T) {
	const depEnvID = "env-deploy"
	rs := raftstore.NewTestStore(t)
	st := rs.State()
	s := NewDeployServer(st, rs, clock.NewFake(), t.TempDir())
	st.PutEnvironment(&zatterav1.Environment{
		Meta:      &zatterav1.Meta{Id: depEnvID},
		ProjectId: "p1",
		AppId:     "a1",
		Name:      "production",
		Service:   &zatterav1.ServiceSpec{Replicas: &zatterav1.ReplicaRange{Min: 1}},
	})
	ctx := context.Background()

	// promote simulates the orchestrator finishing a deployment and switching
	// the active release.
	promote := func(t *testing.T, dep *zatterav1.Deployment) {
		t.Helper()
		d, _ := st.Deployment(dep.GetMeta().GetId())
		d.Phase = zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_SUCCEEDED
		st.PutDeployment(d)
		env, _ := st.Environment(depEnvID)
		env.ActiveReleaseId = dep.GetReleaseId()
		st.PutEnvironment(env)
	}

	var dep1, dep2 *zatterav1.Deployment

	t.Run("deploy creates release v1 and a PENDING deployment", func(t *testing.T) {
		var err error
		dep1, err = s.Deploy(ctx, &zatterav1.DeployRequest{EnvironmentId: depEnvID, ImageRef: "nginx:1"})
		if err != nil {
			t.Fatalf("deploy: %v", err)
		}
		if dep1.GetPhase() != zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PENDING {
			t.Fatalf("phase = %v, want PENDING", dep1.GetPhase())
		}
		if dep1.GetPreviousReleaseId() != "" || dep1.GetIsRollback() {
			t.Fatalf("first deploy should have no previous release and not be a rollback: %+v", dep1)
		}
		rels := st.ListReleases(depEnvID)
		if len(rels) != 1 || rels[0].GetVersion() != 1 || rels[0].GetImageRef() != "nginx:1" {
			t.Fatalf("expected release v1 nginx:1, got %+v", rels)
		}
		if rels[0].GetConfigHash() == "" || rels[0].GetService() == nil {
			t.Fatalf("release must carry a config hash + frozen spec: %+v", rels[0])
		}
	})

	t.Run("a second deploy while one is in progress is rejected", func(t *testing.T) {
		_, err := s.Deploy(ctx, &zatterav1.DeployRequest{EnvironmentId: depEnvID, ImageRef: "nginx:2"})
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("concurrent deploy should 409, got %v", err)
		}
	})

	t.Run("after the first completes, a new deploy is release v2 with previous set", func(t *testing.T) {
		promote(t, dep1)
		var err error
		dep2, err = s.Deploy(ctx, &zatterav1.DeployRequest{EnvironmentId: depEnvID, ImageRef: "nginx:2"})
		if err != nil {
			t.Fatalf("deploy v2: %v", err)
		}
		if dep2.GetPreviousReleaseId() != dep1.GetReleaseId() {
			t.Fatalf("v2 previous_release_id = %q, want %q", dep2.GetPreviousReleaseId(), dep1.GetReleaseId())
		}
		rel, _ := st.Release(dep2.GetReleaseId())
		if rel.GetVersion() != 2 {
			t.Fatalf("second release version = %d, want 2", rel.GetVersion())
		}
	})

	t.Run("rollback defaults to the previous release", func(t *testing.T) {
		promote(t, dep2) // active is now v2
		dep, err := s.Rollback(ctx, &zatterav1.RollbackRequest{EnvironmentId: depEnvID})
		if err != nil {
			t.Fatalf("rollback: %v", err)
		}
		if !dep.GetIsRollback() {
			t.Fatal("rollback deployment must set is_rollback")
		}
		if dep.GetReleaseId() != dep1.GetReleaseId() {
			t.Fatalf("rollback target = %q, want v1 release %q", dep.GetReleaseId(), dep1.GetReleaseId())
		}
		// No new release is minted on rollback.
		if got := len(st.ListReleases(depEnvID)); got != 2 {
			t.Fatalf("rollback should not mint a release, have %d", got)
		}
	})

	t.Run("get, list and instances", func(t *testing.T) {
		got, err := s.GetDeployment(ctx, &zatterav1.GetDeploymentRequest{DeploymentId: dep1.GetMeta().GetId()})
		if err != nil || got.GetMeta().GetId() != dep1.GetMeta().GetId() {
			t.Fatalf("get deployment: %v", err)
		}
		if _, err := s.GetDeployment(ctx, &zatterav1.GetDeploymentRequest{DeploymentId: "nope"}); status.Code(err) != codes.NotFound {
			t.Fatalf("missing deployment should be NotFound, got %v", err)
		}
		deps, _ := s.ListDeployments(ctx, &zatterav1.ListDeploymentsRequest{EnvironmentId: depEnvID})
		if len(deps.GetDeployments()) < 3 {
			t.Fatalf("expected at least 3 deployments, got %d", len(deps.GetDeployments()))
		}

		st.PutAssignment(&zatterav1.Assignment{
			Meta: &zatterav1.Meta{Id: "asg1"}, EnvironmentId: depEnvID, AppId: "a1",
			Desired: zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN,
		})
		inst, _ := s.ListInstances(ctx, &zatterav1.ListInstancesRequest{EnvironmentId: depEnvID, AppId: "a1"})
		if len(inst.GetInstances()) != 1 {
			t.Fatalf("expected 1 instance, got %d", len(inst.GetInstances()))
		}

		// WatchDeployment sends the current state immediately; the fake cancels
		// its context on that first send so the watch loop returns.
		wctx, wcancel := context.WithCancel(ctx)
		fw := &fakeDeployWatch{ctx: wctx, cancel: wcancel}
		if err := s.WatchDeployment(&zatterav1.GetDeploymentRequest{DeploymentId: dep1.GetMeta().GetId()}, fw); err != nil {
			t.Fatalf("watch: %v", err)
		}
		if fw.got == nil || fw.got.GetMeta().GetId() != dep1.GetMeta().GetId() {
			t.Fatal("watch should send the current deployment first")
		}
	})

	t.Run("deploy without image or build is rejected", func(t *testing.T) {
		// Use a fresh env with no in-flight deployment.
		st.PutEnvironment(&zatterav1.Environment{Meta: &zatterav1.Meta{Id: "env-2"}, Service: &zatterav1.ServiceSpec{}})
		if _, err := s.Deploy(ctx, &zatterav1.DeployRequest{EnvironmentId: "env-2"}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("deploy without image_ref/build_id should be InvalidArgument, got %v", err)
		}
		if _, err := s.Deploy(ctx, &zatterav1.DeployRequest{EnvironmentId: "ghost", ImageRef: "x"}); status.Code(err) != codes.NotFound {
			t.Fatalf("deploy to unknown env should be NotFound, got %v", err)
		}
	})
}

// TestDeployPlatforms covers T-88's deploy-time platform resolution: platforms
// are frozen into the release from the resolver (image-ref deploys) or the
// build record, and inspection failures never fail a deploy.
func TestDeployPlatforms(t *testing.T) {
	rs := raftstore.NewTestStore(t)
	st := rs.State()
	s := NewDeployServer(st, rs, clock.NewFake(), t.TempDir())
	ctx := context.Background()

	newEnv := func(id string) {
		st.PutEnvironment(&zatterav1.Environment{
			Meta: &zatterav1.Meta{Id: id}, ProjectId: "p1", AppId: "a1",
			Service: &zatterav1.ServiceSpec{Replicas: &zatterav1.ReplicaRange{Min: 1}},
		})
	}
	releasePlatforms := func(t *testing.T, dep *zatterav1.Deployment) []string {
		t.Helper()
		rel, ok := st.Release(dep.GetReleaseId())
		if !ok {
			t.Fatal("release not found")
		}
		return rel.GetPlatforms()
	}

	t.Run("image-ref deploy freezes the resolver's platforms", func(t *testing.T) {
		newEnv("env-idx")
		s.Platforms = func(_ context.Context, ref string) []string {
			if ref != "reg:5000/p1/a1:v1" {
				t.Fatalf("resolver got ref %q", ref)
			}
			return []string{"linux/amd64", "linux/arm64"}
		}
		dep, err := s.Deploy(ctx, &zatterav1.DeployRequest{EnvironmentId: "env-idx", ImageRef: "reg:5000/p1/a1:v1"})
		if err != nil {
			t.Fatalf("deploy: %v", err)
		}
		if got := releasePlatforms(t, dep); len(got) != 2 || got[0] != "linux/amd64" || got[1] != "linux/arm64" {
			t.Fatalf("release platforms = %v", got)
		}
	})

	t.Run("resolver failure leaves platforms empty and the deploy succeeds", func(t *testing.T) {
		newEnv("env-fail")
		s.Platforms = func(context.Context, string) []string { return nil } // inspect failed
		dep, err := s.Deploy(ctx, &zatterav1.DeployRequest{EnvironmentId: "env-fail", ImageRef: "private.example.com/app:1"})
		if err != nil {
			t.Fatalf("deploy must not fail over inspection: %v", err)
		}
		if got := releasePlatforms(t, dep); len(got) != 0 {
			t.Fatalf("platforms should be empty (unconstrained), got %v", got)
		}
	})

	t.Run("nil resolver leaves platforms empty (regression)", func(t *testing.T) {
		newEnv("env-nil")
		s.Platforms = nil
		dep, err := s.Deploy(ctx, &zatterav1.DeployRequest{EnvironmentId: "env-nil", ImageRef: "nginx:1"})
		if err != nil {
			t.Fatalf("deploy: %v", err)
		}
		if got := releasePlatforms(t, dep); len(got) != 0 {
			t.Fatalf("platforms should be empty, got %v", got)
		}
	})

	t.Run("build-id deploy takes the build's platforms, not the resolver", func(t *testing.T) {
		newEnv("env-build")
		s.Platforms = func(context.Context, string) []string {
			t.Fatal("resolver must not run for build deploys")
			return nil
		}
		st.PutBuild(&zatterav1.Build{
			Meta:     &zatterav1.Meta{Id: "b1"},
			Status:   zatterav1.BuildStatus_BUILD_STATUS_SUCCEEDED,
			ImageRef: "reg:5000/p1/a1@sha256:abc",
			Platforms: []string{
				"linux/arm64",
			},
		})
		dep, err := s.Deploy(ctx, &zatterav1.DeployRequest{EnvironmentId: "env-build", BuildId: "b1"})
		if err != nil {
			t.Fatalf("deploy: %v", err)
		}
		if got := releasePlatforms(t, dep); len(got) != 1 || got[0] != "linux/arm64" {
			t.Fatalf("release platforms = %v, want the build's [linux/arm64]", got)
		}
	})
}

// fakeDeployWatch captures the first streamed deployment and cancels its context
// so WatchDeployment's loop returns.
type fakeDeployWatch struct {
	grpc.ServerStream
	ctx    context.Context
	cancel context.CancelFunc
	got    *zatterav1.Deployment
}

func (f *fakeDeployWatch) Send(d *zatterav1.Deployment) error {
	f.got = d
	f.cancel()
	return nil
}

func (f *fakeDeployWatch) Context() context.Context { return f.ctx }
