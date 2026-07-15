package scheduler

import (
	"context"
	"io"
	"testing"
	"time"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/state"
)

// fakeStream replays a scripted list of build events, then io.EOF. A nil script
// blocks forever (used to exercise the builder-lost timeout).
type fakeStream struct {
	events []*clusterv1.BuildEvent
	i      int
	block  bool
}

func (f *fakeStream) Recv() (*clusterv1.BuildEvent, error) {
	if f.block {
		select {} // never returns
	}
	if f.i >= len(f.events) {
		return nil, io.EOF
	}
	ev := f.events[f.i]
	f.i++
	return ev, nil
}

type fakeDialer struct {
	stream    *fakeStream
	dialErr   error
	gotReq    *clusterv1.RunBuildRequest
	gotNodeID string
}

func (d *fakeDialer) RunBuild(_ context.Context, node *zatterav1.Node, req *clusterv1.RunBuildRequest) (BuildStream, error) {
	d.gotReq = req
	d.gotNodeID = node.GetMeta().GetId()
	if d.dialErr != nil {
		return nil, d.dialErr
	}
	return d.stream, nil
}

func seedBuilder(st *state.Store, id string) {
	st.PutNode(&zatterav1.Node{
		Meta:   &zatterav1.Meta{Id: id},
		Status: zatterav1.NodeStatus_NODE_STATUS_ALIVE,
		Labels: map[string]string{"builder": "true"},
		Roles:  []zatterav1.NodeRole{zatterav1.NodeRole_NODE_ROLE_WORKER},
	})
}

func seedBuild(st *state.Store, id string) {
	st.PutBuild(&zatterav1.Build{
		Meta:          &zatterav1.Meta{Id: id},
		AppId:         "app1",
		ProjectId:     "proj1",
		EnvironmentId: envID,
		Status:        zatterav1.BuildStatus_BUILD_STATUS_QUEUED,
		TarballDigest: "sha256:deadbeef",
	})
}

func waitBuild(t *testing.T, st *state.Store, id string, want zatterav1.BuildStatus) *zatterav1.Build {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if b, ok := st.Build(id); ok && b.GetStatus() == want {
			return b
		}
		time.Sleep(5 * time.Millisecond)
	}
	b, _ := st.Build(id)
	t.Fatalf("build %s did not reach %v (got %v)", id, want, b.GetStatus())
	return nil
}

func TestBuildsDispatchSucceeds(t *testing.T) {
	rs := raftstore.NewTestStore(t)
	st := rs.State()
	seedBuilder(st, "builder-a")
	seedBuild(st, "b1")

	dialer := &fakeDialer{stream: &fakeStream{events: []*clusterv1.BuildEvent{
		{Log: &zatterav1.LogLine{Line: "step 1/2"}},
		{Log: &zatterav1.LogLine{Line: "step 2/2"}},
		{Status: zatterav1.BuildStatus_BUILD_STATUS_SUCCEEDED, ImageDigest: "sha256:abc123"},
	}}}
	d := NewBuildDispatcher(rs, clock.Real{}, dialer, BuildDispatcherConfig{
		SourceURLBase: "https://ctrl:8443/internal/blobs/",
		RegistryAddr:  "ctrl:5000",
	}, nil)

	d.reconcile(context.Background())
	b := waitBuild(t, st, "b1", zatterav1.BuildStatus_BUILD_STATUS_SUCCEEDED)

	if b.GetNodeId() != "builder-a" {
		t.Errorf("build node = %q, want builder-a", b.GetNodeId())
	}
	if want := "ctrl:5000/proj1/app1@sha256:abc123"; b.GetImageRef() != want {
		t.Errorf("image_ref = %q, want %q", b.GetImageRef(), want)
	}
	// The request carried the source URL and push ref.
	if dialer.gotReq.GetSourceUrl() != "https://ctrl:8443/internal/blobs/sha256:deadbeef" {
		t.Errorf("source_url = %q", dialer.gotReq.GetSourceUrl())
	}
	if dialer.gotReq.GetPushImageRef() != "ctrl:5000/proj1/app1:b1" {
		t.Errorf("push_image_ref = %q", dialer.gotReq.GetPushImageRef())
	}
	if logs := d.BuildLog("b1"); len(logs) != 2 {
		t.Errorf("expected 2 buffered log lines, got %v", logs)
	}
}

func TestBuildsBuilderLost(t *testing.T) {
	rs := raftstore.NewTestStore(t)
	st := rs.State()
	seedBuilder(st, "builder-a")
	seedBuild(st, "b1")

	dialer := &fakeDialer{stream: &fakeStream{block: true}}
	d := NewBuildDispatcher(rs, clock.Real{}, dialer, BuildDispatcherConfig{RegistryAddr: "ctrl:5000"}, nil)
	d.SetBuilderTimeout(50 * time.Millisecond)

	d.reconcile(context.Background())
	b := waitBuild(t, st, "b1", zatterav1.BuildStatus_BUILD_STATUS_FAILED)
	if b.GetError() != "builder lost" {
		t.Errorf("error = %q, want 'builder lost'", b.GetError())
	}
}

func TestBuildsNoBuilderLeavesQueued(t *testing.T) {
	rs := raftstore.NewTestStore(t)
	st := rs.State()
	seedBuild(st, "b1") // no builder node

	d := NewBuildDispatcher(rs, clock.Real{}, &fakeDialer{}, BuildDispatcherConfig{}, nil)
	d.reconcile(context.Background())

	// Give the async dispatch a moment; the build stays QUEUED.
	time.Sleep(50 * time.Millisecond)
	b, _ := st.Build("b1")
	if b.GetStatus() != zatterav1.BuildStatus_BUILD_STATUS_QUEUED {
		t.Fatalf("build should stay QUEUED without a builder, got %v", b.GetStatus())
	}
}

// TestBuildsDeploymentGating checks the orchestrator BUILDING gate: a source
// deployment waits in BUILDING until its build succeeds, then advances with the
// built image stamped onto the release.
func TestBuildsDeploymentGating(t *testing.T) {
	rs := raftstore.NewTestStore(t)
	st := rs.State()
	o := NewOrchestrator(rs, clock.NewFake(), nil)
	ctx := context.Background()

	spec := &zatterav1.ServiceSpec{Replicas: &zatterav1.ReplicaRange{Min: 1, Max: 3}}
	st.PutEnvironment(&zatterav1.Environment{Meta: &zatterav1.Meta{Id: envID}, Name: "production", Service: spec})
	st.PutRelease(&zatterav1.Release{Meta: &zatterav1.Meta{Id: relID}, EnvironmentId: envID, Service: spec}) // empty image
	st.PutBuild(&zatterav1.Build{Meta: &zatterav1.Meta{Id: "b1"}, EnvironmentId: envID, Status: zatterav1.BuildStatus_BUILD_STATUS_RUNNING})
	st.PutDeployment(&zatterav1.Deployment{
		Meta:          &zatterav1.Meta{Id: "d1"},
		EnvironmentId: envID,
		ReleaseId:     relID,
		BuildId:       "b1",
		Phase:         zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PENDING,
	})

	// PENDING → BUILDING (build not done yet).
	dep, _ := st.Deployment("d1")
	if err := o.reconcile(ctx, dep); err != nil {
		t.Fatal(err)
	}
	dep, _ = st.Deployment("d1")
	if dep.GetPhase() != zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_BUILDING {
		t.Fatalf("phase = %v, want BUILDING", dep.GetPhase())
	}

	// Still building → stays put.
	if err := o.reconcile(ctx, dep); err != nil {
		t.Fatal(err)
	}
	dep, _ = st.Deployment("d1")
	if dep.GetPhase() != zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_BUILDING {
		t.Fatalf("phase = %v, want still BUILDING", dep.GetPhase())
	}

	// Build succeeds → BUILDING stamps the image + platforms and advances to
	// PLACING.
	b, _ := st.Build("b1")
	b.Status = zatterav1.BuildStatus_BUILD_STATUS_SUCCEEDED
	b.ImageRef = "ctrl:5000/proj1/app1@sha256:abc123"
	b.Platforms = []string{"linux/arm64"}
	st.PutBuild(b)
	if err := o.reconcile(ctx, dep); err != nil {
		t.Fatal(err)
	}
	dep, _ = st.Deployment("d1")
	if dep.GetPhase() != zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PLACING {
		t.Fatalf("phase = %v, want PLACING", dep.GetPhase())
	}
	rel, _ := st.Release(relID)
	if rel.GetImageRef() != "ctrl:5000/proj1/app1@sha256:abc123" {
		t.Fatalf("release image not stamped: %q", rel.GetImageRef())
	}
	if p := rel.GetPlatforms(); len(p) != 1 || p[0] != "linux/arm64" {
		t.Fatalf("release platforms not copied from the build: %v", p)
	}
}

// TestBuildsDeploymentBuildFailed checks a failed build fails the deployment.
func TestBuildsDeploymentBuildFailed(t *testing.T) {
	rs := raftstore.NewTestStore(t)
	st := rs.State()
	o := NewOrchestrator(rs, clock.NewFake(), nil)
	ctx := context.Background()

	spec := &zatterav1.ServiceSpec{Replicas: &zatterav1.ReplicaRange{Min: 1, Max: 3}}
	st.PutEnvironment(&zatterav1.Environment{Meta: &zatterav1.Meta{Id: envID}, Name: "production", Service: spec})
	st.PutRelease(&zatterav1.Release{Meta: &zatterav1.Meta{Id: relID}, EnvironmentId: envID, Service: spec})
	st.PutBuild(&zatterav1.Build{Meta: &zatterav1.Meta{Id: "b1"}, EnvironmentId: envID, Status: zatterav1.BuildStatus_BUILD_STATUS_FAILED, Error: "compile error"})
	st.PutDeployment(&zatterav1.Deployment{
		Meta: &zatterav1.Meta{Id: "d1"}, EnvironmentId: envID, ReleaseId: relID, BuildId: "b1",
		Phase: zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_BUILDING,
	})

	dep, _ := st.Deployment("d1")
	if err := o.reconcile(ctx, dep); err != nil {
		t.Fatal(err)
	}
	dep, _ = st.Deployment("d1")
	if dep.GetPhase() != zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_FAILED {
		t.Fatalf("phase = %v, want FAILED", dep.GetPhase())
	}
}
