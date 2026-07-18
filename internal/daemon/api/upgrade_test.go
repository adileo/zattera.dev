package api

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/daemon/upgrade"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
)

// fakeResolver returns a fixed release.
type fakeResolver struct {
	rel upgrade.Release
	err error
}

func (f fakeResolver) Resolve(context.Context, string) (upgrade.Release, error) {
	if f.err != nil {
		return upgrade.Release{}, f.err
	}
	return f.rel, nil
}

// fakeUpgradeDialer records which nodes were asked to upgrade and with what.
type fakeUpgradeDialer struct {
	calls []*clusterv1.AgentUpgradeRequest
	nodes []string
	err   error
}

func (f *fakeUpgradeDialer) UpgradeBinary(_ context.Context, node *zatterav1.Node, req *clusterv1.AgentUpgradeRequest) (*clusterv1.AgentUpgradeResponse, error) {
	f.nodes = append(f.nodes, node.GetName())
	f.calls = append(f.calls, req)
	if f.err != nil {
		return nil, f.err
	}
	return &clusterv1.AgentUpgradeResponse{PreviousVersion: node.GetBinaryVersion(), StagedVersion: req.GetVersion()}, nil
}

// leaderApplier reports a fixed leader node id via the LeaderAddr interface the
// upgrade planner probes for.
type leaderApplier struct {
	*raftstore.Store
	leader string
}

func (l leaderApplier) LeaderAddr() (string, string) { return "", l.leader }

func testRelease(version string) upgrade.Release {
	return upgrade.Release{
		Version: version,
		Assets: map[string]upgrade.Asset{
			"linux/amd64": {URL: "https://example.test/releases/download/" + version + "/zattera-linux-amd64", SHA256: strings.Repeat("a", 64)},
			"linux/arm64": {URL: "https://example.test/releases/download/" + version + "/zattera-linux-arm64", SHA256: strings.Repeat("b", 64)},
		},
	}
}

// upgradeHarness builds a 4-node cluster: two workers, a control follower and
// the control leader.
func upgradeHarness(t *testing.T) (*NodeServer, *fakeUpgradeDialer, map[string]string) {
	t.Helper()
	rs := raftstore.NewTestStore(t)
	st := rs.State()

	ids4 := map[string]string{}
	add := func(name, ver string, roles []zatterav1.NodeRole) {
		id := ids.New()
		ids4[name] = id
		st.PutNode(&zatterav1.Node{
			Meta: &zatterav1.Meta{Id: id}, Name: name, Roles: roles,
			Status: zatterav1.NodeStatus_NODE_STATUS_ALIVE, Schedulable: true,
			OsArch: "linux/amd64", BinaryVersion: ver,
		})
	}
	worker := []zatterav1.NodeRole{zatterav1.NodeRole_NODE_ROLE_WORKER}
	control := []zatterav1.NodeRole{zatterav1.NodeRole_NODE_ROLE_CONTROL}
	add("worker-a", "v0.3.0", worker)
	add("worker-b", "v0.3.0", worker)
	add("control-follower", "v0.3.0", control)
	add("control-leader", "v0.3.0", control)

	dialer := &fakeUpgradeDialer{}
	srv := NewNodeServer(st, leaderApplier{Store: rs, leader: ids4["control-leader"]}, clock.NewFake(), nil)
	srv.SetUpgrader(fakeResolver{rel: testRelease("v0.4.0")}, dialer)
	return srv, dialer, ids4
}

// TestUpgradePlanOrder is the correctness property of the whole feature: the
// raft leader must come last. The FSM is additive-only and an unknown mutation
// is an error that does NOT halt the node, so a new leader proposing a mutation
// an old follower cannot apply diverges that follower silently.
func TestUpgradePlanOrder(t *testing.T) {
	srv, _, _ := upgradeHarness(t)

	plan, err := srv.UpgradePlan(context.Background(), &zatterav1.UpgradePlanRequest{})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if plan.GetTargetVersion() != "v0.4.0" {
		t.Fatalf("target = %q", plan.GetTargetVersion())
	}
	if len(plan.GetSteps()) != 4 {
		t.Fatalf("plan has %d steps, want 4", len(plan.GetSteps()))
	}
	last := plan.GetSteps()[len(plan.GetSteps())-1]
	if !last.GetLeader() || last.GetNodeName() != "control-leader" {
		t.Fatalf("last step is %s (leader=%v), want the leader last", last.GetNodeName(), last.GetLeader())
	}
	for _, s := range plan.GetSteps()[:len(plan.GetSteps())-1] {
		if s.GetLeader() {
			t.Errorf("leader %s scheduled before the end", s.GetNodeName())
		}
		if s.GetUpToDate() {
			t.Errorf("node %s on %s marked up to date", s.GetNodeName(), s.GetCurrentVersion())
		}
	}
}

// TestUpgradePlanSkipsAndWarns covers the preflight signals an operator needs
// before starting.
func TestUpgradePlanSkipsAndWarns(t *testing.T) {
	srv, _, ids4 := upgradeHarness(t)

	// One node is already on the target; one is DOWN; one has an arch the
	// release does not publish.
	up, _ := srv.store.Node(ids4["worker-a"])
	up.BinaryVersion = "v0.4.0"
	srv.store.PutNode(up)

	down, _ := srv.store.Node(ids4["worker-b"])
	down.Status = zatterav1.NodeStatus_NODE_STATUS_DOWN
	srv.store.PutNode(down)

	odd, _ := srv.store.Node(ids4["control-follower"])
	odd.OsArch = "linux/riscv64"
	srv.store.PutNode(odd)

	plan, err := srv.UpgradePlan(context.Background(), &zatterav1.UpgradePlanRequest{})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	byName := map[string]*zatterav1.UpgradeStep{}
	for _, s := range plan.GetSteps() {
		byName[s.GetNodeName()] = s
	}
	if s, ok := byName["worker-a"]; !ok || !s.GetUpToDate() {
		t.Errorf("node already on the target not marked up-to-date: %+v", s)
	}
	if _, ok := byName["worker-b"]; ok {
		t.Error("a DOWN node was included in the plan")
	}
	if _, ok := byName["control-follower"]; ok {
		t.Error("a node with no published asset was included in the plan")
	}

	joined := strings.Join(plan.GetWarnings(), "\n")
	for _, want := range []string{"worker-b", "DOWN", "riscv64"} {
		if !strings.Contains(joined, want) {
			t.Errorf("warnings missing %q:\n%s", want, joined)
		}
	}
}

// TestUpgradePlanSingleIngressWarning: with one ingress node the restart is a
// real, unavoidable blip — say so rather than promise zero downtime.
func TestUpgradePlanSingleIngressWarning(t *testing.T) {
	rs := raftstore.NewTestStore(t)
	st := rs.State()
	id := ids.New()
	st.PutNode(&zatterav1.Node{
		Meta: &zatterav1.Meta{Id: id}, Name: "solo",
		Roles:  []zatterav1.NodeRole{zatterav1.NodeRole_NODE_ROLE_CONTROL, zatterav1.NodeRole_NODE_ROLE_WORKER},
		Status: zatterav1.NodeStatus_NODE_STATUS_ALIVE, OsArch: "linux/amd64", BinaryVersion: "v0.3.0",
	})
	srv := NewNodeServer(st, leaderApplier{Store: rs, leader: id}, clock.NewFake(), nil)
	srv.SetUpgrader(fakeResolver{rel: testRelease("v0.4.0")}, &fakeUpgradeDialer{})

	plan, err := srv.UpgradePlan(context.Background(), &zatterav1.UpgradePlanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(plan.GetWarnings(), "\n"), "one node serves ingress") {
		t.Errorf("no ingress warning for a single-node cluster: %v", plan.GetWarnings())
	}
}

// TestUpgradeNode checks the per-node step: the node's arch picks the asset and
// the checksum travels with it.
func TestUpgradeNode(t *testing.T) {
	srv, dialer, ids4 := upgradeHarness(t)
	ctx := context.Background()

	arm, _ := srv.store.Node(ids4["worker-b"])
	arm.OsArch = "linux/arm64"
	srv.store.PutNode(arm)

	if _, err := srv.UpgradeNode(ctx, &zatterav1.UpgradeNodeRequest{NodeId: ids4["worker-b"]}); err != nil {
		t.Fatalf("upgrade: %v", err)
	}
	if len(dialer.calls) != 1 {
		t.Fatalf("dialer called %d times", len(dialer.calls))
	}
	call := dialer.calls[0]
	if !strings.HasSuffix(call.GetUrl(), "zattera-linux-arm64") {
		t.Errorf("wrong asset for the node's arch: %s", call.GetUrl())
	}
	if call.GetSha256() != strings.Repeat("b", 64) {
		t.Errorf("checksum not passed through: %q", call.GetSha256())
	}
	if call.GetVersion() != "v0.4.0" {
		t.Errorf("version = %q", call.GetVersion())
	}

	t.Run("down node is refused", func(t *testing.T) {
		n, _ := srv.store.Node(ids4["worker-a"])
		n.Status = zatterav1.NodeStatus_NODE_STATUS_DOWN
		srv.store.PutNode(n)
		_, err := srv.UpgradeNode(ctx, &zatterav1.UpgradeNodeRequest{NodeId: ids4["worker-a"]})
		if status.Code(err) != codes.FailedPrecondition {
			t.Errorf("upgrade of a DOWN node = %v, want FailedPrecondition", err)
		}
	})

	t.Run("unpublished arch is refused", func(t *testing.T) {
		n, _ := srv.store.Node(ids4["control-follower"])
		n.OsArch = "linux/riscv64"
		srv.store.PutNode(n)
		_, err := srv.UpgradeNode(ctx, &zatterav1.UpgradeNodeRequest{NodeId: ids4["control-follower"]})
		if status.Code(err) != codes.FailedPrecondition {
			t.Errorf("upgrade with no asset = %v, want FailedPrecondition", err)
		}
	})

	t.Run("node failure surfaces", func(t *testing.T) {
		dialer.err = errors.New("checksum mismatch")
		defer func() { dialer.err = nil }()
		_, err := srv.UpgradeNode(ctx, &zatterav1.UpgradeNodeRequest{NodeId: ids4["control-leader"]})
		if err == nil || !strings.Contains(err.Error(), "checksum") {
			t.Errorf("node error not surfaced: %v", err)
		}
	})
}

// TestUpgradeUnavailable: a node without the upgrader wired reports
// Unimplemented rather than panicking.
func TestUpgradeUnavailable(t *testing.T) {
	rs := raftstore.NewTestStore(t)
	srv := NewNodeServer(rs.State(), rs, clock.NewFake(), nil)
	if _, err := srv.UpgradePlan(context.Background(), &zatterav1.UpgradePlanRequest{}); status.Code(err) != codes.Unimplemented {
		t.Errorf("plan without an upgrader = %v", err)
	}
	if _, err := srv.UpgradeNode(context.Background(), &zatterav1.UpgradeNodeRequest{NodeId: "x"}); status.Code(err) != codes.Unimplemented {
		t.Errorf("upgrade without an upgrader = %v", err)
	}
}

// TestCordonUncordon covers T-94: cordon must not disturb running work, and
// uncordon must bring a node back — the thing that was impossible before.
func TestCordonUncordon(t *testing.T) {
	rs := raftstore.NewTestStore(t)
	st := rs.State()
	id := ids.New()
	st.PutNode(&zatterav1.Node{
		Meta: &zatterav1.Meta{Id: id}, Name: "n1",
		Status: zatterav1.NodeStatus_NODE_STATUS_ALIVE, Schedulable: true,
	})
	// An assignment on the node; cordon must leave it alone.
	assignID := ids.New()
	st.PutAssignment(&zatterav1.Assignment{
		Meta: &zatterav1.Meta{Id: assignID}, NodeId: id,
		Desired: zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN,
	})
	srv := NewNodeServer(st, rs, clock.NewFake(), nil)
	ctx := context.Background()

	n, err := srv.CordonNode(ctx, &zatterav1.CordonNodeRequest{NodeId: id})
	if err != nil {
		t.Fatalf("cordon: %v", err)
	}
	if n.GetSchedulable() {
		t.Error("cordoned node is still schedulable")
	}
	if n.GetStatus() != zatterav1.NodeStatus_NODE_STATUS_ALIVE {
		t.Errorf("cordon changed status to %v; it must stay ALIVE so its work keeps serving", n.GetStatus())
	}
	if a, ok := st.Assignment(assignID); !ok || a.GetDesired() != zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN {
		t.Error("cordon disturbed a running assignment — that is drain's job, not cordon's")
	}

	n, err = srv.UncordonNode(ctx, &zatterav1.UncordonNodeRequest{NodeId: id})
	if err != nil {
		t.Fatalf("uncordon: %v", err)
	}
	if !n.GetSchedulable() || n.GetStatus() != zatterav1.NodeStatus_NODE_STATUS_ALIVE {
		t.Errorf("uncordon left node at %v schedulable=%v", n.GetStatus(), n.GetSchedulable())
	}

	t.Run("uncordon recovers a drained node", func(t *testing.T) {
		if _, err := srv.DrainNode(ctx, &zatterav1.DrainNodeRequest{NodeId: id}); err != nil {
			t.Fatal(err)
		}
		out, err := srv.UncordonNode(ctx, &zatterav1.UncordonNodeRequest{NodeId: id})
		if err != nil {
			t.Fatalf("uncordon after drain: %v", err)
		}
		if out.GetStatus() != zatterav1.NodeStatus_NODE_STATUS_ALIVE || !out.GetSchedulable() {
			t.Errorf("drained node not returned to service: %v schedulable=%v", out.GetStatus(), out.GetSchedulable())
		}
	})

	t.Run("cordon leaves a draining node alone", func(t *testing.T) {
		if _, err := srv.DrainNode(ctx, &zatterav1.DrainNodeRequest{NodeId: id}); err != nil {
			t.Fatal(err)
		}
		out, err := srv.CordonNode(ctx, &zatterav1.CordonNodeRequest{NodeId: id})
		if err != nil {
			t.Fatal(err)
		}
		if out.GetStatus() != zatterav1.NodeStatus_NODE_STATUS_DRAINING {
			t.Errorf("cordon overrode a drain in progress: %v", out.GetStatus())
		}
	})

	t.Run("uncordon refuses a DOWN node", func(t *testing.T) {
		d, _ := st.Node(id)
		d.Status = zatterav1.NodeStatus_NODE_STATUS_DOWN
		st.PutNode(d)
		_, err := srv.UncordonNode(ctx, &zatterav1.UncordonNodeRequest{NodeId: id})
		if status.Code(err) != codes.FailedPrecondition {
			t.Errorf("uncordon of a DOWN node = %v, want FailedPrecondition — liveness owns that transition", err)
		}
	})

	t.Run("unknown node", func(t *testing.T) {
		if _, err := srv.CordonNode(ctx, &zatterav1.CordonNodeRequest{NodeId: "nope"}); status.Code(err) != codes.NotFound {
			t.Errorf("cordon of an unknown node = %v", err)
		}
	})
}

// TestClusterVersionRange covers the T-93 helper, including the rule that an
// unknown version is reported rather than silently treated as oldest.
func TestClusterVersionRange(t *testing.T) {
	rs := raftstore.NewTestStore(t)
	st := rs.State()
	for i, v := range []string{"v0.3.0", "v0.4.1", "dev"} {
		st.PutNode(&zatterav1.Node{
			Meta: &zatterav1.Meta{Id: ids.New()}, Name: fmt.Sprintf("n%d", i), BinaryVersion: v,
		})
	}
	min, max, unknown := st.ClusterVersionRange()
	if min != "v0.3.0" || max != "v0.4.1" {
		t.Errorf("range = [%s,%s], want [v0.3.0,v0.4.1]", min, max)
	}
	if !unknown {
		t.Error("an unparseable version was not reported")
	}
}
