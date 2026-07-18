package api

import (
	"context"
	"fmt"
	"sort"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/upgrade"
	"github.com/zattera-dev/zattera/internal/pkgutil/version"
)

// UpgradeDialer reaches a node's AgentLocalService to swap its binary.
type UpgradeDialer interface {
	UpgradeBinary(ctx context.Context, node *zatterav1.Node, req *clusterv1.AgentUpgradeRequest) (*clusterv1.AgentUpgradeResponse, error)
}

// SetUpgrader wires cluster upgrade (T-95). Without it UpgradePlan and
// UpgradeNode report Unimplemented.
func (s *NodeServer) SetUpgrader(r upgrade.Resolver, d UpgradeDialer) {
	s.releases, s.upgradeDial = r, d
}

// UpgradePlan resolves the target release and returns the node-by-node order.
//
// Ordering is a correctness constraint, not a preference: the FSM is
// additive-only and an unknown mutation is surfaced as an error *without*
// halting the node, so a new leader proposing a mutation an old follower cannot
// apply diverges that follower's state silently. An old leader only ever
// proposes mutations newer followers understand, so the leader goes last.
func (s *NodeServer) UpgradePlan(ctx context.Context, req *zatterav1.UpgradePlanRequest) (*zatterav1.UpgradePlanResponse, error) {
	if s.releases == nil {
		return nil, status.Error(codes.Unimplemented, "cluster upgrade is not available on this node")
	}
	rel, err := s.releases.Resolve(ctx, req.GetVersion())
	if err != nil {
		return nil, toStatus(err)
	}

	nodes := s.store.ListNodes()
	leaderID := s.leaderNodeID()
	steps := make([]*zatterav1.UpgradeStep, 0, len(nodes))
	var warnings []string
	ingress := 0

	for _, n := range nodes {
		if _, ok := rel.Asset(n.GetOsArch()); !ok {
			warnings = append(warnings, fmt.Sprintf(
				"node %s runs %s, which release %s does not publish — it will be skipped",
				n.GetName(), n.GetOsArch(), rel.Version))
			continue
		}
		if n.GetStatus() == zatterav1.NodeStatus_NODE_STATUS_DOWN {
			warnings = append(warnings, fmt.Sprintf("node %s is DOWN and cannot be upgraded", n.GetName()))
			continue
		}
		cur := n.GetBinaryVersion()
		if cur == "" {
			warnings = append(warnings, fmt.Sprintf(
				"node %s reports no version; it will be upgraded anyway", n.GetName()))
		}
		if hasRole(n, zatterav1.NodeRole_NODE_ROLE_WORKER) {
			ingress++
		}
		steps = append(steps, &zatterav1.UpgradeStep{
			NodeId:         n.GetMeta().GetId(),
			NodeName:       n.GetName(),
			CurrentVersion: cur,
			OsArch:         n.GetOsArch(),
			Leader:         n.GetMeta().GetId() == leaderID,
			UpToDate:       cur != "" && sameVersion(cur, rel.Version),
		})
	}
	sortUpgradeSteps(steps)

	if ingress < 2 {
		warnings = append(warnings, "only one node serves ingress: requests to it will fail for a few seconds while it restarts")
	}
	if _, _, unknown := s.store.ClusterVersionRange(); unknown {
		warnings = append(warnings, "some nodes report an unparseable version; they are treated as needing the upgrade")
	}
	return &zatterav1.UpgradePlanResponse{TargetVersion: rel.Version, Steps: steps, Warnings: warnings}, nil
}

// sortUpgradeSteps orders workers first, then control followers, then the
// leader — see UpgradePlan for why the leader is last.
func sortUpgradeSteps(steps []*zatterav1.UpgradeStep) {
	rank := func(s *zatterav1.UpgradeStep) int {
		if s.GetLeader() {
			return 2
		}
		return 0
	}
	sort.SliceStable(steps, func(i, j int) bool {
		ri, rj := rank(steps[i]), rank(steps[j])
		if ri != rj {
			return ri < rj
		}
		return steps[i].GetNodeName() < steps[j].GetNodeName()
	})
}

// UpgradeNode swaps one node's binary. The CLI drives the rollout; this is one
// step of it, so a failure stops the run with the rest of the cluster untouched.
func (s *NodeServer) UpgradeNode(ctx context.Context, req *zatterav1.UpgradeNodeRequest) (*zatterav1.UpgradeNodeResponse, error) {
	if s.releases == nil || s.upgradeDial == nil {
		return nil, status.Error(codes.Unimplemented, "cluster upgrade is not available on this node")
	}
	node, ok := s.store.Node(req.GetNodeId())
	if !ok {
		return nil, status.Error(codes.NotFound, "node not found")
	}
	if node.GetStatus() == zatterav1.NodeStatus_NODE_STATUS_DOWN {
		return nil, status.Errorf(codes.FailedPrecondition, "node %s is down", node.GetName())
	}
	rel, err := s.releases.Resolve(ctx, req.GetVersion())
	if err != nil {
		return nil, toStatus(err)
	}
	asset, ok := rel.Asset(node.GetOsArch())
	if !ok {
		return nil, status.Errorf(codes.FailedPrecondition,
			"release %s publishes no binary for %s", rel.Version, node.GetOsArch())
	}

	resp, err := s.upgradeDial.UpgradeBinary(ctx, node, &clusterv1.AgentUpgradeRequest{
		Version: rel.Version, Url: asset.URL, Sha256: asset.SHA256,
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return &zatterav1.UpgradeNodeResponse{
		PreviousVersion: resp.GetPreviousVersion(),
		StagedVersion:   resp.GetStagedVersion(),
	}, nil
}

// leaderNodeID returns the raft leader's node id, or "" when unknown.
func (s *NodeServer) leaderNodeID() string {
	if lr, ok := s.raft.(interface{ LeaderAddr() (string, string) }); ok {
		_, id := lr.LeaderAddr()
		return id
	}
	return ""
}

// sameVersion compares two build versions, falling back to string equality when
// either side is not parseable.
func sameVersion(a, b string) bool {
	if c, ok := version.Compare(a, b); ok {
		return c == 0
	}
	return a == b
}

func hasRole(n *zatterav1.Node, want zatterav1.NodeRole) bool {
	for _, r := range n.GetRoles() {
		if r == want {
			return true
		}
	}
	return false
}

// GRPCUpgradeDialer is the production UpgradeDialer.
type GRPCUpgradeDialer struct {
	Connect func(ctx context.Context, node *zatterav1.Node) (*grpc.ClientConn, error)
}

func (g GRPCUpgradeDialer) UpgradeBinary(ctx context.Context, node *zatterav1.Node, req *clusterv1.AgentUpgradeRequest) (*clusterv1.AgentUpgradeResponse, error) {
	conn, err := g.Connect(ctx, node)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	return clusterv1.NewAgentLocalServiceClient(conn).UpgradeBinary(ctx, req)
}
