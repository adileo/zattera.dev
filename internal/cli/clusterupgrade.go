package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/pkg/apiclient"
)

const (
	// nodeUpgradeTimeout bounds the wait for one node to come back on the new
	// version. A node that never returns must fail the run, not hang it.
	nodeUpgradeTimeout = 5 * time.Minute
	// nodePollInterval is how often we re-check a restarting node.
	nodePollInterval = 3 * time.Second
	// reconnectGrace is how long to keep retrying a call whose connection died
	// because the node serving it was the one being upgraded.
	reconnectGrace = 90 * time.Second
)

func newClusterCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cluster",
		Short: "Cluster-wide operations",
	}
	cmd.AddCommand(newClusterUpgradeCmd())
	return cmd
}

func newClusterUpgradeCmd() *cobra.Command {
	var (
		target string
		dryRun bool
		yes    bool
	)
	cmd := &cobra.Command{
		Use:   "upgrade",
		Short: "Roll every node to the same version, one at a time",
		Long: "Upgrade the whole cluster with minimal downtime.\n\n" +
			"Nodes are upgraded one at a time: cordon (stop new placements), swap the\n" +
			"binary, restart, wait for the node to report the new version, uncordon.\n" +
			"Workload containers are managed by docker and keep running across the\n" +
			"restart — what blinks is that node's ingress, for a few seconds.\n\n" +
			"The raft leader is upgraded LAST. The FSM only ever gains new mutations,\n" +
			"so an old leader proposes nothing a new follower cannot apply; the reverse\n" +
			"would silently diverge the followers' state.\n\n" +
			"Any failure stops the run with the remaining nodes untouched.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, _, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			p := printerFor(cmd)

			planCtx, cancel := cmdContext(cmd)
			plan, err := client.Nodes.UpgradePlan(planCtx, &zatterav1.UpgradePlanRequest{Version: target})
			cancel()
			if err != nil {
				return apiError(err)
			}

			pending := pendingSteps(plan.GetSteps())
			renderPlan(p, plan, pending)
			if dryRun {
				return nil
			}
			if len(pending) == 0 {
				return nil
			}
			if !yes {
				return fmt.Errorf("re-run with --yes to apply this plan")
			}

			ctx := cmd.Context()
			for i, step := range pending {
				p.Infof("[%d/%d] %s (%s → %s)%s", i+1, len(pending), step.GetNodeName(),
					displayVersion(step.GetCurrentVersion()), plan.GetTargetVersion(), leaderNote(step))
				if err := upgradeOneNode(ctx, client, p, step, plan.GetTargetVersion()); err != nil {
					p.Errorf("node %s failed: %v", step.GetNodeName(), err)
					p.Infof("the remaining %d node(s) were not touched; %s is left cordoned",
						len(pending)-i-1, step.GetNodeName())
					return fmt.Errorf("cluster upgrade aborted at node %s", step.GetNodeName())
				}
				p.Successf("%s is on %s", step.GetNodeName(), plan.GetTargetVersion())
			}
			p.Successf("cluster upgraded to %s", plan.GetTargetVersion())
			return nil
		},
	}
	cmd.Flags().StringVar(&target, "version", "", "target version (default: the latest release)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "show the plan and exit")
	cmd.Flags().BoolVar(&yes, "yes", false, "apply the plan")
	return cmd
}

// upgradeOneNode runs the full cordon → swap → wait → uncordon cycle for a node.
func upgradeOneNode(ctx context.Context, client *apiclient.Client, p printer, step *zatterav1.UpgradeStep, target string) error {
	nodeID := step.GetNodeId()

	if err := withTimeout(ctx, 30*time.Second, func(c context.Context) error {
		_, err := client.Nodes.CordonNode(c, &zatterav1.CordonNodeRequest{NodeId: nodeID})
		return err
	}); err != nil {
		return fmt.Errorf("cordon: %w", apiError(err))
	}

	if err := withTimeout(ctx, 2*time.Minute, func(c context.Context) error {
		_, err := client.Nodes.UpgradeNode(c, &zatterav1.UpgradeNodeRequest{NodeId: nodeID, Version: target})
		return err
	}); err != nil {
		// Upgrading the node that serves this API connection tears the call
		// down mid-flight; that is expected, not a failure. The version check
		// below is what actually decides whether it worked.
		p.Infof("  connection dropped while %s restarted; waiting for it to come back", step.GetNodeName())
	}

	if err := waitForVersion(ctx, client, nodeID, target); err != nil {
		return err
	}
	return withRetry(ctx, reconnectGrace, func(c context.Context) error {
		_, err := client.Nodes.UncordonNode(c, &zatterav1.UncordonNodeRequest{NodeId: nodeID})
		return apiError(err)
	})
}

// waitForVersion blocks until the node reports the target version and is alive.
func waitForVersion(ctx context.Context, client *apiclient.Client, nodeID, target string) error {
	deadline := time.Now().Add(nodeUpgradeTimeout)
	var lastErr error
	for time.Now().Before(deadline) {
		var node *zatterav1.Node
		err := withTimeout(ctx, 10*time.Second, func(c context.Context) error {
			n, err := client.Nodes.GetNode(c, &zatterav1.GetNodeRequest{NodeId: nodeID})
			node = n
			return err
		})
		switch {
		case err != nil:
			lastErr = err // the control plane itself may be restarting
		case node.GetBinaryVersion() == target && node.GetStatus() == zatterav1.NodeStatus_NODE_STATUS_ALIVE:
			return nil
		default:
			lastErr = fmt.Errorf("still on %s (%s)", displayVersion(node.GetBinaryVersion()),
				node.GetStatus().String())
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(nodePollInterval):
		}
	}
	return fmt.Errorf("did not report %s within %s: %v", target, nodeUpgradeTimeout, lastErr)
}

// withRetry retries a call until it succeeds or the budget runs out. Used for
// calls that must survive the control plane restarting under them.
func withRetry(ctx context.Context, budget time.Duration, fn func(context.Context) error) error {
	deadline := time.Now().Add(budget)
	var err error
	for time.Now().Before(deadline) {
		if err = withTimeout(ctx, 15*time.Second, fn); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return err
}

func withTimeout(ctx context.Context, d time.Duration, fn func(context.Context) error) error {
	c, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	return fn(c)
}

// pendingSteps drops nodes already on the target version.
func pendingSteps(steps []*zatterav1.UpgradeStep) []*zatterav1.UpgradeStep {
	out := make([]*zatterav1.UpgradeStep, 0, len(steps))
	for _, s := range steps {
		if !s.GetUpToDate() {
			out = append(out, s)
		}
	}
	return out
}

// printer is the subset of ui.Printer the upgrade flow needs, so the plan
// rendering is testable without a terminal.
type printer interface {
	Successf(format string, args ...any)
	Infof(format string, args ...any)
	Errorf(format string, args ...any)
	Table(headers []string, rows [][]string)
}

func renderPlan(p printer, plan *zatterav1.UpgradePlanResponse, pending []*zatterav1.UpgradeStep) {
	rows := make([][]string, 0, len(plan.GetSteps()))
	for _, s := range plan.GetSteps() {
		action := "upgrade"
		if s.GetUpToDate() {
			action = "skip (up to date)"
		}
		role := "worker"
		if s.GetLeader() {
			role = "leader (last)"
		}
		rows = append(rows, []string{
			s.GetNodeName(), role, displayVersion(s.GetCurrentVersion()), plan.GetTargetVersion(), action,
		})
	}
	p.Table([]string{"NODE", "ROLE", "CURRENT", "TARGET", "ACTION"}, rows)
	for _, w := range plan.GetWarnings() {
		p.Infof("! %s", w)
	}
	if len(pending) == 0 {
		p.Successf("every node is already on %s", plan.GetTargetVersion())
	}
}

func displayVersion(v string) string {
	if v == "" {
		return "unknown"
	}
	return v
}

func leaderNote(s *zatterav1.UpgradeStep) string {
	if s.GetLeader() {
		return " — raft leader, upgraded last"
	}
	return ""
}
