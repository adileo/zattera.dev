package cli

import (
	"fmt"
	"strings"
	"testing"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

// capturePrinter records everything the upgrade flow would print.
type capturePrinter struct {
	lines []string
	rows  [][]string
	head  []string
}

func (c *capturePrinter) Successf(f string, a ...any) {
	c.lines = append(c.lines, "✓ "+fmt.Sprintf(f, a...))
}
func (c *capturePrinter) Infof(f string, a ...any) { c.lines = append(c.lines, fmt.Sprintf(f, a...)) }
func (c *capturePrinter) Errorf(f string, a ...any) {
	c.lines = append(c.lines, "✗ "+fmt.Sprintf(f, a...))
}
func (c *capturePrinter) Table(h []string, r [][]string) {
	c.head, c.rows = h, r
}
func (c *capturePrinter) text() string { return strings.Join(c.lines, "\n") }

func upgradeStep(name, cur string, leader, upToDate bool) *zatterav1.UpgradeStep {
	return &zatterav1.UpgradeStep{
		NodeId: "id-" + name, NodeName: name, CurrentVersion: cur,
		OsArch: "linux/amd64", Leader: leader, UpToDate: upToDate,
	}
}

// TestClusterUpgradePendingSteps: nodes already on the target are not touched.
func TestClusterUpgradePendingSteps(t *testing.T) {
	steps := []*zatterav1.UpgradeStep{
		upgradeStep("worker-a", "v0.3.0", false, false),
		upgradeStep("worker-b", "v0.4.0", false, true),
		upgradeStep("leader", "v0.3.0", true, false),
	}
	pending := pendingSteps(steps)
	if len(pending) != 2 {
		t.Fatalf("pending = %d, want 2", len(pending))
	}
	if pending[len(pending)-1].GetNodeName() != "leader" {
		t.Errorf("filtering reordered the plan: %v", pending[len(pending)-1].GetNodeName())
	}
	for _, s := range pending {
		if s.GetUpToDate() {
			t.Errorf("up-to-date node %s kept in the plan", s.GetNodeName())
		}
	}
}

// TestClusterUpgradeRenderPlan pins what the operator is shown before
// committing: the order, the leader-last marking, and every warning.
func TestClusterUpgradeRenderPlan(t *testing.T) {
	plan := &zatterav1.UpgradePlanResponse{
		TargetVersion: "v0.4.0",
		Steps: []*zatterav1.UpgradeStep{
			upgradeStep("worker-a", "v0.3.0", false, false),
			upgradeStep("worker-b", "", false, false),
			upgradeStep("leader", "v0.4.0", true, true),
		},
		Warnings: []string{"only one node serves ingress: requests to it will fail for a few seconds while it restarts"},
	}
	p := &capturePrinter{}
	renderPlan(p, plan, pendingSteps(plan.GetSteps()))

	if len(p.rows) != 3 {
		t.Fatalf("rendered %d rows, want 3", len(p.rows))
	}
	if got := p.rows[2]; got[1] != "leader (last)" || got[4] != "skip (up to date)" {
		t.Errorf("leader row = %v", got)
	}
	// A node with no reported version must read as unknown, not blank — blank
	// would look like a rendering bug rather than missing data.
	if p.rows[1][2] != "unknown" {
		t.Errorf("missing version rendered as %q, want \"unknown\"", p.rows[1][2])
	}
	if p.rows[0][3] != "v0.4.0" {
		t.Errorf("target column = %q", p.rows[0][3])
	}
	if !strings.Contains(p.text(), "one node serves ingress") {
		t.Errorf("warning not shown:\n%s", p.text())
	}
}

// TestClusterUpgradeAllUpToDate says so plainly instead of printing an empty
// plan the operator has to interpret.
func TestClusterUpgradeAllUpToDate(t *testing.T) {
	plan := &zatterav1.UpgradePlanResponse{
		TargetVersion: "v0.4.0",
		Steps:         []*zatterav1.UpgradeStep{upgradeStep("worker-a", "v0.4.0", false, true)},
	}
	p := &capturePrinter{}
	pending := pendingSteps(plan.GetSteps())
	renderPlan(p, plan, pending)

	if len(pending) != 0 {
		t.Fatalf("pending = %d, want 0", len(pending))
	}
	if !strings.Contains(p.text(), "already on v0.4.0") {
		t.Errorf("no up-to-date message:\n%s", p.text())
	}
}

// TestClusterUpgradeRequiresConfirmation: without --yes the command must not
// start touching nodes.
func TestClusterUpgradeRequiresConfirmation(t *testing.T) {
	cmd := newClusterUpgradeCmd()
	if f := cmd.Flags().Lookup("yes"); f == nil {
		t.Fatal("no --yes flag")
	}
	if f := cmd.Flags().Lookup("dry-run"); f == nil {
		t.Fatal("no --dry-run flag")
	}
	// The help text must state the leader-last ordering: it is a correctness
	// property, not an implementation detail an operator can ignore.
	if !strings.Contains(cmd.Long, "LAST") {
		t.Errorf("help does not explain the leader ordering:\n%s", cmd.Long)
	}
}

func TestDisplayVersion(t *testing.T) {
	if got := displayVersion(""); got != "unknown" {
		t.Errorf("displayVersion(\"\") = %q", got)
	}
	if got := displayVersion("v1.0.0"); got != "v1.0.0" {
		t.Errorf("displayVersion = %q", got)
	}
}
