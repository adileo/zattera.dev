package commands

import (
	"testing"

	"github.com/spf13/cobra"
)

// TestClusterGroupMerged guards the bug where the CLI and daemon trees each
// registered a `cluster` group: cobra kept both, resolved the name to whichever
// was added first, and silently hid the other's subcommands. In a full build all
// four must be reachable under a single group.
func TestClusterGroupMerged(t *testing.T) {
	var groups int
	for _, c := range root.Commands() {
		if c.Name() == "cluster" {
			groups++
		}
	}
	if groups != 1 {
		t.Fatalf("want exactly 1 top-level cluster group, got %d", groups)
	}

	cluster, _, err := root.Find([]string{"cluster"})
	if err != nil {
		t.Fatalf("find cluster: %v", err)
	}
	for _, name := range []string{"init", "join", "teardown", "upgrade"} {
		if sub, _, err := cluster.Find([]string{name}); err != nil || sub.Name() != name {
			t.Errorf("cluster %s unreachable (err=%v)", name, err)
		}
	}
}

// TestRegisterMergesDuplicateGroups exercises Register directly so the merge
// behaviour is covered even under build tags that trim one of the trees.
func TestRegisterMergesDuplicateGroups(t *testing.T) {
	before := len(root.Commands())
	first := &cobra.Command{Use: "dup-test-group"}
	first.AddCommand(&cobra.Command{Use: "one", Run: func(*cobra.Command, []string) {}})
	second := &cobra.Command{Use: "dup-test-group"}
	second.AddCommand(&cobra.Command{Use: "two", Run: func(*cobra.Command, []string) {}})

	Register(first)
	Register(second)
	t.Cleanup(func() { root.RemoveCommand(first, second) })

	if got := len(root.Commands()); got != before+1 {
		t.Fatalf("want 1 new top-level command, got %d", got-before)
	}
	for _, name := range []string{"one", "two"} {
		if sub, _, err := first.Find([]string{name}); err != nil || sub.Name() != name {
			t.Errorf("subcommand %s unreachable after merge (err=%v)", name, err)
		}
	}
}
