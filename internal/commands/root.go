// Package commands assembles the cobra command tree. CLI commands and server
// commands are registered from build-tagged files (ADR-0002): this file and
// Execute always compile.
package commands

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/zattera-dev/zattera/internal/pkgutil/version"
)

var root = &cobra.Command{
	Use:           "zattera",
	Short:         "Zattera — the single-binary PaaS",
	SilenceUsage:  true,
	SilenceErrors: false,
}

func init() {
	root.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print the binary version",
		Run: func(cmd *cobra.Command, _ []string) {
			fmt.Fprintln(cmd.OutOrStdout(), version.Version)
		},
	})
}

// Register adds a top-level command (called from build-tagged registration
// files' init()).
//
// The CLI and daemon trees both contribute a `cluster` group (host setup vs
// cluster-wide operations). Cobra would keep both and resolve the name to
// whichever was added first, silently hiding the other's subcommands — so a
// group whose name is already taken is merged into the existing one instead.
func Register(cmds ...*cobra.Command) {
	for _, cmd := range cmds {
		if existing := findChild(cmd.Name()); existing != nil {
			for _, sub := range cmd.Commands() {
				existing.AddCommand(sub)
			}
			continue
		}
		root.AddCommand(cmd)
	}
}

// findChild returns root's direct subcommand with this name, or nil.
func findChild(name string) *cobra.Command {
	for _, c := range root.Commands() {
		if c.Name() == name {
			return c
		}
	}
	return nil
}

// Execute runs the CLI.
func Execute() error {
	return root.Execute()
}
