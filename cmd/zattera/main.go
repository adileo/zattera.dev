// zattera is the single binary: CLI, control-plane node and worker node.
// Role selection happens at runtime (subcommands); build tags cli_only /
// server_only trim the binary (ADR-0002).
package main

import (
	"os"

	"github.com/zattera-dev/zattera/internal/commands"
)

func main() {
	if err := commands.Execute(); err != nil {
		os.Exit(1)
	}
}
