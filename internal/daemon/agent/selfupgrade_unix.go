//go:build unix

package agent

import (
	"os"
	"syscall"
)

// execSelf replaces this process image with the (already swapped) binary,
// keeping the same pid, arguments and environment. Used when nothing supervises
// the daemon; under systemd restartSelf prefers a unit restart.
func execSelf() error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	return syscall.Exec(exe, os.Args, os.Environ())
}
