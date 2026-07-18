//go:build !unix

package agent

import "fmt"

// execSelf is unix-only; elsewhere a supervisor must restart the daemon.
func execSelf() error {
	return fmt.Errorf("agent: cannot restart in place on this platform; restart the zattera service")
}
