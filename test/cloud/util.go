//go:build cloud

package cloud

import "strings"

// shQuote single-quotes a string for safe use in a POSIX shell command.
func shQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
