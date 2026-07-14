//go:build !linux

package intdns

import "log/slog"

// newNetLinker returns a no-op VIP address manager off Linux (dev/macOS): the
// VIP proxy compiles but cannot pin addresses without netlink.
func newNetLinker(log *slog.Logger) netLinker { return noopNetLinker{log: log} }

type noopNetLinker struct{ log *slog.Logger }

func (n noopNetLinker) EnsureDummy() error {
	n.log.Warn("intdns: VIP interface management is Linux-only; skipping")
	return nil
}
func (noopNetLinker) AddAddr(string) error { return nil }
func (noopNetLinker) DelAddr(string) error { return nil }
