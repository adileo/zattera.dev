//go:build linux

package intdns

import (
	"fmt"
	"log/slog"

	"github.com/vishvananda/netlink"
)

// dummyDev is the interface VIP addresses are pinned to.
const dummyDev = "zt-vip"

// newNetLinker returns the netlink-backed VIP address manager on Linux.
func newNetLinker(log *slog.Logger) netLinker { return &linuxNetLinker{log: log} }

type linuxNetLinker struct {
	log  *slog.Logger
	link netlink.Link
}

// EnsureDummy creates (idempotently) and brings up the zt-vip dummy interface.
func (l *linuxNetLinker) EnsureDummy() error {
	if link, err := netlink.LinkByName(dummyDev); err == nil {
		l.link = link
		return netlink.LinkSetUp(link)
	}
	dummy := &netlink.Dummy{LinkAttrs: netlink.NewLinkAttrs()}
	dummy.Name = dummyDev
	if err := netlink.LinkAdd(dummy); err != nil {
		return fmt.Errorf("intdns: add %s: %w", dummyDev, err)
	}
	link, err := netlink.LinkByName(dummyDev)
	if err != nil {
		return err
	}
	l.link = link
	return netlink.LinkSetUp(link)
}

// AddAddr pins a VIP CIDR onto the dummy interface (idempotent).
func (l *linuxNetLinker) AddAddr(cidr string) error {
	addr, err := netlink.ParseAddr(cidr)
	if err != nil {
		return err
	}
	if err := netlink.AddrAdd(l.link, addr); err != nil && !isExists(err) {
		return err
	}
	return nil
}

// DelAddr removes a VIP CIDR from the dummy interface.
func (l *linuxNetLinker) DelAddr(cidr string) error {
	addr, err := netlink.ParseAddr(cidr)
	if err != nil {
		return err
	}
	if err := netlink.AddrDel(l.link, addr); err != nil && !isNotExist(err) {
		return err
	}
	return nil
}

func isExists(err error) bool { return err != nil && contains(err.Error(), "exists") }
func isNotExist(err error) bool {
	return err != nil && (contains(err.Error(), "cannot assign") || contains(err.Error(), "no such"))
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
