package intdns

import (
	"context"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	"github.com/zattera-dev/zattera/internal/daemon/proxy"
)

const vipDialTimeout = 5 * time.Second

// netLinker manages VIP addresses on the dummy interface. The real
// implementation (Linux, netlink) lives in vipproxy_linux.go; other platforms
// get a logging no-op.
type netLinker interface {
	EnsureDummy() error
	AddAddr(cidr string) error
	DelAddr(cidr string) error
}

// VIPProxy binds each environment's service VIP on the dummy interface and
// L4-splices VIP:port to the service's endpoints (F26 / spec §2.7). It
// reconciles to the current RouteSnapshot's InternalServices; TCP only in v1.
type VIPProxy struct {
	source proxy.RouteSource
	nl     netLinker
	listen func(addr string) (net.Listener, error)
	dial   func(addr string) (net.Conn, error)
	log    *slog.Logger

	mu        sync.Mutex
	started   bool
	inflight  map[string]*int64
	addrs     map[string]bool         // vip → present on the dummy iface
	listeners map[string]*vipListener // "vip:port" → listener
}

// NewVIPProxy builds the VIP proxy over a route source.
func NewVIPProxy(source proxy.RouteSource, log *slog.Logger) *VIPProxy {
	if log == nil {
		log = slog.Default()
	}
	return &VIPProxy{
		source: source, nl: newNetLinker(log), log: log,
		listen:    func(addr string) (net.Listener, error) { return net.Listen("tcp", addr) },
		dial:      func(addr string) (net.Conn, error) { return net.DialTimeout("tcp", addr, vipDialTimeout) },
		inflight:  map[string]*int64{},
		addrs:     map[string]bool{},
		listeners: map[string]*vipListener{},
	}
}

// Run reconciles to every route snapshot until ctx is canceled.
func (v *VIPProxy) Run(ctx context.Context) {
	updates := v.source.Updates(ctx)
	for {
		select {
		case <-ctx.Done():
			v.Close()
			return
		case snap, ok := <-updates:
			if !ok {
				v.Close()
				return
			}
			v.Reconcile(snap)
		}
	}
}

// Reconcile brings the VIP addresses and listeners in line with the snapshot.
// VIP addresses are added BEFORE their listeners (you cannot bind an address
// that is not yet on an interface).
func (v *VIPProxy) Reconcile(snap *clusterv1.RouteSnapshot) {
	wantAddr := map[string]bool{}
	wantLis := map[string][]*clusterv1.Endpoint{}
	for _, is := range snap.GetInternalServices() {
		vip := is.GetVip()
		if vip == "" {
			continue
		}
		wantAddr[vip] = true
		for _, p := range is.GetPorts() {
			if p.GetPort() == 0 {
				continue
			}
			if strings.EqualFold(p.GetProtocol(), "udp") {
				v.log.Warn("intdns: UDP internal port skipped (TCP only in v1)", "vip", vip, "port", p.GetPort())
				continue
			}
			wantLis[net.JoinHostPort(vip, strconv.Itoa(int(p.GetPort())))] = p.GetEndpoints()
		}
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	if len(wantAddr) > 0 && !v.started {
		if err := v.nl.EnsureDummy(); err != nil {
			v.log.Warn("intdns: ensure dummy interface failed", "err", err)
		} else {
			v.started = true
		}
	}
	// Add VIP addresses first.
	for vip := range wantAddr {
		if v.addrs[vip] {
			continue
		}
		if err := v.nl.AddAddr(vip + "/32"); err != nil {
			v.log.Warn("intdns: add VIP addr failed", "vip", vip, "err", err)
			continue
		}
		v.addrs[vip] = true
	}
	// Open/update listeners.
	for key, eps := range wantLis {
		if l, ok := v.listeners[key]; ok {
			l.setEndpoints(eps)
			continue
		}
		ln, err := v.listen(key)
		if err != nil {
			v.log.Warn("intdns: VIP listen failed", "addr", key, "err", err)
			continue
		}
		l := &vipListener{ln: ln, v: v}
		l.setEndpoints(eps)
		v.listeners[key] = l
		go l.serve()
	}
	// Remove listeners no longer wanted.
	for key, l := range v.listeners {
		if _, ok := wantLis[key]; !ok {
			_ = l.ln.Close()
			delete(v.listeners, key)
		}
	}
	// Remove VIP addresses no longer wanted (after their listeners are gone).
	for vip := range v.addrs {
		if !wantAddr[vip] {
			if err := v.nl.DelAddr(vip + "/32"); err != nil {
				v.log.Warn("intdns: del VIP addr failed", "vip", vip, "err", err)
			}
			delete(v.addrs, vip)
		}
	}
}

// Close stops all listeners and removes VIP addresses.
func (v *VIPProxy) Close() {
	v.mu.Lock()
	defer v.mu.Unlock()
	for key, l := range v.listeners {
		_ = l.ln.Close()
		delete(v.listeners, key)
	}
	for vip := range v.addrs {
		_ = v.nl.DelAddr(vip + "/32")
		delete(v.addrs, vip)
	}
}

// vipListener accepts on one VIP:port and splices to the current endpoints.
type vipListener struct {
	ln  net.Listener
	v   *VIPProxy
	eps atomic.Pointer[[]*clusterv1.Endpoint]
}

func (l *vipListener) setEndpoints(eps []*clusterv1.Endpoint) { l.eps.Store(&eps) }

func (l *vipListener) serve() {
	for {
		conn, err := l.ln.Accept()
		if err != nil {
			return
		}
		go l.v.handle(conn, l.eps.Load())
	}
}

// handle picks a healthy endpoint (P2C by in-flight) and splices bidirectionally.
func (v *VIPProxy) handle(client net.Conn, epsPtr *[]*clusterv1.Endpoint) {
	defer func() { _ = client.Close() }()
	var eps []*clusterv1.Endpoint
	if epsPtr != nil {
		eps = *epsPtr
	}
	ep := v.pick(eps)
	if ep == nil {
		return
	}
	release := v.acquire(ep.GetAddr())
	defer release()

	upstream, err := v.dial(ep.GetAddr())
	if err != nil {
		return
	}
	defer func() { _ = upstream.Close() }()

	done := make(chan struct{}, 2)
	go splice(upstream, client, done)
	go splice(client, upstream, done)
	<-done
	<-done
}

// pick chooses a healthy endpoint via power-of-two-choices over in-flight.
func (v *VIPProxy) pick(eps []*clusterv1.Endpoint) *clusterv1.Endpoint {
	var healthy []*clusterv1.Endpoint
	for _, e := range eps {
		if e.GetHealthy() {
			healthy = append(healthy, e)
		}
	}
	switch len(healthy) {
	case 0:
		return nil
	case 1:
		return healthy[0]
	}
	i := rand.IntN(len(healthy))
	j := rand.IntN(len(healthy) - 1)
	if j >= i {
		j++
	}
	if v.load(healthy[j].GetAddr()) < v.load(healthy[i].GetAddr()) {
		return healthy[j]
	}
	return healthy[i]
}

func (v *VIPProxy) counter(addr string) *int64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	c, ok := v.inflight[addr]
	if !ok {
		c = new(int64)
		v.inflight[addr] = c
	}
	return c
}

func (v *VIPProxy) load(addr string) int64 { return atomic.LoadInt64(v.counter(addr)) }
func (v *VIPProxy) acquire(addr string) func() {
	c := v.counter(addr)
	atomic.AddInt64(c, 1)
	return func() { atomic.AddInt64(c, -1) }
}

// splice copies src→dst then half-closes dst (mirrors the L4 proxy, T-43).
func splice(dst, src net.Conn, done chan<- struct{}) {
	_, _ = io.Copy(dst, src)
	if cw, ok := dst.(interface{ CloseWrite() error }); ok {
		_ = cw.CloseWrite()
	}
	done <- struct{}{}
}
