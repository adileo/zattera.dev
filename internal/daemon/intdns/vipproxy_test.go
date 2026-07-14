package intdns

import (
	"io"
	"net"
	"sync"
	"testing"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	"github.com/zattera-dev/zattera/internal/daemon/proxy"
)

// fakeNL records VIP-address operations.
type fakeNL struct {
	mu    sync.Mutex
	dummy bool
	addrs map[string]bool
}

func newFakeNL() *fakeNL { return &fakeNL{addrs: map[string]bool{}} }

func (f *fakeNL) EnsureDummy() error { f.mu.Lock(); f.dummy = true; f.mu.Unlock(); return nil }
func (f *fakeNL) AddAddr(cidr string) error {
	f.mu.Lock()
	f.addrs[cidr] = true
	f.mu.Unlock()
	return nil
}
func (f *fakeNL) DelAddr(cidr string) error {
	f.mu.Lock()
	delete(f.addrs, cidr)
	f.mu.Unlock()
	return nil
}
func (f *fakeNL) has(cidr string) bool { f.mu.Lock(); defer f.mu.Unlock(); return f.addrs[cidr] }

// fakeListen binds loopback listeners and records the requested VIP address.
type fakeListen struct {
	mu        sync.Mutex
	requested map[string]string // "vip:port" → actual loopback addr
}

func newFakeListen() *fakeListen { return &fakeListen{requested: map[string]string{}} }

func (f *fakeListen) listen(addr string) (net.Listener, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.requested[addr] = ln.Addr().String()
	f.mu.Unlock()
	return ln, nil
}

func (f *fakeListen) actual(vipPort string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requested[vipPort]
}

func newTestVIP(src proxy.RouteSource, nl netLinker, l *fakeListen) *VIPProxy {
	v := NewVIPProxy(src, nil)
	v.nl = nl
	v.listen = l.listen
	return v
}

func snap(services ...*clusterv1.InternalService) *clusterv1.RouteSnapshot {
	return &clusterv1.RouteSnapshot{InternalServices: services}
}

func service(vip string, port uint32, proto string, eps ...*clusterv1.Endpoint) *clusterv1.InternalService {
	return &clusterv1.InternalService{
		Vip:   vip,
		Ports: []*clusterv1.InternalPort{{Port: port, Protocol: proto, Endpoints: eps}},
	}
}

func TestVIPReconcileAddsAndRemoves(t *testing.T) {
	nl := newFakeNL()
	fl := newFakeListen()
	src := &proxy.StaticRouteSource{}
	v := newTestVIP(src, nl, fl)

	// Add a service → dummy ensured, VIP addr pinned, listener opened.
	v.Reconcile(snap(service("10.97.0.5", 8080, "tcp")))
	if !nl.dummy {
		t.Fatal("dummy interface not ensured")
	}
	if !nl.has("10.97.0.5/32") {
		t.Fatal("VIP address not added")
	}
	if fl.actual("10.97.0.5:8080") == "" {
		t.Fatal("VIP listener not opened")
	}

	// Remove the service → address and listener are torn down.
	v.Reconcile(snap())
	if nl.has("10.97.0.5/32") {
		t.Fatal("VIP address not removed")
	}
	v.mu.Lock()
	n := len(v.listeners)
	v.mu.Unlock()
	if n != 0 {
		t.Fatalf("listeners not closed: %d", n)
	}
}

func TestVIPSkipsUDPPorts(t *testing.T) {
	nl := newFakeNL()
	fl := newFakeListen()
	v := newTestVIP(&proxy.StaticRouteSource{}, nl, fl)

	v.Reconcile(snap(&clusterv1.InternalService{Vip: "10.97.0.9", Ports: []*clusterv1.InternalPort{
		{Port: 53, Protocol: "udp"}, // skipped
		{Port: 6379, Protocol: "tcp"},
	}}))
	if !nl.has("10.97.0.9/32") {
		t.Fatal("VIP addr should still be pinned for the TCP port")
	}
	if fl.actual("10.97.0.9:53") != "" {
		t.Fatal("UDP port must be skipped")
	}
	if fl.actual("10.97.0.9:6379") == "" {
		t.Fatal("TCP port must be listened on")
	}
}

func TestVIPSplicesToEndpoint(t *testing.T) {
	back := tcpEchoBackend(t)
	nl := newFakeNL()
	fl := newFakeListen()
	src := &proxy.StaticRouteSource{}
	v := newTestVIP(src, nl, fl)

	v.Reconcile(snap(service("10.97.0.5", 8080, "tcp", &clusterv1.Endpoint{Addr: back, Healthy: true})))

	conn, err := net.Dial("tcp", fl.actual("10.97.0.5:8080"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "ping" {
		t.Fatalf("VIP splice echo = %q", buf)
	}
}

func tcpEchoBackend(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) { defer func() { _ = c.Close() }(); _, _ = io.Copy(c, c) }(c)
		}
	}()
	return ln.Addr().String()
}
