package intdns

import (
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/miekg/dns"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	"github.com/zattera-dev/zattera/internal/daemon/proxy"
	"github.com/zattera-dev/zattera/internal/testutil/freeport"
)

func isvc(fqdn, proj, env, vip string) *clusterv1.InternalService {
	return &clusterv1.InternalService{Fqdn: fqdn, ProjectId: proj, EnvironmentId: env, Vip: vip}
}

// testSnapshot: two services in (p1, prod), one in (p1, staging), one in p2.
func testResolver(upstreams []string) (*Resolver, NetworkScope) {
	src := &proxy.StaticRouteSource{Snapshot: &clusterv1.RouteSnapshot{InternalServices: []*clusterv1.InternalService{
		isvc("api.production.demo.internal.", "p1", "prod", "10.97.0.5"),
		isvc("db.production.demo.internal.", "p1", "prod", "10.97.0.6"),
		isvc("api.staging.demo.internal.", "p1", "staging", "10.97.0.7"),
		isvc("api.production.other.internal.", "p2", "prod2", "10.97.1.5"),
	}}}
	r := New(src, upstreams, nil)
	return r, NetworkScope{Gateway: "10.201.0.1", ProjectID: "p1", EnvID: "prod"}
}

func query(name string, qtype uint16) *dns.Msg {
	m := new(dns.Msg)
	m.SetQuestion(dns.Fqdn(name), qtype)
	return m
}

func TestInternalResolutionInScope(t *testing.T) {
	r, scope := testResolver([]string{"127.0.0.1:1"})

	// Full form within scope.
	resp := r.Answer(scope, query("api.production.demo.internal", dns.TypeA))
	if resp.Rcode != dns.RcodeSuccess || len(resp.Answer) != 1 {
		t.Fatalf("in-scope A = rcode %d, %d answers", resp.Rcode, len(resp.Answer))
	}
	if a := resp.Answer[0].(*dns.A); a.A.String() != "10.97.0.5" || a.Hdr.Ttl != internalTTL {
		t.Fatalf("A record = %v ttl=%d", a.A, a.Hdr.Ttl)
	}
	if resp.RecursionAvailable {
		t.Error(".internal must be answered authoritatively (RA=false)")
	}

	// Shorthand within the same env.
	resp = r.Answer(scope, query("db.internal", dns.TypeA))
	if resp.Rcode != dns.RcodeSuccess || len(resp.Answer) != 1 || resp.Answer[0].(*dns.A).A.String() != "10.97.0.6" {
		t.Fatalf("shorthand = rcode %d answers %d", resp.Rcode, len(resp.Answer))
	}
}

func TestInternalIsolation(t *testing.T) {
	r, scope := testResolver([]string{"127.0.0.1:1"})

	// A different env of the same project → NXDOMAIN (staging ≠ production).
	if resp := r.Answer(scope, query("api.staging.demo.internal", dns.TypeA)); resp.Rcode != dns.RcodeNameError {
		t.Fatalf("cross-env rcode = %d, want NXDOMAIN", resp.Rcode)
	}
	// A different project → NXDOMAIN (isolation) even though it exists.
	if resp := r.Answer(scope, query("api.production.other.internal", dns.TypeA)); resp.Rcode != dns.RcodeNameError {
		t.Fatalf("cross-project rcode = %d, want NXDOMAIN", resp.Rcode)
	}
	// Unknown service.
	if resp := r.Answer(scope, query("ghost.internal", dns.TypeA)); resp.Rcode != dns.RcodeNameError {
		t.Fatalf("unknown rcode = %d, want NXDOMAIN", resp.Rcode)
	}
	// Case-insensitive matching.
	if resp := r.Answer(scope, query("API.Production.Demo.Internal", dns.TypeA)); resp.Rcode != dns.RcodeSuccess {
		t.Fatalf("case-insensitive rcode = %d, want success", resp.Rcode)
	}
}

func TestForwardingFallback(t *testing.T) {
	up := fakeUpstream(t, "1.2.3.4")
	r, scope := testResolver([]string{up})

	resp := r.Answer(scope, query("example.com", dns.TypeA))
	if resp.Rcode != dns.RcodeSuccess || len(resp.Answer) != 1 || resp.Answer[0].(*dns.A).A.String() != "1.2.3.4" {
		t.Fatalf("forwarded A = rcode %d answers %d", resp.Rcode, len(resp.Answer))
	}

	// No reachable upstream → SERVFAIL.
	r.upstreams = nil
	if resp := r.Answer(scope, query("example.com", dns.TypeA)); resp.Rcode != dns.RcodeServerFailure {
		t.Fatalf("no-upstream rcode = %d, want SERVFAIL", resp.Rcode)
	}
}

func TestReconcileServesOverTheWire(t *testing.T) {
	r, scope := testResolver(nil)
	r.upstreams = nil
	port := freeport.Get(t)
	r.port = strconv.Itoa(port)
	scope.Gateway = "127.0.0.1"

	r.Reconcile([]NetworkScope{scope})
	defer r.Close()

	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	c := &dns.Client{Timeout: 500 * time.Millisecond}
	var resp *dns.Msg
	for i := 0; i < 40; i++ {
		if got, _, err := c.Exchange(query("api.production.demo.internal", dns.TypeA), addr); err == nil && got != nil {
			resp = got
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if resp == nil || resp.Rcode != dns.RcodeSuccess || len(resp.Answer) != 1 {
		t.Fatalf("over-the-wire query failed: %+v", resp)
	}
	if resp.Answer[0].(*dns.A).A.String() != "10.97.0.5" {
		t.Fatalf("wire A = %v", resp.Answer[0])
	}

	// Reconcile the network away → the listener stops.
	r.Reconcile(nil)
}

// fakeUpstream starts a UDP DNS server that answers every A query with ip.
func fakeUpstream(t *testing.T, ip string) string {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &dns.Server{PacketConn: pc, Handler: dns.HandlerFunc(func(w dns.ResponseWriter, req *dns.Msg) {
		m := new(dns.Msg)
		m.SetReply(req)
		m.Answer = []dns.RR{&dns.A{
			Hdr: dns.RR_Header{Name: req.Question[0].Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 30},
			A:   net.ParseIP(ip),
		}}
		_ = w.WriteMsg(m)
	})}
	started := make(chan struct{})
	srv.NotifyStartedFunc = func() { close(started) }
	go func() { _ = srv.ActivateAndServe() }()
	<-started
	t.Cleanup(func() { _ = srv.Shutdown() })
	return pc.LocalAddr().String()
}
