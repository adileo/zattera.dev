// Package intdns is the per-node internal DNS resolver (F26). It binds a DNS
// server on each per-(project,env) bridge network's gateway IP and answers
// <svc>.<env>.<project>.internal (and the <svc>.internal shorthand) with the
// service's VIP — but only within the network's own scope, so services in one
// project/env cannot discover another's. Everything else forwards to the host's
// upstream resolvers.
package intdns

import (
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/miekg/dns"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	"github.com/zattera-dev/zattera/internal/daemon/proxy"
)

const (
	internalTTL    = 5
	forwardTimeout = 2 * time.Second
	internalZone   = "internal"
	defaultDNSPort = "53"
)

// NetworkScope binds one DNS listener: the bridge gateway to serve on and the
// (project, env) the network belongs to. The listener address determines the
// resolution scope.
type NetworkScope struct {
	Gateway   string // e.g. "10.201.5.1"
	ProjectID string
	EnvID     string
}

// Resolver serves internal DNS across a node's bridge networks.
type Resolver struct {
	routes    proxy.RouteSource
	upstreams []string
	log       *slog.Logger
	port      string

	mu      sync.Mutex
	servers map[string]*boundServer // key: gateway
}

type boundServer struct {
	udp, tcp *dns.Server
}

// New builds a resolver reading service VIPs from the route source and
// forwarding other queries to upstreams (host:port each). Empty upstreams are
// filled from /etc/resolv.conf.
func New(routes proxy.RouteSource, upstreams []string, log *slog.Logger) *Resolver {
	if log == nil {
		log = slog.Default()
	}
	if len(upstreams) == 0 {
		upstreams = systemUpstreams()
	}
	return &Resolver{routes: routes, upstreams: upstreams, log: log, port: defaultDNSPort, servers: map[string]*boundServer{}}
}

// Reconcile starts a DNS server for each scope's gateway and stops servers whose
// gateway is no longer present.
func (r *Resolver) Reconcile(scopes []NetworkScope) {
	want := make(map[string]NetworkScope, len(scopes))
	for _, s := range scopes {
		want[s.Gateway] = s
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for gw, scope := range want {
		if _, ok := r.servers[gw]; ok {
			continue
		}
		bs := r.startServer(scope)
		if bs != nil {
			r.servers[gw] = bs
		}
	}
	for gw, bs := range r.servers {
		if _, ok := want[gw]; !ok {
			bs.shutdown()
			delete(r.servers, gw)
		}
	}
}

// Close stops all listeners.
func (r *Resolver) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for gw, bs := range r.servers {
		bs.shutdown()
		delete(r.servers, gw)
	}
}

func (r *Resolver) startServer(scope NetworkScope) *boundServer {
	addr := net.JoinHostPort(scope.Gateway, r.port)
	handler := dns.HandlerFunc(func(w dns.ResponseWriter, req *dns.Msg) {
		_ = w.WriteMsg(r.Answer(scope, req))
	})
	bs := &boundServer{
		udp: &dns.Server{Addr: addr, Net: "udp", Handler: handler},
		tcp: &dns.Server{Addr: addr, Net: "tcp", Handler: handler},
	}
	go func() {
		if err := bs.udp.ListenAndServe(); err != nil {
			r.log.Warn("intdns: udp listen failed", "addr", addr, "err", err)
		}
	}()
	go func() {
		if err := bs.tcp.ListenAndServe(); err != nil {
			r.log.Warn("intdns: tcp listen failed", "addr", addr, "err", err)
		}
	}()
	return bs
}

func (bs *boundServer) shutdown() {
	_ = bs.udp.Shutdown()
	_ = bs.tcp.Shutdown()
}

// Answer builds the reply for a query received on the listener for scope. It is
// exported so it can be unit-tested without binding a socket.
func (r *Resolver) Answer(scope NetworkScope, req *dns.Msg) *dns.Msg {
	m := new(dns.Msg)
	m.SetReply(req)
	m.Authoritative = true
	m.RecursionAvailable = false // authoritative for .internal
	if len(req.Question) != 1 {
		m.Rcode = dns.RcodeFormatError
		return m
	}
	q := req.Question[0]
	name := strings.ToLower(strings.TrimSuffix(q.Name, "."))
	if isInternalName(name) {
		return r.answerInternal(scope, m, q, name)
	}
	return r.forward(req)
}

func (r *Resolver) answerInternal(scope NetworkScope, m *dns.Msg, q dns.Question, name string) *dns.Msg {
	svc := r.findService(scope, name)
	if svc == nil || svc.GetVip() == "" {
		m.Rcode = dns.RcodeNameError // NXDOMAIN (isolation / unknown)
		return m
	}
	if q.Qtype != dns.TypeA && q.Qtype != dns.TypeANY {
		return m // name exists but no record of this type (NOERROR, empty)
	}
	rr := &dns.A{
		Hdr: dns.RR_Header{Name: q.Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: internalTTL},
		A:   net.ParseIP(svc.GetVip()),
	}
	m.Answer = append(m.Answer, rr)
	return m
}

// findService resolves an internal name within a listener's scope. It honours
// isolation: a name in another (project,env) returns nil (NXDOMAIN) even if it
// exists elsewhere in the cluster.
func (r *Resolver) findService(scope NetworkScope, name string) *clusterv1.InternalService {
	labels := strings.Split(name, ".")
	snap := r.routes.Current()

	// Shorthand: "<svc>.internal" → the service named <svc> in this scope.
	if len(labels) == 2 {
		svc := labels[0]
		for _, is := range snap.GetInternalServices() {
			if is.GetProjectId() == scope.ProjectID && is.GetEnvironmentId() == scope.EnvID && firstLabel(is.GetFqdn()) == svc {
				return is
			}
		}
		return nil
	}

	// Full form: "<svc>.<env>.<project>.internal" — match the fqdn, then enforce
	// scope isolation.
	fqdn := name + "."
	for _, is := range snap.GetInternalServices() {
		if strings.EqualFold(is.GetFqdn(), fqdn) {
			if is.GetProjectId() == scope.ProjectID && is.GetEnvironmentId() == scope.EnvID {
				return is
			}
			return nil // exists but out of scope
		}
	}
	return nil
}

// forward relays a non-internal query to the upstream resolvers.
func (r *Resolver) forward(req *dns.Msg) *dns.Msg {
	c := &dns.Client{Timeout: forwardTimeout}
	for _, up := range r.upstreams {
		if resp, _, err := c.Exchange(req, up); err == nil && resp != nil {
			return resp
		}
	}
	m := new(dns.Msg)
	m.SetReply(req)
	m.Rcode = dns.RcodeServerFailure
	return m
}

func isInternalName(name string) bool {
	return name == internalZone || strings.HasSuffix(name, "."+internalZone)
}

func firstLabel(fqdn string) string {
	if i := strings.IndexByte(fqdn, '.'); i >= 0 {
		return strings.ToLower(fqdn[:i])
	}
	return strings.ToLower(fqdn)
}

// systemUpstreams reads /etc/resolv.conf, skipping loopback servers (the local
// resolver would loop) and appending :53.
func systemUpstreams() []string {
	cfg, err := dns.ClientConfigFromFile("/etc/resolv.conf")
	if err != nil {
		return nil
	}
	var out []string
	for _, s := range cfg.Servers {
		if ip := net.ParseIP(s); ip != nil && ip.IsLoopback() {
			continue
		}
		out = append(out, net.JoinHostPort(s, cfg.Port))
	}
	return out
}
