package proxy

import (
	"encoding/json"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// L7 is the ingress HTTP reverse proxy: it matches a request to a route from
// the current RouteSnapshot, load-balances across the route's healthy endpoints
// (P2C), applies per-route middleware, and proxies. It serves both the :80 and
// :443 listeners (HTTPS redirect happens on the plaintext side).
type L7 struct {
	source RouteSource
	node   string // this node's id (local-endpoint preference)
	stats  *Stats
	lb     *balancer
	tr     *http.Transport
	clk    clock.Clock
	rnd    func(n int) int

	// activator parks and wakes requests for scaled-to-zero envs (T-70). Nil
	// disables wake-on-request (a no-endpoint route then just 502s).
	activator *Activator

	// DisableHTTPSRedirect serves plaintext requests directly instead of
	// 308-redirecting them to HTTPS. Single-node/dev sets this: HTTPS runs on a
	// non-standard port with a self-signed cert, so a bare https:// redirect
	// would be broken (T-54).
	DisableHTTPSRedirect bool

	cidrMu    sync.Mutex
	cidrVer   uint64
	cidrCache map[string][]*net.IPNet
}

// NewL7 builds an L7 proxy over a route source. node is this node's id (used to
// prefer node-local endpoints).
func NewL7(source RouteSource, node string, clk clock.Clock) *L7 {
	if clk == nil {
		clk = clock.Real{}
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = 64
	return &L7{
		source: source, node: node, stats: NewStats(clk), lb: newBalancer(node),
		tr: tr, clk: clk, rnd: rand.IntN, cidrCache: map[string][]*net.IPNet{},
	}
}

// Stats exposes the metrics accumulator (heartbeat sampling).
func (p *L7) Stats() *Stats { return p.stats }

// SetActivator enables wake-on-request for scaled-to-zero envs (T-70).
func (p *L7) SetActivator(a *Activator) { p.activator = a }

// Activator returns the configured activator (nil if wake-on-request is off).
func (p *L7) Activator() *Activator { return p.activator }

func (p *L7) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	snap := p.source.Current()
	route := matchHTTP(snap, r)
	if route == nil {
		writeProxyError(w, http.StatusNotFound, "no route for host")
		return
	}

	// HTTPS redirect on the plaintext listener.
	if r.TLS == nil && !p.DisableHTTPSRedirect && redirectHTTPS(route) {
		target := "https://" + stripPort(r.Host) + r.URL.RequestURI()
		http.Redirect(w, r, target, http.StatusPermanentRedirect)
		return
	}

	mw := route.GetMiddleware()
	if !allowedIP(r.RemoteAddr, p.cidrs(snap, route)) {
		writeProxyError(w, http.StatusForbidden, "forbidden")
		return
	}
	if !checkBasicAuth(r, mw.GetBasicAuth()) {
		w.Header().Set("WWW-Authenticate", `Basic realm="zattera"`)
		writeProxyError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if mb := mw.GetMaxBodyBytes(); mb > 0 {
		r.Body = http.MaxBytesReader(w, r.Body, int64(mb))
	}

	ep, setCookie := p.selectEndpoint(route, r)
	if ep == nil {
		// Scale-to-zero: park the request, wake the env, and retry once an
		// endpoint appears (T-70). Anything else is a genuine bad gateway.
		if route.GetScaleToZero() && p.activator != nil {
			if !p.activator.Hold(w, r, route.GetEnvironmentId(), route.GetHostname()) {
				return // activator wrote the response (shed / timeout / oversized)
			}
			snap = p.source.Current()
			if route = matchHTTP(snap, r); route == nil {
				writeProxyError(w, http.StatusNotFound, "no route for host")
				return
			}
			ep, setCookie = p.selectEndpoint(route, r)
		}
		if ep == nil {
			writeProxyError(w, http.StatusBadGateway, "no healthy endpoint")
			return
		}
	}
	if setCookie != "" {
		http.SetCookie(w, &http.Cookie{
			Name: stickyCookie, Value: setCookie, Path: "/",
			HttpOnly: true, SameSite: http.SameSiteLaxMode, Secure: r.TLS != nil,
		})
	}

	release := p.lb.acquire(ep.GetAddr())
	env := route.GetEnvironmentId()
	start := p.clk.Now()
	p.stats.begin(env)
	rw := newRespWriter(w, wantsGzip(r, mw.GetCompress()))
	defer func() {
		rw.finish()
		release()
		p.stats.end(env, float64(p.clk.Now().Sub(start).Milliseconds()), rw.status >= 500)
	}()

	rp := &httputil.ReverseProxy{
		Transport: p.tr,
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(&url.URL{Scheme: "http", Host: ep.GetAddr()})
			pr.SetXForwarded()
			pr.Out.Host = pr.In.Host // preserve the virtual host
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, _ error) {
			writeProxyError(w, http.StatusBadGateway, "upstream error")
		},
	}
	rp.ServeHTTP(rw, r)
}

// selectEndpoint chooses the backend for a request. With sticky sessions, a
// request carrying a valid affinity cookie reuses its pinned endpoint (when
// still healthy in the current snapshot); otherwise it picks via P2C and, for a
// sticky route, returns the cookie value to pin future requests. cookieVal is ""
// when no Set-Cookie should be written.
func (p *L7) selectEndpoint(route *clusterv1.HTTPRoute, r *http.Request) (ep *clusterv1.Endpoint, cookieVal string) {
	sticky := route.GetMiddleware().GetStickySessions()
	if sticky {
		if ck, err := r.Cookie(stickyCookie); err == nil {
			if e := healthyByStickyID(route.GetEndpoints(), ck.Value); e != nil {
				return e, "" // honor the existing pin; client already has the cookie
			}
		}
	}
	ep = p.lb.pick(route.GetEndpoints(), p.rnd)
	if ep == nil {
		return nil, ""
	}
	if sticky {
		return ep, stickyID(ep) // pin future requests to this endpoint
	}
	return ep, ""
}

// matchHTTP finds the route for a request: exact host (port stripped), then the
// longest matching path_prefix.
func matchHTTP(snap *clusterv1.RouteSnapshot, r *http.Request) *clusterv1.HTTPRoute {
	host := strings.ToLower(stripPort(r.Host))
	var best *clusterv1.HTTPRoute
	for _, rt := range snap.GetHttpRoutes() {
		if !strings.EqualFold(rt.GetHostname(), host) {
			continue
		}
		if !strings.HasPrefix(r.URL.Path, rt.GetPathPrefix()) {
			continue
		}
		if best == nil || len(rt.GetPathPrefix()) > len(best.GetPathPrefix()) {
			best = rt
		}
	}
	return best
}

// cidrs returns the parsed allowlist for a route, cached per snapshot version.
func (p *L7) cidrs(snap *clusterv1.RouteSnapshot, route *clusterv1.HTTPRoute) []*net.IPNet {
	list := route.GetMiddleware().GetIpAllowlist()
	if len(list) == 0 {
		return nil
	}
	key := route.GetHostname() + "|" + route.GetPathPrefix()
	p.cidrMu.Lock()
	defer p.cidrMu.Unlock()
	if snap.GetVersion() != p.cidrVer {
		p.cidrVer = snap.GetVersion()
		p.cidrCache = map[string][]*net.IPNet{}
	}
	if c, ok := p.cidrCache[key]; ok {
		return c
	}
	c := parseCIDRs(list)
	p.cidrCache[key] = c
	return c
}

// redirectHTTPS reports whether plaintext requests should be 308-redirected to
// HTTPS. A route with no middleware redirects by default; with middleware, the
// stored redirect_https flag governs.
func redirectHTTPS(route *clusterv1.HTTPRoute) bool {
	mw := route.GetMiddleware()
	if mw == nil {
		return true
	}
	return mw.GetRedirectHttps()
}

func stripPort(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		return h
	}
	return host
}

func writeProxyError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
