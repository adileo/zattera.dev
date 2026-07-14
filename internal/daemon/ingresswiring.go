package daemon

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"net/http"
	"time"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	"github.com/zattera-dev/zattera/internal/daemon/ca"
	"github.com/zattera-dev/zattera/internal/daemon/proxy"
	"github.com/zattera-dev/zattera/internal/daemon/scheduler"
	"github.com/zattera-dev/zattera/internal/daemon/tlsmgr"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// startIngress binds the HTTP and HTTPS ingress listeners and routes requests to
// app instances using the live RouteSnapshot (T-54). In single-node/dev the
// route source is the in-process RouteBuilder; multi-node ingress dials
// RouteService (wired later). HTTPS certs come from the TLS manager (dev: CA).
func startIngress(ctx context.Context, cfg configForIngress, rb *scheduler.RouteBuilder, authority *ca.CA, nodeID string, clk clock.Clock, log *slog.Logger) error {
	tm, err := tlsmgr.New(tlsmgr.Options{Dev: true, CA: authority, Logger: log})
	if err != nil {
		return err
	}
	src := routeBuilderSource{rb: rb}
	l7 := proxy.NewL7(src, nodeID, clk)
	l7.DisableHTTPSRedirect = true // dev: HTTPS is on a non-standard port, self-signed

	// HTTP listener (:8080 in dev): route L7 traffic; the ACME solver is a
	// passthrough in dev.
	httpSrv := &http.Server{
		Addr:              cfg.HTTPListen,
		Handler:           tm.HTTP01Handler(l7),
		ReadHeaderTimeout: 30 * time.Second,
	}
	// HTTPS listener (:9443 in dev): same routing under TLS.
	httpsSrv := &http.Server{
		Addr:              cfg.HTTPSListen,
		Handler:           l7,
		TLSConfig:         tm.GetTLSConfig(),
		ReadHeaderTimeout: 30 * time.Second,
	}

	httpLn, err := net.Listen("tcp", cfg.HTTPListen)
	if err != nil {
		return err
	}
	httpsLn, err := net.Listen("tcp", cfg.HTTPSListen)
	if err != nil {
		_ = httpLn.Close()
		return err
	}
	go func() {
		if err := httpSrv.Serve(httpLn); err != nil && err != http.ErrServerClosed {
			log.Warn("ingress http server stopped", "err", err)
		}
	}()
	go func() {
		if err := httpsSrv.Serve(tls.NewListener(httpsLn, tm.GetTLSConfig())); err != nil && err != http.ErrServerClosed {
			log.Warn("ingress https server stopped", "err", err)
		}
	}()
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(sctx)
		_ = httpsSrv.Shutdown(sctx)
	}()
	log.Info("ingress listening", "http", cfg.HTTPListen, "https", cfg.HTTPSListen)
	return nil
}

// configForIngress is the subset of config the ingress needs.
type configForIngress struct {
	HTTPListen  string
	HTTPSListen string
}

// routeBuilderSource adapts the in-process RouteBuilder to proxy.RouteSource.
type routeBuilderSource struct{ rb *scheduler.RouteBuilder }

func (s routeBuilderSource) Current() *clusterv1.RouteSnapshot { return s.rb.Current() }

func (s routeBuilderSource) Updates(ctx context.Context) <-chan *clusterv1.RouteSnapshot {
	id, ch := s.rb.Subscribe()
	out := make(chan *clusterv1.RouteSnapshot)
	go func() {
		defer s.rb.Unsubscribe(id)
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case snap, ok := <-ch:
				if !ok {
					return
				}
				select {
				case out <- snap:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}
