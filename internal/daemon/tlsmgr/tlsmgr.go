package tlsmgr

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/caddyserver/certmagic"
	"go.uber.org/zap"

	"github.com/zattera-dev/zattera/internal/daemon/ca"
)

// devCertTTL is how long a dev self-signed cert is valid.
const devCertTTL = 90 * 24 * time.Hour

// CertHostSource reports the hostnames the on-demand issuer is allowed to serve
// (the RouteSnapshot's cert_hosts). Without it, on-demand ACME would be an open
// certificate factory.
type CertHostSource interface {
	CertHosts() []string
}

// Options configures the TLS manager.
type Options struct {
	// Dev (or Disabled ACME) mints self-signed certs from the cluster CA on
	// demand instead of dialing ACME.
	Dev bool
	// CA is the cluster CA used for dev certs (required in dev mode).
	CA *ca.CA
	// Storage backs certmagic in production (the raft KV).
	Storage certmagic.Storage
	// Hosts gates on-demand issuance to known route hostnames.
	Hosts CertHostSource
	// Email, Staging configure the ACME account (production).
	Email   string
	Staging bool
	Logger  *slog.Logger
	// EmitEvent records a cluster event (T-109). Optional; nil disables event
	// emission and failures are logged only. Note that only the raft leader can
	// append events, so on a follower this is best-effort — see T-110.
	EmitEvent func(kind, severity, message string)
}

// Manager issues and serves TLS certificates for the ingress :443 listener and
// mounts the ACME HTTP-01 solver on :80.
type Manager struct {
	opts   Options
	dev    bool
	magic  *certmagic.Config
	issuer *certmagic.ACMEIssuer

	mu       sync.Mutex
	devCerts map[string]*tls.Certificate
}

// New builds a TLS manager. In dev mode only the cluster CA is used; otherwise
// certmagic is configured for on-demand ACME over the given storage.
func New(opts Options) (*Manager, error) {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	m := &Manager{opts: opts, dev: opts.Dev, devCerts: map[string]*tls.Certificate{}}
	if m.dev {
		if opts.CA == nil {
			return nil, fmt.Errorf("tlsmgr: dev mode requires a cluster CA")
		}
		return m, nil
	}
	if opts.Storage == nil {
		return nil, fmt.Errorf("tlsmgr: production mode requires storage")
	}

	var cfg *certmagic.Config
	cache := certmagic.NewCache(certmagic.CacheOptions{
		GetConfigForCert: func(certmagic.Certificate) (*certmagic.Config, error) { return cfg, nil },
	})
	cfg = certmagic.New(cache, certmagic.Config{
		Storage:  opts.Storage,
		OnDemand: &certmagic.OnDemandConfig{DecisionFunc: m.decide},
		Logger:   zap.NewNop(),
		OnEvent:  m.onCertMagicEvent,
	})
	caURL := certmagic.LetsEncryptProductionCA
	if opts.Staging {
		caURL = certmagic.LetsEncryptStagingCA
	}
	m.issuer = certmagic.NewACMEIssuer(cfg, certmagic.ACMEIssuer{
		CA: caURL, Email: opts.Email, Agreed: true,
	})
	cfg.Issuers = []certmagic.Issuer{m.issuer}
	m.magic = cfg
	return m, nil
}

// onCertMagicEvent turns a failed certificate renewal into a cluster event so
// the built-in cert-renew-failed alert rule can fire (T-109).
//
// Only renewals are reported. A failed *initial* issuance is a different
// condition — a domain that never worked, usually a DNS or firewall mistake
// visible the moment it is added — and it has no documented event kind, so
// mapping it onto cert.renew_failed would misreport it.
//
// Always returns nil: certmagic treats a non-nil error from OnEvent as a veto
// of the operation, and observing a failure must never cause one.
func (m *Manager) onCertMagicEvent(_ context.Context, event string, data map[string]any) error {
	if event != "cert_failed" {
		return nil
	}
	renewal, _ := data["renewal"].(bool)
	if !renewal {
		return nil
	}
	name, _ := data["identifier"].(string)
	msg := "certificate renewal failed for " + name
	if err, ok := data["error"].(error); ok && err != nil {
		msg += ": " + err.Error()
	}
	// Logged unconditionally: on a follower the event cannot reach the log
	// (raft rejects non-leader appends), so this line is the only trace.
	m.opts.Logger.Error("certificate renewal failed", "host", name, "detail", msg)
	if m.opts.EmitEvent != nil {
		m.opts.EmitEvent("cert.renew_failed", "error", msg)
	}
	return nil
}

// GetTLSConfig returns the *tls.Config for the :443 listener.
func (m *Manager) GetTLSConfig() *tls.Config {
	if m.dev {
		return &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: m.devGetCertificate}
	}
	// certmagic's TLSConfig only advertises its own ALPN (acme-tls/1); without
	// the HTTP protocols every client that offers ALPN — all browsers — fails
	// the handshake with "no application protocol".
	cfg := m.magic.TLSConfig()
	cfg.NextProtos = append([]string{"h2", "http/1.1"}, cfg.NextProtos...)
	return cfg
}

// HTTP01Handler wraps h with the ACME HTTP-01 challenge solver for the :80
// listener. In dev mode it is a passthrough (no ACME).
func (m *Manager) HTTP01Handler(h http.Handler) http.Handler {
	if m.dev || m.issuer == nil {
		return h
	}
	return m.issuer.HTTPChallengeHandler(h)
}

// decide is certmagic's on-demand gate: only issue for a hostname currently in
// the route table's cert_hosts.
func (m *Manager) decide(_ context.Context, name string) error {
	if m.opts.Hosts == nil {
		return fmt.Errorf("tlsmgr: no host source; refusing on-demand issuance for %q", name)
	}
	for _, h := range m.opts.Hosts.CertHosts() {
		if h == name {
			return nil
		}
	}
	return fmt.Errorf("tlsmgr: hostname %q not in the route table", name)
}

// devGetCertificate mints (and caches) a cluster-CA-signed cert per SNI host.
func (m *Manager) devGetCertificate(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
	name := hello.ServerName
	if name == "" {
		name = "localhost"
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.devCerts[name]; ok {
		return c, nil
	}
	leaf, err := m.opts.CA.IssueServer([]string{name}, nil, devCertTTL)
	if err != nil {
		return nil, err
	}
	tc, err := leaf.TLSCertificate(m.opts.CA.CABundlePEM())
	if err != nil {
		return nil, err
	}
	m.devCerts[name] = &tc
	return &tc, nil
}
