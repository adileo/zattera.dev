package tlsmgr

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"testing"

	"github.com/zattera-dev/zattera/internal/daemon/ca"
)

// staticHosts is a fixed CertHostSource.
type staticHosts []string

func (s staticHosts) CertHosts() []string { return s }

func TestDecisionFunc(t *testing.T) {
	m := &Manager{opts: Options{Hosts: staticHosts{"api.example.com", "app.example.com"}}}

	if err := m.decide(context.Background(), "api.example.com"); err != nil {
		t.Fatalf("known host should be allowed: %v", err)
	}
	if err := m.decide(context.Background(), "evil.example.com"); err == nil {
		t.Fatal("unknown host must be refused (no open cert factory)")
	}

	// No host source configured → refuse everything.
	empty := &Manager{}
	if err := empty.decide(context.Background(), "api.example.com"); err == nil {
		t.Fatal("nil host source must refuse issuance")
	}
}

func TestDevModeCertIssuance(t *testing.T) {
	authority, err := ca.LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m, err := New(Options{Dev: true, CA: authority})
	if err != nil {
		t.Fatal(err)
	}

	cfg := m.GetTLSConfig()
	if cfg.GetCertificate == nil {
		t.Fatal("dev TLS config must serve certs via GetCertificate")
	}

	cert, err := cfg.GetCertificate(&tls.ClientHelloInfo{ServerName: "api.example.com"})
	if err != nil {
		t.Fatalf("dev cert issuance: %v", err)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	// The cert covers the requested SNI host and chains to the cluster CA.
	if err := leaf.VerifyHostname("api.example.com"); err != nil {
		t.Fatalf("dev cert does not cover the host: %v", err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(authority.Certificate())
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}); err != nil {
		t.Fatalf("dev cert does not chain to the cluster CA: %v", err)
	}

	// Second request for the same host is served from cache (same leaf bytes).
	cert2, _ := cfg.GetCertificate(&tls.ClientHelloInfo{ServerName: "api.example.com"})
	if &cert2.Certificate[0][0] != &cert.Certificate[0][0] {
		t.Fatal("dev cert not cached per host")
	}
}

func TestDevRequiresCA(t *testing.T) {
	if _, err := New(Options{Dev: true}); err == nil {
		t.Fatal("dev mode without a CA must error")
	}
}

func TestProdRequiresStorage(t *testing.T) {
	if _, err := New(Options{}); err == nil {
		t.Fatal("production mode without storage must error")
	}
}
