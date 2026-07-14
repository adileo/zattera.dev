package api

import (
	"context"
	"crypto/tls"
	"net"
	"testing"

	"github.com/zattera-dev/zattera/internal/daemon/ca"
)

// TestServerACME verifies the API serves the ACME/public certificate only for
// the configured public hostname SNI, and the cluster-CA cert for every other
// SNI (loopback, mesh IPs, node mTLS).
func TestServerACME(t *testing.T) {
	authority, err := ca.LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// Stand-in "public" cert from an unrelated CA, with a unique DNS name.
	pubCA, err := ca.LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	pubLeaf, err := pubCA.IssueServer([]string{"api.example.com"}, nil, ca.NodeCertTTL)
	if err != nil {
		t.Fatal(err)
	}
	pubCert, err := pubLeaf.TLSCertificate(pubCA.CABundlePEM())
	if err != nil {
		t.Fatal(err)
	}

	srv, err := New(Options{
		CA:             authority,
		Listen:         "127.0.0.1:0",
		DNSNames:       []string{"localhost"},
		IPs:            []net.IP{net.ParseIP("127.0.0.1")},
		PublicHostname: "api.example.com",
		PublicCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return &pubCert, nil
		},
	})
	if err != nil {
		t.Fatalf("api.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = srv.Serve(ctx) }()
	addr := srv.Addr().String()
	waitReady(t, addr, authority.Pool())

	// SNI = public hostname → the public (unrelated-CA) cert.
	if dns := peerLeafDNS(t, addr, "api.example.com"); !contains(dns, "api.example.com") {
		t.Fatalf("public SNI served %v, want the api.example.com cert", dns)
	}
	// SNI = loopback → the cluster CA server cert (not the public one).
	if dns := peerLeafDNS(t, addr, "localhost"); contains(dns, "api.example.com") {
		t.Fatalf("loopback SNI served the public cert (%v), want the CA cert", dns)
	}
}

// peerLeafDNS dials addr with the given SNI and returns the presented leaf's
// DNS SANs.
func peerLeafDNS(t *testing.T, addr, sni string) []string {
	t.Helper()
	conn, err := tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true, ServerName: sni})
	if err != nil {
		t.Fatalf("dial %s (sni %s): %v", addr, sni, err)
	}
	defer func() { _ = conn.Close() }()
	certs := conn.ConnectionState().PeerCertificates
	if len(certs) == 0 {
		t.Fatal("no peer certificates")
	}
	return certs[0].DNSNames
}

func contains(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}
