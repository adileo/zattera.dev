package cli

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"net"
	"strings"
	"testing"

	"github.com/zattera-dev/zattera/internal/daemon/ca"
)

// TestLoginPin verifies trust-on-first-use: fetchPinnedCA returns the cluster CA
// when the pin matches a cert in the presented chain, and rejects a mismatch.
func TestLoginPin(t *testing.T) {
	authority, err := ca.LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// A server cert whose chain is [leaf, CA] (how the API server presents it).
	leaf, err := authority.IssueServer([]string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")}, ca.NodeCertTTL)
	if err != nil {
		t.Fatal(err)
	}
	tlsCert, err := leaf.TLSCertificate(authority.CABundlePEM())
	if err != nil {
		t.Fatal(err)
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{tlsCert}})
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
			_ = c.(*tls.Conn).Handshake()
			_ = c.Close()
		}
	}()
	addr := ln.Addr().String()

	// The CA's DER fingerprint (what boot / a join token would print).
	caCert := authority.Certificate()
	sum := sha256.Sum256(caCert.Raw)
	pin := hex.EncodeToString(sum[:])

	// Correct pin → returns the CA PEM.
	pemStr, err := fetchPinnedCA("https://"+addr, pin)
	if err != nil {
		t.Fatalf("fetchPinnedCA: %v", err)
	}
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil {
		t.Fatal("returned value is not PEM")
	}
	got, err := x509.ParseCertificate(block.Bytes)
	if err != nil || !got.Equal(caCert) {
		t.Fatalf("returned cert is not the cluster CA: %v", err)
	}

	// Wrong pin → rejected.
	bad := strings.Repeat("00", sha256.Size)
	if _, err := fetchPinnedCA("https://"+addr, bad); err == nil {
		t.Fatal("expected a pin mismatch error")
	}

	// Malformed pin → rejected.
	if _, err := fetchPinnedCA("https://"+addr, "not-hex"); err == nil {
		t.Fatal("expected an invalid-pin error")
	}
}
