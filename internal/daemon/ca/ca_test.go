package ca

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadOrCreatePersistsAndReloads(t *testing.T) {
	dir := t.TempDir()
	c1, err := LoadOrCreate(dir)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	// Files exist with 0600.
	for _, name := range []string{"ca.crt", "ca.key"} {
		fi, err := os.Stat(filepath.Join(dir, "ca", name))
		if err != nil {
			t.Fatalf("stat %s: %v", name, err)
		}
		if fi.Mode().Perm() != 0o600 {
			t.Errorf("%s mode = %v, want 0600", name, fi.Mode().Perm())
		}
	}
	c2, err := LoadOrCreate(dir)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if !c1.cert.Equal(c2.cert) {
		t.Error("reloaded CA cert differs from created one")
	}
	if c1.cert.Subject.CommonName != rootCommonName {
		t.Errorf("root CN = %q, want %q", c1.cert.Subject.CommonName, rootCommonName)
	}
	if !c1.cert.IsCA {
		t.Error("root cert is not marked IsCA")
	}
}

func TestLoadOrCreateInconsistentMaterialFailsLoudly(t *testing.T) {
	dir := t.TempDir()
	if _, err := LoadOrCreate(dir); err != nil {
		t.Fatalf("create: %v", err)
	}
	// Remove only the key: a regeneration here would brick trust.
	if err := os.Remove(filepath.Join(dir, "ca", "ca.key")); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadOrCreate(dir); err == nil {
		t.Fatal("expected error on inconsistent CA material, got nil")
	}
}

func TestLoadOrCreateCorruptKeyFailsLoudly(t *testing.T) {
	dir := t.TempDir()
	if _, err := LoadOrCreate(dir); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ca", "ca.key"), []byte("garbage"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadOrCreate(dir); err == nil {
		t.Fatal("expected error on corrupt key, got nil")
	}
}

func TestIssueServerChainVerifies(t *testing.T) {
	c, err := LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := c.IssueServer([]string{"localhost", "api.zattera.test"}, []net.IP{net.ParseIP("127.0.0.1")}, time.Hour)
	if err != nil {
		t.Fatalf("issue server: %v", err)
	}
	verifyChain(t, c, leaf, x509.ExtKeyUsageServerAuth)

	if err := leaf.cert.VerifyHostname("api.zattera.test"); err != nil {
		t.Errorf("hostname verify: %v", err)
	}
	if len(leaf.cert.IPAddresses) != 1 || !leaf.cert.IPAddresses[0].Equal(net.ParseIP("127.0.0.1")) {
		t.Errorf("server cert missing 127.0.0.1 SAN: %v", leaf.cert.IPAddresses)
	}
}

func TestIssueNodeHasURISAN(t *testing.T) {
	c, err := LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nodeID := "01ABCNODE"
	leaf, err := c.IssueNode(nodeID, net.ParseIP("10.90.0.1"), NodeCertTTL)
	if err != nil {
		t.Fatalf("issue node: %v", err)
	}
	verifyChain(t, c, leaf, x509.ExtKeyUsageClientAuth)
	verifyChain(t, c, leaf, x509.ExtKeyUsageServerAuth)

	want := nodeURISAN + nodeID
	if len(leaf.cert.URIs) != 1 || leaf.cert.URIs[0].String() != want {
		t.Errorf("node URI SAN = %v, want %q", leaf.cert.URIs, want)
	}
	if leaf.cert.DNSNames[0] != "node-"+nodeID {
		t.Errorf("node DNS SAN = %v, want node-%s", leaf.cert.DNSNames, nodeID)
	}
	// 1y TTL (within a small tolerance).
	gotTTL := leaf.cert.NotAfter.Sub(leaf.cert.NotBefore)
	if gotTTL < NodeCertTTL || gotTTL > NodeCertTTL+10*time.Minute {
		t.Errorf("node TTL = %v, want ~%v", gotTTL, NodeCertTTL)
	}
}

func TestSignCSR(t *testing.T) {
	c, err := LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	// Attacker requests SANs we must ignore.
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject:  pkix.Name{CommonName: "attacker"},
		DNSNames: []string{"evil.example.com"},
	}, key)
	if err != nil {
		t.Fatal(err)
	}
	csrPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER})

	nodeID := "01NODE7"
	certPEM, err := c.SignCSR(csrPEM, nodeID, net.ParseIP("10.90.1.1"), NodeCertTTL)
	if err != nil {
		t.Fatalf("sign csr: %v", err)
	}
	block, _ := pem.Decode(certPEM)
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	// Imposed SANs, requested ones dropped.
	if got := cert.URIs[0].String(); got != nodeURISAN+nodeID {
		t.Errorf("URI SAN = %q, want %q", got, nodeURISAN+nodeID)
	}
	for _, d := range cert.DNSNames {
		if d == "evil.example.com" {
			t.Error("signed CSR retained attacker-requested SAN")
		}
	}
}

func TestSignCSRRejectsBadSignature(t *testing.T) {
	c, err := LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: "node"},
	}, key)
	if err != nil {
		t.Fatal(err)
	}
	// Corrupt the signature bytes (last byte) to break CheckSignature.
	csrDER[len(csrDER)-1] ^= 0xff
	csrPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER})
	if _, err := c.SignCSR(csrPEM, "node", nil, NodeCertTTL); err == nil {
		t.Fatal("expected bad-signature CSR to be rejected")
	}
}

func TestServerTLSConfig(t *testing.T) {
	c, err := LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := c.ServerTLSConfig([]string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion = %d, want TLS 1.2", cfg.MinVersion)
	}
	if len(cfg.Certificates) != 1 {
		t.Fatalf("want 1 server cert, got %d", len(cfg.Certificates))
	}
	if cfg.ClientCAs == nil {
		t.Error("ClientCAs not set (mTLS clients cannot be verified)")
	}
}

// verifyChain checks the leaf chains to the CA root for the given usage.
func verifyChain(t *testing.T, c *CA, leaf *Leaf, usage x509.ExtKeyUsage) {
	t.Helper()
	_, err := leaf.cert.Verify(x509.VerifyOptions{
		Roots:     c.pool,
		KeyUsages: []x509.ExtKeyUsage{usage},
	})
	if err != nil {
		t.Fatalf("chain verify (usage %v): %v", usage, err)
	}
}
