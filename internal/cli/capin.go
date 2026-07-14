package cli

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"net"
	"strings"
	"time"
)

// fetchPinnedCA connects to a Zattera API endpoint, verifies that the presented
// certificate chain contains a certificate whose SHA-256 matches pinHex (the CA
// fingerprint shown at cluster boot / embedded in join tokens), and returns that
// certificate as PEM. This is trust-on-first-use: no CA file needed out of band,
// but the operator must supply the pin they trust. Mirrors the node join flow's
// caPinCreds (internal/daemon/join.go).
func fetchPinnedCA(server, pinHex string) (string, error) {
	want, err := hex.DecodeString(strings.TrimSpace(pinHex))
	if err != nil || len(want) != sha256.Size {
		return "", fmt.Errorf("invalid --ca-pin (want a sha256 hex fingerprint)")
	}
	addr := serverHostPort(server)

	var matched []byte
	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true, // replaced by the pin check
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			for _, raw := range rawCerts {
				sum := sha256.Sum256(raw)
				if equalHash(sum[:], want) {
					matched = raw
					return nil
				}
			}
			return fmt.Errorf("server CA does not match the pin")
		},
	}
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", addr, cfg)
	if err != nil {
		return "", fmt.Errorf("connect %s: %w", addr, err)
	}
	_ = conn.Close()
	if matched == nil {
		return "", fmt.Errorf("no certificate in the chain matched the pin")
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: matched})), nil
}

// serverHostPort turns "https://host:8443" (or "host:8443") into "host:8443".
func serverHostPort(server string) string {
	s := server
	s = strings.TrimPrefix(s, "https://")
	s = strings.TrimPrefix(s, "http://")
	s = strings.TrimSuffix(s, "/")
	if !strings.Contains(s, ":") {
		s += ":8443"
	}
	return s
}

func equalHash(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var diff byte
	for i := range a {
		diff |= a[i] ^ b[i]
	}
	return diff == 0
}
