package daemon

import (
	"bytes"
	"context"
	"regexp"
	"testing"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/api"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
)

var tokenLine = regexp.MustCompile(`Bootstrap admin token: (zpat_\S+)`)

func TestBootstrap(t *testing.T) {
	rs := raftstore.NewTestStore(t)
	ctx := context.Background()

	var out bytes.Buffer
	keyring, err := Bootstrap(ctx, rs, BootstrapOptions{Out: &out})
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if keyring == nil {
		t.Fatal("expected a keyring on first bootstrap")
	}

	st := rs.State()

	// Org + admin user exist.
	org, ok := st.Org()
	if !ok || org.GetName() != defaultOrgName {
		t.Fatalf("org = %+v, ok=%v", org, ok)
	}
	admin, ok := st.UserByEmail(adminEmail)
	if !ok {
		t.Fatal("admin user not created")
	}
	if admin.GetOrgRole() != zatterav1.Role_ROLE_OWNER {
		t.Errorf("admin role = %v, want OWNER", admin.GetOrgRole())
	}
	if admin.GetPasswordHash() == "" {
		t.Error("admin password hash placeholder empty")
	}

	// The printed token hashes to the stored SecretHash.
	m := tokenLine.FindStringSubmatch(out.String())
	if m == nil {
		t.Fatalf("no token line in output:\n%s", out.String())
	}
	printedToken := m[1]
	tok, ok := st.TokenByHash(api.HashToken(printedToken))
	if !ok {
		t.Fatal("printed token hash does not resolve to a stored token")
	}
	if tok.GetUserId() != admin.GetMeta().GetId() {
		t.Error("bootstrap token not owned by admin")
	}
	if tok.GetKind() != zatterav1.TokenKind_TOKEN_KIND_PERSONAL {
		t.Errorf("token kind = %v, want PERSONAL", tok.GetKind())
	}

	// Recovery passphrase is printed and the cluster key material is stored.
	if !bytes.Contains(out.Bytes(), []byte("Recovery passphrase")) {
		t.Error("recovery passphrase not printed")
	}
	if _, ok := st.ClusterKeyMaterial(); !ok {
		t.Error("cluster key material not stored")
	}
	// The keyring's data key unseals against the sealed material.
	sealer, err := keyring.Sealer()
	if err != nil {
		t.Fatalf("sealer: %v", err)
	}
	ev, err := sealer.Seal([]byte("hello"))
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	pt, err := sealer.Open(ev)
	if err != nil || string(pt) != "hello" {
		t.Fatalf("seal round-trip: pt=%q err=%v", pt, err)
	}
}

func TestBootstrapIdempotent(t *testing.T) {
	rs := raftstore.NewTestStore(t)
	ctx := context.Background()

	var out1 bytes.Buffer
	if _, err := Bootstrap(ctx, rs, BootstrapOptions{Out: &out1}); err != nil {
		t.Fatalf("first bootstrap: %v", err)
	}
	tokensAfterFirst := len(rs.State().ListTokens(""))

	// Second run is a no-op: no new token, no new print.
	var out2 bytes.Buffer
	keyring, err := Bootstrap(ctx, rs, BootstrapOptions{Out: &out2})
	if err != nil {
		t.Fatalf("second bootstrap: %v", err)
	}
	if keyring != nil {
		t.Error("second bootstrap should not return a keyring")
	}
	if out2.Len() != 0 {
		t.Errorf("second bootstrap printed output: %q", out2.String())
	}
	if got := len(rs.State().ListTokens("")); got != tokensAfterFirst {
		t.Errorf("token count changed on restart: %d → %d", tokensAfterFirst, got)
	}
}
