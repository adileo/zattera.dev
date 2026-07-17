package daemon

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/api"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
)

const (
	defaultOrgName   = "default"
	adminEmail       = "admin@local"
	initialKeyVer    = 1
	bootstrapTokenNm = "bootstrap"
)

// BootstrapOptions configures first-boot initialization.
type BootstrapOptions struct {
	// Out receives the one-time human-readable token/passphrase prints. When
	// nil, os.Stdout is used.
	Out io.Writer
	// RecoveryPassphraseFile, when set, supplies the recovery passphrase
	// instead of generating a random one (its trimmed contents are used).
	RecoveryPassphraseFile string
	Logger                 *slog.Logger
}

// Bootstrap performs first-boot initialization on the leader: it creates the
// org, the admin user, a personal bootstrap token, and the sealed cluster key,
// then returns the in-memory keyring. It is a no-op when the org already
// exists (a restart must not print a new token); in that case it returns a nil
// keyring, since the plaintext data key lives only in the memory of the
// process that created it (unseal-on-restart is a later flow).
//
// Bootstrap must run only on the leader; callers gate on IsLeader.
func Bootstrap(ctx context.Context, rs *raftstore.Store, opts BootstrapOptions) (*secrets.Keyring, error) {
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	out := opts.Out
	if out == nil {
		out = os.Stdout
	}

	if _, ok := rs.State().Org(); ok {
		log.Info("bootstrap skipped: cluster already initialized")
		return nil, nil
	}

	now := timestamppb.Now()
	orgID := ids.New()
	adminID := ids.New()

	// 1) Org.
	if err := apply(ctx, rs, &clusterv1.Command{Mutation: &clusterv1.Command_PutOrg{PutOrg: &clusterv1.PutOrg{
		Org: &zatterav1.Org{Meta: newMeta(orgID, now), Name: defaultOrgName},
	}}}); err != nil {
		return nil, fmt.Errorf("bootstrap: org: %w", err)
	}

	// 2) Admin user (owner). Password hash is a random placeholder until a real
	// password is set via CreateUser (T-04); the bootstrap token is the entry.
	placeholder, err := randomHex(32)
	if err != nil {
		return nil, err
	}
	if err := apply(ctx, rs, &clusterv1.Command{Mutation: &clusterv1.Command_PutUser{PutUser: &clusterv1.PutUser{
		User: &zatterav1.User{
			Meta:         newMeta(adminID, now),
			Email:        adminEmail,
			DisplayName:  "Administrator",
			PasswordHash: placeholder,
			OrgId:        orgID,
			OrgRole:      zatterav1.Role_ROLE_OWNER,
		},
	}}}); err != nil {
		return nil, fmt.Errorf("bootstrap: admin user: %w", err)
	}

	// 3) Bootstrap personal token for the admin.
	tokenStr, secretHash, err := api.MintToken()
	if err != nil {
		return nil, fmt.Errorf("bootstrap: token: %w", err)
	}
	if err := apply(ctx, rs, &clusterv1.Command{Mutation: &clusterv1.Command_PutToken{PutToken: &clusterv1.PutToken{
		Token: &zatterav1.Token{
			Meta:       newMeta(ids.New(), now),
			UserId:     adminID,
			Name:       bootstrapTokenNm,
			SecretHash: secretHash,
			Kind:       zatterav1.TokenKind_TOKEN_KIND_PERSONAL,
		},
	}}}); err != nil {
		return nil, fmt.Errorf("bootstrap: put token: %w", err)
	}

	// 4) Cluster data key, sealed under the recovery passphrase.
	dataKey, err := secrets.GenerateDataKey()
	if err != nil {
		return nil, fmt.Errorf("bootstrap: data key: %w", err)
	}
	passphrase, err := recoveryPassphrase(opts.RecoveryPassphraseFile)
	if err != nil {
		return nil, err
	}
	material, err := secrets.SealDataKey(dataKey, passphrase, initialKeyVer)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: seal data key: %w", err)
	}
	if err := apply(ctx, rs, &clusterv1.Command{Mutation: &clusterv1.Command_PutClusterKeyMaterial{
		PutClusterKeyMaterial: &clusterv1.PutClusterKeyMaterial{Material: material},
	}}); err != nil {
		return nil, fmt.Errorf("bootstrap: put cluster key: %w", err)
	}
	keyring, err := secrets.NewKeyring(dataKey, initialKeyVer)
	if err != nil {
		return nil, err
	}

	// 5) Built-in alert rules (T-74) — deletable defaults, no channels attached
	// until the operator adds one.
	for _, r := range defaultAlertRules(now) {
		if err := apply(ctx, rs, &clusterv1.Command{Mutation: &clusterv1.Command_PutAlertRule{
			PutAlertRule: &clusterv1.PutAlertRule{Rule: r},
		}}); err != nil {
			return nil, fmt.Errorf("bootstrap: alert rule %q: %w", r.GetName(), err)
		}
	}

	// 6) One-time human-readable output — stdout only, never the logger.
	fmt.Fprintf(out, "Bootstrap admin token: %s\n", tokenStr)
	fmt.Fprintf(out, "Recovery passphrase (STORE THIS SAFELY): %s\n", passphrase)

	log.Info("cluster bootstrapped", "org", defaultOrgName, "admin", adminEmail)
	return keyring, nil
}

// defaultAlertRules are the built-in rules created at bootstrap (T-74). They
// carry no channels until an operator attaches one, and are deletable.
func defaultAlertRules(now *timestamppb.Timestamp) []*zatterav1.AlertRule {
	event := func(name, kind string) *zatterav1.AlertRule {
		return &zatterav1.AlertRule{Meta: newMeta(ids.New(), now), Name: name, EventKind: kind}
	}
	return []*zatterav1.AlertRule{
		event("deploy-failed", "deploy.failed"),
		event("node-down", "node.down"),
		event("cert-renew-failed", "cert.renew_failed"),
		event("backup-failed", "backup.failed"),
		{
			Meta: newMeta(ids.New(), now), Name: "disk-full",
			Metric: &zatterav1.MetricCondition{
				Metric: "disk_percent", Scope: "cluster", Op: ">", Threshold: 90,
				Sustained: durationpb.New(5 * time.Minute),
			},
		},
	}
}

// apply stamps a command with request id / actor / time and proposes it
// through raft. The caller sets cmd.Mutation.
func apply(ctx context.Context, rs *raftstore.Store, cmd *clusterv1.Command) error {
	cmd.RequestId = ids.New()
	cmd.Actor = "system:bootstrap"
	cmd.Time = timestamppb.Now()
	return rs.Apply(ctx, cmd)
}

func newMeta(id string, now *timestamppb.Timestamp) *zatterav1.Meta {
	return &zatterav1.Meta{Id: id, CreatedAt: now, UpdatedAt: now}
}

func recoveryPassphrase(file string) (string, error) {
	if file == "" {
		p, err := secrets.GeneratePassphrase()
		if err != nil {
			return "", fmt.Errorf("bootstrap: passphrase: %w", err)
		}
		return p, nil
	}
	b, err := os.ReadFile(file)
	if err != nil {
		return "", fmt.Errorf("bootstrap: read passphrase file: %w", err)
	}
	p := strings.TrimSpace(string(b))
	if p == "" {
		return "", fmt.Errorf("bootstrap: passphrase file %s is empty", file)
	}
	return p, nil
}

func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("bootstrap: entropy: %w", err)
	}
	return hex.EncodeToString(b), nil
}
