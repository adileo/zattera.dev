package api

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

// nodeURIScheme is the prefix of the node identity URI SAN.
const nodeURIScheme = "zattera://node/"

// Applier proposes commands through raft and reports leadership.
// *raftstore.Store implements it; T-08 wraps it with leader-forwarding.
type Applier interface {
	Apply(ctx context.Context, cmd *clusterv1.Command) error
	IsLeader() bool
}

// Identity is the authenticated caller, resolved by the auth interceptor and
// stored in the request context. Exactly one of UserID / NodeID is set.
type Identity struct {
	UserID  string
	NodeID  string
	OrgRole zatterav1.Role
}

// IsNode reports whether the caller is a cluster node (mTLS).
func (i Identity) IsNode() bool { return i.NodeID != "" }

// Actor returns a stable actor string for audit/command attribution.
func (i Identity) Actor() string {
	switch {
	case i.NodeID != "":
		return "node:" + i.NodeID
	case i.UserID != "":
		return "user:" + i.UserID
	default:
		return "anonymous"
	}
}

type identityKey struct{}

// IdentityFrom returns the caller identity placed by the auth interceptor.
func IdentityFrom(ctx context.Context) (Identity, bool) {
	id, ok := ctx.Value(identityKey{}).(Identity)
	return id, ok
}

func withIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, identityKey{}, id)
}

// Requirement is the auth tier a method demands.
type Requirement int

const (
	// reqDenied is the zero value: an unlisted method is denied (fail closed).
	reqDenied Requirement = iota
	reqPublic             // no identity needed (Login, Join)
	reqUser               // any authenticated user
	reqNode               // a cluster node (mTLS)
	reqAdmin              // org owner/admin
)

// Authenticator resolves identity and enforces the method policy table. It
// also batches token last_used_at updates.
type Authenticator struct {
	store *state.Store
	raft  Applier
	clock clock.Clock

	mu       sync.Mutex
	lastUsed map[string]time.Time // token id → newest use
}

// NewAuthenticator builds the auth interceptor.
func NewAuthenticator(store *state.Store, raft Applier, clk clock.Clock) *Authenticator {
	return &Authenticator{
		store:    store,
		raft:     raft,
		clock:    clk,
		lastUsed: map[string]time.Time{},
	}
}

// UnaryInterceptor authenticates and authorizes unary calls.
func (a *Authenticator) UnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	ctx, err := a.authorize(ctx, info.FullMethod)
	if err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

// StreamInterceptor authenticates and authorizes streaming calls.
func (a *Authenticator) StreamInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx, err := a.authorize(ss.Context(), info.FullMethod)
	if err != nil {
		return err
	}
	return handler(srv, &wrappedStream{ServerStream: ss, ctx: ctx})
}

type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }

// authorize resolves identity, enforces the method requirement, and returns a
// context carrying the identity.
func (a *Authenticator) authorize(ctx context.Context, fullMethod string) (context.Context, error) {
	// Health checks bypass the policy entirely.
	if strings.HasPrefix(fullMethod, "/grpc.health.") {
		return ctx, nil
	}

	req := methodAuth[fullMethod]
	if req == reqDenied {
		// Fail closed: unlisted methods are never reachable.
		return ctx, status.Errorf(codes.PermissionDenied, "method %s is not permitted", fullMethod)
	}

	id, err := a.resolveIdentity(ctx)
	if err != nil {
		return ctx, err
	}

	switch req {
	case reqPublic:
		// No identity required; still attach it if present.
	case reqUser:
		if id.UserID == "" {
			return ctx, status.Error(codes.Unauthenticated, "a user token is required")
		}
	case reqNode:
		if id.NodeID == "" {
			return ctx, status.Error(codes.PermissionDenied, "this method is restricted to cluster nodes")
		}
	case reqAdmin:
		if id.UserID == "" {
			return ctx, status.Error(codes.Unauthenticated, "a user token is required")
		}
		if id.OrgRole != zatterav1.Role_ROLE_OWNER && id.OrgRole != zatterav1.Role_ROLE_ADMIN {
			return ctx, status.Error(codes.PermissionDenied, "admin privileges are required")
		}
	}
	return withIdentity(ctx, id), nil
}

// resolveIdentity checks mTLS node identity first, then a bearer token.
func (a *Authenticator) resolveIdentity(ctx context.Context) (Identity, error) {
	if nodeID := nodeIDFromPeer(ctx); nodeID != "" {
		return Identity{NodeID: nodeID}, nil
	}
	tok := bearerToken(ctx)
	if tok == "" {
		return Identity{}, nil // anonymous; requirement check decides.
	}
	stored, ok := a.store.TokenByHash(HashToken(tok))
	if !ok {
		return Identity{}, status.Error(codes.Unauthenticated, "invalid token")
	}
	if exp := stored.GetExpiresAt(); exp != nil && a.clock.Now().After(exp.AsTime()) {
		return Identity{}, status.Error(codes.Unauthenticated, "token expired")
	}
	user, ok := a.store.User(stored.GetUserId())
	if !ok {
		return Identity{}, status.Error(codes.Unauthenticated, "token owner not found")
	}
	a.markUsed(stored.GetMeta().GetId())
	return Identity{UserID: user.GetMeta().GetId(), OrgRole: user.GetOrgRole()}, nil
}

// nodeIDFromPeer extracts the node id from a verified client cert's URI SAN.
func nodeIDFromPeer(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return ""
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return ""
	}
	chains := tlsInfo.State.VerifiedChains
	if len(chains) == 0 || len(chains[0]) == 0 {
		return ""
	}
	for _, uri := range chains[0][0].URIs {
		if s := uri.String(); strings.HasPrefix(s, nodeURIScheme) {
			return strings.TrimPrefix(s, nodeURIScheme)
		}
	}
	return ""
}

// bearerToken pulls a zpat token out of the authorization metadata.
func bearerToken(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	vals := md.Get("authorization")
	if len(vals) == 0 {
		return ""
	}
	v := strings.TrimSpace(vals[0])
	if lower := strings.ToLower(v); strings.HasPrefix(lower, "bearer ") {
		v = strings.TrimSpace(v[len("bearer "):])
	}
	if !LooksLikeToken(v) {
		return ""
	}
	return v
}

func (a *Authenticator) markUsed(tokenID string) {
	if tokenID == "" {
		return
	}
	a.mu.Lock()
	a.lastUsed[tokenID] = a.clock.Now()
	a.mu.Unlock()
}

// RunTokenFlusher periodically flushes accumulated last_used_at through one
// TouchTokens apply (leader only). Blocks until ctx is done.
func (a *Authenticator) RunTokenFlusher(ctx context.Context) {
	tick := a.clock.NewTicker(60 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C():
			a.flush(ctx)
		}
	}
}

func (a *Authenticator) flush(ctx context.Context) {
	if !a.raft.IsLeader() {
		// Drop on followers for now (leader owns the flush).
		a.mu.Lock()
		a.lastUsed = map[string]time.Time{}
		a.mu.Unlock()
		return
	}
	a.mu.Lock()
	if len(a.lastUsed) == 0 {
		a.mu.Unlock()
		return
	}
	batch := a.lastUsed
	a.lastUsed = map[string]time.Time{}
	a.mu.Unlock()

	m := make(map[string]*timestamppb.Timestamp, len(batch))
	for id, t := range batch {
		m[id] = timestamppb.New(t)
	}
	cmd := &clusterv1.Command{
		RequestId: ids.New(),
		Actor:     "system:token-flush",
		Time:      timestamppb.Now(),
		Mutation:  &clusterv1.Command_TouchTokens{TouchTokens: &clusterv1.TouchTokens{LastUsed: m}},
	}
	if err := a.raft.Apply(ctx, cmd); err != nil {
		// Best-effort; the samples are already dropped.
		return
	}
}

// ValidateMethodTable asserts that every method registered on the gRPC server
// (excluding health) has a policy entry. A missing entry is a fail-closed hole
// waiting to open — surface it at startup, not at request time.
func ValidateMethodTable(info map[string]grpc.ServiceInfo) error {
	for svc, si := range info {
		if strings.HasPrefix(svc, "grpc.health.") || strings.HasPrefix(svc, "grpc.reflection.") {
			continue
		}
		for _, m := range si.Methods {
			full := "/" + svc + "/" + m.Name
			if _, ok := methodAuth[full]; !ok {
				return fmt.Errorf("api: method %s has no auth policy entry (fail-closed)", full)
			}
		}
	}
	return nil
}
