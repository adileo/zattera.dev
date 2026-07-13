package api

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

// forwardedMDKey caps forwarding at one hop: a forwarded request carries it, and
// a follower refuses to forward again.
const forwardedMDKey = "x-zattera-forwarded"

// LeaderForwarder transparently proxies mutating unary calls that land on a
// follower to the current leader's API, preserving the caller's deadline and
// authorization metadata (the leader re-runs auth/rbac/audit on the forwarded
// call). It is the OUTERMOST interceptor.
//
// Streams are not forwarded — callers redial the leader (documented in
// pkg/apiclient).
type LeaderForwarder struct {
	isLeader func() bool
	// resolve returns the current leader's API address, or "" when this node is
	// (or believes it is) the leader / the leader is unknown.
	resolve  func() (string, error)
	dialOpts []grpc.DialOption
	log      *slog.Logger

	mu       sync.Mutex
	conn     *grpc.ClientConn
	connAddr string
}

// NewLeaderForwarder builds the forwarder. dialOpts must establish TLS trusting
// the cluster CA (no client cert: the forwarded bearer token authenticates the
// caller on the leader).
func NewLeaderForwarder(isLeader func() bool, resolve func() (string, error), dialOpts []grpc.DialOption, log *slog.Logger) *LeaderForwarder {
	if log == nil {
		log = slog.Default()
	}
	return &LeaderForwarder{isLeader: isLeader, resolve: resolve, dialOpts: dialOpts, log: log}
}

// UnaryInterceptor forwards mutating calls to the leader when this node is a
// follower.
func (f *LeaderForwarder) UnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	// Leader (or single-node): serve locally.
	if f.isLeader() {
		return handler(ctx, req)
	}
	// Reads are served from local (eventually-consistent) state; only mutating
	// calls must reach the leader.
	if !isMutatingMethod(info.FullMethod) {
		return handler(ctx, req)
	}
	// One hop only: never forward an already-forwarded request.
	if forwardedAlready(ctx) {
		return nil, status.Error(codes.Internal, "leader-forward loop: request already forwarded once")
	}

	addr, err := f.resolve()
	if err != nil || addr == "" {
		return nil, status.Error(codes.Unavailable, "no leader currently available; retry")
	}
	conn, err := f.connFor(addr)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "dial leader %s: %v", addr, err)
	}
	reqMsg, ok := req.(proto.Message)
	if !ok {
		return nil, status.Error(codes.Internal, "leader-forward: request is not a proto message")
	}
	reply, err := newReplyMessage(info.FullMethod)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "leader-forward: %v", err)
	}
	// Preserve the caller's metadata (authorization included) + the deadline,
	// and mark the request as forwarded.
	octx := metadata.NewOutgoingContext(ctx, forwardMetadata(ctx))
	if err := conn.Invoke(octx, info.FullMethod, reqMsg, reply); err != nil {
		return nil, err // already a gRPC status from the leader.
	}
	return reply, nil
}

// connFor returns a cached client conn to addr, redialing when the leader
// address changes.
func (f *LeaderForwarder) connFor(addr string) (*grpc.ClientConn, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.conn != nil && f.connAddr == addr {
		return f.conn, nil
	}
	if f.conn != nil {
		_ = f.conn.Close()
		f.conn = nil
	}
	conn, err := grpc.NewClient(addr, f.dialOpts...)
	if err != nil {
		return nil, err
	}
	f.conn, f.connAddr = conn, addr
	return conn, nil
}

// Invalidate drops the cached leader connection. Wire it to raft's LeaderCh so a
// leadership change forces a fresh dial.
func (f *LeaderForwarder) Invalidate() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.conn != nil {
		_ = f.conn.Close()
		f.conn = nil
		f.connAddr = ""
	}
}

// forwardMetadata copies the incoming metadata and marks the request forwarded.
func forwardMetadata(ctx context.Context) metadata.MD {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		md = metadata.MD{}
	}
	out := md.Copy()
	out.Set(forwardedMDKey, "1")
	return out
}

func forwardedAlready(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	vals := md.Get(forwardedMDKey)
	return len(vals) > 0 && vals[0] == "1"
}

// newReplyMessage builds an empty response message for a full method name using
// the global proto registry, so forwarding stays generic across services.
func newReplyMessage(fullMethod string) (proto.Message, error) {
	// fullMethod is "/pkg.Service/Method".
	trimmed := strings.TrimPrefix(fullMethod, "/")
	slash := strings.LastIndex(trimmed, "/")
	if slash < 0 {
		return nil, fmt.Errorf("bad method %q", fullMethod)
	}
	svcName := protoreflect.FullName(trimmed[:slash])
	methodName := protoreflect.Name(trimmed[slash+1:])

	desc, err := protoregistry.GlobalFiles.FindDescriptorByName(svcName)
	if err != nil {
		return nil, fmt.Errorf("service %s: %w", svcName, err)
	}
	svc, ok := desc.(protoreflect.ServiceDescriptor)
	if !ok {
		return nil, fmt.Errorf("%s is not a service", svcName)
	}
	m := svc.Methods().ByName(methodName)
	if m == nil {
		return nil, fmt.Errorf("method %s not found", methodName)
	}
	return dynamicpb.NewMessage(m.Output()), nil
}
