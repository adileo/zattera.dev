package api

import (
	"context"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

const (
	auditQueueSize   = 4096
	auditMaxBatch    = 100
	auditFlushPeriod = 2 * time.Second
)

// summaryAllow lists methods whose request may contribute a safe one-line
// summary (a name only). Everything else records no summary, so secrets and
// passwords never reach the audit log.
var summaryAllow = map[string]bool{
	"/zattera.v1.ProjectService/CreateProject": true,
	"/zattera.v1.AppService/CreateApp":         true,
}

// Auditor records mutating API calls. The interceptor enqueues entries
// non-blocking; a background batcher flushes them through AppendAudit on the
// leader.
type Auditor struct {
	zatterav1.UnimplementedAuditServiceServer
	store    *state.Store
	raft     Applier
	log      *slog.Logger
	interval time.Duration

	queue   chan *zatterav1.AuditEntry
	dropped atomic.Int64
}

// NewAuditor builds the audit middleware. interval<=0 uses the 2s default.
func NewAuditor(store *state.Store, raft Applier, log *slog.Logger, interval time.Duration) *Auditor {
	if log == nil {
		log = slog.Default()
	}
	if interval <= 0 {
		interval = auditFlushPeriod
	}
	return &Auditor{
		store:    store,
		raft:     raft,
		log:      log,
		interval: interval,
		queue:    make(chan *zatterav1.AuditEntry, auditQueueSize),
	}
}

// UnaryInterceptor records the outcome of authorized mutating unary calls. It
// runs after auth+rbac, so only authorized calls are recorded (a denied caller
// cannot flood the audit log).
func (a *Auditor) UnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	resp, err := handler(ctx, req)
	if isMutatingMethod(info.FullMethod) {
		a.record(ctx, info.FullMethod, req, err)
	}
	return resp, err
}

// record builds an AuditEntry and enqueues it (dropping if the buffer is full).
func (a *Auditor) record(ctx context.Context, method string, req any, callErr error) {
	id, _ := IdentityFrom(ctx)
	entry := &zatterav1.AuditEntry{
		Meta:        &zatterav1.Meta{Id: ids.New(), CreatedAt: timestamppb.Now()},
		ActorUserId: id.UserID,
		Method:      method,
		Outcome:     outcomeString(callErr),
		RemoteAddr:  remoteAddr(ctx),
	}
	if msg, ok := req.(proto.Message); ok {
		entry.ProjectId = stringField(msg, "project_id")
		if summaryAllow[method] {
			entry.RequestSummary = safeSummary(msg)
		}
	}
	select {
	case a.queue <- entry:
	default:
		if n := a.dropped.Add(1); n%100 == 1 {
			a.log.Warn("audit buffer full; dropping entries", "dropped_total", n)
		}
	}
}

// Run flushes queued entries every interval (or when a batch fills). Blocks
// until ctx is done, then drains once more.
func (a *Auditor) Run(ctx context.Context) {
	tick := time.NewTicker(a.interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			a.drainOnce(context.Background())
			return
		case <-tick.C:
			a.drainOnce(ctx)
		}
	}
}

// drainOnce pulls all currently-queued entries and flushes them in batches of
// at most auditMaxBatch.
func (a *Auditor) drainOnce(ctx context.Context) {
	batch := make([]*zatterav1.AuditEntry, 0, auditMaxBatch)
	for {
		select {
		case e := <-a.queue:
			batch = append(batch, e)
			if len(batch) >= auditMaxBatch {
				a.flush(ctx, batch)
				batch = batch[:0]
			}
		default:
			a.flush(ctx, batch)
			return
		}
	}
}

func (a *Auditor) flush(ctx context.Context, batch []*zatterav1.AuditEntry) {
	if len(batch) == 0 {
		return
	}
	if !a.raft.IsLeader() {
		// Only the leader writes audit; followers drop (their streams will be
		// re-driven to the leader in M2).
		return
	}
	cmd := &clusterv1.Command{
		RequestId: ids.New(),
		Actor:     "system:audit",
		Time:      timestamppb.Now(),
		Mutation:  &clusterv1.Command_AppendAudit{AppendAudit: &clusterv1.AppendAudit{Entries: batch}},
	}
	if err := a.raft.Apply(ctx, cmd); err != nil {
		a.log.Warn("audit flush failed", "err", err, "entries", len(batch))
	}
}

// QueryAudit implements zatterav1.AuditServiceServer over state.QueryAudit.
func (a *Auditor) QueryAudit(_ context.Context, req *zatterav1.QueryAuditRequest) (*zatterav1.QueryAuditResponse, error) {
	limit := int(req.GetLimit())
	filter := func(e *zatterav1.AuditEntry) bool {
		if p := req.GetProjectId(); p != "" && e.GetProjectId() != p {
			return false
		}
		if u := req.GetActorUserId(); u != "" && e.GetActorUserId() != u {
			return false
		}
		if mp := req.GetMethodPrefix(); mp != "" && !strings.HasPrefix(e.GetMethod(), mp) {
			return false
		}
		if s := req.GetSinceUnixMs(); s > 0 && e.GetMeta().GetCreatedAt().AsTime().UnixMilli() < s {
			return false
		}
		if u := req.GetUntilUnixMs(); u > 0 && e.GetMeta().GetCreatedAt().AsTime().UnixMilli() > u {
			return false
		}
		return true
	}
	return &zatterav1.QueryAuditResponse{Entries: a.store.QueryAudit(filter, limit)}, nil
}

// --- helpers ---

// isMutatingMethod reports whether a method should be audited: everything whose
// final segment is not a Get/List/Watch/Query read.
func isMutatingMethod(fullMethod string) bool {
	name := fullMethod
	if i := strings.LastIndex(fullMethod, "/"); i >= 0 {
		name = fullMethod[i+1:]
	}
	for _, ro := range []string{"Get", "List", "Watch", "Query"} {
		if strings.HasPrefix(name, ro) {
			return false
		}
	}
	return true
}

func outcomeString(err error) string {
	if err == nil {
		return "ok"
	}
	return status.Code(err).String()
}

func remoteAddr(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		return p.Addr.String()
	}
	return ""
}

// stringField reads a top-level string field by name via reflection ("" if
// absent or not a string).
func stringField(msg proto.Message, name string) string {
	fd := msg.ProtoReflect().Descriptor().Fields().ByName(protoreflect.Name(name))
	if fd == nil || fd.Kind() != protoreflect.StringKind {
		return ""
	}
	return msg.ProtoReflect().Get(fd).String()
}

// safeSummary returns "name=<value>" for allow-listed messages carrying a name.
func safeSummary(msg proto.Message) string {
	if n := stringField(msg, "name"); n != "" {
		return "name=" + n
	}
	return ""
}
