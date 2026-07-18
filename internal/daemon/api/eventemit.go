package api

import (
	"context"
	"log/slog"

	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
)

// emitEvent appends one event to the replicated event log, where alert rules
// with a matching event_kind pick it up (T-74/T-109).
//
// Best-effort by contract: the caller is reporting something that already
// happened, so a failure to record it must never fail or block that operation.
// Followers cannot append — raftstore.Apply rejects them without forwarding —
// so callers must either run leader-only or accept that the event is dropped
// with a log line. Actor identifies the subsystem, e.g. "system:liveness".
func emitEvent(ctx context.Context, raft Applier, log *slog.Logger, actor string, ev *zatterav1.Event) {
	if raft == nil || ev == nil {
		return
	}
	if ev.GetMeta() == nil {
		ev.Meta = &zatterav1.Meta{Id: ids.New(), CreatedAt: timestamppb.Now()}
	}
	cmd := &clusterv1.Command{
		RequestId: ids.New(),
		Actor:     actor,
		Time:      timestamppb.Now(),
		Mutation: &clusterv1.Command_AppendEvents{
			AppendEvents: &clusterv1.AppendEvents{Events: []*zatterav1.Event{ev}},
		},
	}
	if err := raft.Apply(ctx, cmd); err != nil {
		if log == nil {
			log = slog.Default()
		}
		// Debug, not Warn: losing leadership mid-loop is routine, and the
		// event is a notification, not state anyone reads back.
		log.Debug("emit event failed", "kind", ev.GetKind(), "err", err)
	}
}

// nodeLabel names a node for humans reading an alert: its name when set, its
// id otherwise (a node can reach DOWN before it ever reported a name).
func nodeLabel(n *zatterav1.Node) string {
	if name := n.GetName(); name != "" {
		return name
	}
	return n.GetMeta().GetId()
}
