package daemon

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/leaderrunner"
	"github.com/zattera-dev/zattera/internal/daemon/livestate"
	"github.com/zattera-dev/zattera/internal/daemon/notify"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
)

// alertEngineTick is the alert-engine evaluation cadence on the leader.
const alertEngineTick = 30 * time.Second

// runAlertEngine drives the notify engine on the leader (T-74): a fresh term
// resets its state (so history is not replayed) and it evaluates every 30s.
func runAlertEngine(ctx context.Context, rs *raftstore.Store, engine *notify.Engine, clk clock.Clock) {
	leaderrunner.Run(ctx, rs, clk, func(ctx context.Context) {
		engine.Reset()
		tick := clk.NewTicker(alertEngineTick)
		defer tick.Stop()
		for {
			engine.Tick(ctx)
			select {
			case <-ctx.Done():
				return
			case <-rs.LeaderCh():
				if !rs.IsLeader() {
					return
				}
			case <-tick.C():
			}
		}
	})
}

// raftEventEmitter records an event through raft (best-effort) so subsystem
// failures surface in the event log and can match an alert rule's event_kind.
// Only the leader can append (raftstore.Apply does not forward), so on a
// follower this is a no-op with a debug line — see T-110.
func raftEventEmitter(rs *raftstore.Store, clk clock.Clock, log *slog.Logger, actor string) func(kind, severity, message string) {
	return func(kind, severity, message string) {
		ev := &zatterav1.Event{
			Meta: &zatterav1.Meta{Id: ids.New(), CreatedAt: timestamppb.New(clk.Now())},
			Kind: kind, Severity: severity, Message: message,
		}
		cmd := &clusterv1.Command{
			RequestId: ids.New(), Actor: actor, Time: timestamppb.New(clk.Now()),
			Mutation: &clusterv1.Command_AppendEvents{AppendEvents: &clusterv1.AppendEvents{Events: []*zatterav1.Event{ev}}},
		}
		if err := rs.Apply(context.Background(), cmd); err != nil {
			log.Debug("emit event failed", "actor", actor, "kind", kind, "err", err)
		}
	}
}

// liveMetrics resolves alert metric values from the leader's live heartbeat
// view (notify.MetricSource). Node scopes read a single node; env scopes read
// proxy counters; the cluster scope aggregates (max for disk, average for
// cpu/memory).
type liveMetrics struct{ live *livestate.Registry }

func (m liveMetrics) Value(metric, scope string) (float64, bool) {
	kind, id := parseScope(scope)
	switch kind {
	case "node":
		return m.nodeMetric(metric, id)
	case "env":
		return m.envMetric(metric, id)
	default: // cluster
		return m.clusterMetric(metric)
	}
}

func (m liveMetrics) nodeMetric(metric, nodeID string) (float64, bool) {
	for _, ns := range m.live.Snapshot() {
		if ns.NodeID != nodeID || ns.Heartbeat == nil {
			continue
		}
		return nodeSampleMetric(ns.Heartbeat, metric)
	}
	return 0, false
}

func (m liveMetrics) envMetric(metric, envID string) (float64, bool) {
	var val float64
	var ok bool
	for _, ns := range m.live.Snapshot() {
		if ns.Heartbeat == nil {
			continue
		}
		ps, present := ns.Heartbeat.GetProxy()[envID]
		if !present {
			continue
		}
		ok = true
		switch metric {
		case "error_rate":
			if ps.GetErrorRate() > val {
				val = ps.GetErrorRate() // worst across nodes
			}
		case "rps":
			val += ps.GetRps()
		case "inflight":
			val += float64(ps.GetInflight())
		case "latency_p99_ms":
			if ps.GetLatencyP99Ms() > val {
				val = ps.GetLatencyP99Ms()
			}
		default:
			return 0, false
		}
	}
	return val, ok
}

func (m liveMetrics) clusterMetric(metric string) (float64, bool) {
	var sum, max float64
	var n int
	for _, ns := range m.live.Snapshot() {
		if ns.Heartbeat == nil {
			continue
		}
		v, ok := nodeSampleMetric(ns.Heartbeat, metric)
		if !ok {
			continue
		}
		n++
		sum += v
		if v > max {
			max = v
		}
	}
	if n == 0 {
		return 0, false
	}
	if metric == "disk_percent" {
		return max, true // any node over threshold should fire
	}
	return sum / float64(n), true
}

// nodeSampleMetric extracts a node-level metric from a heartbeat.
func nodeSampleMetric(hb *clusterv1.Heartbeat, metric string) (float64, bool) {
	switch metric {
	case "cpu_percent":
		return hb.GetCpuPercent(), true
	case "memory_percent":
		return pct(hb.GetMemoryUsedBytes(), hb.GetMemoryTotalBytes())
	case "disk_percent":
		return pct(hb.GetDiskUsedBytes(), hb.GetDiskTotalBytes())
	default:
		return 0, false
	}
}

func pct(used, total uint64) (float64, bool) {
	if total == 0 {
		return 0, false
	}
	return float64(used) / float64(total) * 100, true
}

// parseScope splits "node:<id>" / "env:<id>" / "cluster".
func parseScope(scope string) (kind, id string) {
	for i := 0; i < len(scope); i++ {
		if scope[i] == ':' {
			return scope[:i], scope[i+1:]
		}
	}
	return scope, ""
}
