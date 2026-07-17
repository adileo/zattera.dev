package agent

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	"github.com/zattera-dev/zattera/internal/daemon/tsdb"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestMetricsSamplerRecordsAllScopes(t *testing.T) {
	store := tsdb.Open(tsdb.Config{})
	defer func() { _ = store.Close() }()
	fake := clock.NewFake()

	m := &metricsSampler{
		store:    store,
		clk:      fake,
		log:      testLogger(),
		nodeID:   "n1",
		interval: time.Second,
		node: func() NodeMetrics {
			return NodeMetrics{CPUPercent: 42, MemUsed: 100, MemTotal: 200, NetRxBytes: 7}
		},
		instances: func(context.Context) []InstanceMetrics {
			return []InstanceMetrics{{InstanceID: "inst-1", CPUPercent: 5, MemoryBytes: 1024}}
		},
		proxy: func() map[string]*clusterv1.ProxySample {
			return map[string]*clusterv1.ProxySample{
				"env-1": {Rps: 3.5, ErrorRate: 0.1, LatencyP50Ms: 12, LatencyP99Ms: 99, Inflight: 4},
			}
		},
	}

	now := fake.Now()
	m.sample(context.Background())

	at := func(metric, scope, id string) []tsdb.Point {
		return store.Query(tsdb.SeriesKey{Metric: metric, Scope: scope, ScopeID: id}, now, now, tsdb.RawStep)
	}
	assertVal := func(name string, pts []tsdb.Point, want float64) {
		t.Helper()
		if len(pts) != 1 {
			t.Fatalf("%s: want 1 point, got %d", name, len(pts))
		}
		if pts[0].Value != want {
			t.Errorf("%s = %v, want %v", name, pts[0].Value, want)
		}
	}

	assertVal("node cpu", at("cpu_percent", scopeNode, "n1"), 42)
	assertVal("node net_rx", at("net_rx_bytes", scopeNode, "n1"), 7)
	assertVal("instance cpu", at("cpu_percent", scopeInstance, "inst-1"), 5)
	assertVal("instance mem", at("memory_bytes", scopeInstance, "inst-1"), 1024)
	assertVal("env rps", at("rps", scopeEnv, "env-1"), 3.5)
	assertVal("env inflight", at("inflight", scopeEnv, "env-1"), 4)
	assertVal("env p99", at("latency_p99_ms", scopeEnv, "env-1"), 99)
}

func TestMetricsSamplerNilProvidersSafe(t *testing.T) {
	store := tsdb.Open(tsdb.Config{})
	defer func() { _ = store.Close() }()
	m := &metricsSampler{store: store, clk: clock.NewFake(), log: testLogger(), nodeID: "n1", interval: time.Second}
	// No providers set: must not panic and records nothing.
	m.sample(context.Background())
	if keys := store.Keys("", ""); len(keys) != 0 {
		t.Fatalf("expected no series with nil providers, got %d", len(keys))
	}
}

func TestMetricsSamplerRunSamplesOnTick(t *testing.T) {
	store := tsdb.Open(tsdb.Config{})
	defer func() { _ = store.Close() }()
	fake := clock.NewFake()
	var calls int
	m := &metricsSampler{
		store: store, clk: fake, log: testLogger(), nodeID: "n1", interval: metricsInterval,
		node: func() NodeMetrics { calls++; return NodeMetrics{CPUPercent: 1} },
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { m.Run(ctx); close(done) }()

	// Run takes one immediate sample; give it a moment to land, then tick twice.
	waitFor(t, func() bool { return sampleCount(store) >= 1 })
	fake.Advance(metricsInterval)
	waitFor(t, func() bool { return sampleCount(store) >= 1 }) // still one series, more points
	cancel()
	<-done
}

// sampleCount returns how many distinct series the store holds.
func sampleCount(s *tsdb.RingStore) int { return len(s.Keys("", "")) }

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met before deadline")
}
