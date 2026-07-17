package tsdb

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// base is aligned to a 5m boundary (Unix divisible by 300) so a run of raw
// slots lands inside a single downsample slot.
var base = time.Unix(1_800_000_000, 0).UTC()

func nodeKey(metric string) SeriesKey {
	return SeriesKey{Metric: metric, Scope: "node", ScopeID: "n1"}
}

func TestRecordQueryRoundTrip(t *testing.T) {
	s := Open(Config{})
	defer s.Close()
	k := nodeKey("cpu_percent")

	for i := 0; i < 4; i++ {
		s.Record(k, Point{Time: base.Add(time.Duration(i) * RawStep), Value: float64(i) * 10})
	}
	got := s.Query(k, base, base.Add(3*RawStep), RawStep)
	if len(got) != 4 {
		t.Fatalf("want 4 points, got %d: %v", len(got), got)
	}
	for i, p := range got {
		wantT := base.Add(time.Duration(i) * RawStep)
		if !p.Time.Equal(wantT) {
			t.Errorf("point %d time = %v, want %v", i, p.Time, wantT)
		}
		if p.Value != float64(i)*10 {
			t.Errorf("point %d value = %v, want %v", i, p.Value, float64(i)*10)
		}
	}
}

func TestQueryRangeExcludesOutside(t *testing.T) {
	s := Open(Config{})
	defer s.Close()
	k := nodeKey("cpu_percent")
	for i := 0; i < 10; i++ {
		s.Record(k, Point{Time: base.Add(time.Duration(i) * RawStep), Value: 1})
	}
	// Query a sub-window [slot 3, slot 6].
	got := s.Query(k, base.Add(3*RawStep), base.Add(6*RawStep), RawStep)
	if len(got) != 4 {
		t.Fatalf("want 4 points in sub-window, got %d", len(got))
	}
	if !got[0].Time.Equal(base.Add(3 * RawStep)) {
		t.Errorf("first point = %v, want slot 3", got[0].Time)
	}
}

func TestOutOfOrderDrop(t *testing.T) {
	s := Open(Config{})
	defer s.Close()
	k := nodeKey("cpu_percent")

	s.Record(k, Point{Time: base.Add(5 * RawStep), Value: 50})
	// Older-than-current-slot sample: dropped.
	s.Record(k, Point{Time: base.Add(2 * RawStep), Value: 20})
	if got := s.Query(k, base, base.Add(5*RawStep), RawStep); len(got) != 1 || got[0].Value != 50 {
		t.Fatalf("out-of-order sample not dropped: %v", got)
	}

	// Same-slot re-sample overwrites last-write-wins.
	s.Record(k, Point{Time: base.Add(5 * RawStep), Value: 99})
	if got := s.Query(k, base.Add(5*RawStep), base.Add(5*RawStep), RawStep); len(got) != 1 || got[0].Value != 99 {
		t.Fatalf("same-slot overwrite failed: %v", got)
	}
}

func TestWrapAround(t *testing.T) {
	s := Open(Config{})
	defer s.Close()
	k := nodeKey("cpu_percent")

	s.Record(k, Point{Time: base, Value: 1})
	// One full raw retention later maps to the same ring position.
	later := base.Add(time.Duration(rawSlots) * RawStep)
	s.Record(k, Point{Time: later, Value: 2})

	// The original slot's position now holds the newer abs → the old sample is
	// gone, not misread as belonging to the old time.
	if got := s.Query(k, base, base, RawStep); len(got) != 0 {
		t.Fatalf("evicted slot still returned: %v", got)
	}
	if got := s.Query(k, later, later, RawStep); len(got) != 1 || got[0].Value != 2 {
		t.Fatalf("wrapped-in slot wrong: %v", got)
	}
}

func TestDownsampleAverage(t *testing.T) {
	s := Open(Config{})
	defer s.Close()
	k := nodeKey("cpu_percent")

	// 20 raw slots (300s / 15s) all inside one 5m downsample slot; values 1..20.
	n := int(DownStep / RawStep)
	var sum float64
	for i := 0; i < n; i++ {
		v := float64(i + 1)
		sum += v
		s.Record(k, Point{Time: base.Add(time.Duration(i) * RawStep), Value: v})
	}
	want := sum / float64(n) // 10.5

	got := s.Query(k, base, base.Add(DownStep), DownStep)
	if len(got) != 1 {
		t.Fatalf("want 1 downsampled point, got %d: %v", len(got), got)
	}
	if diff := got[0].Value - want; diff > 0.01 || diff < -0.01 {
		t.Fatalf("downsample avg = %v, want ~%v", got[0].Value, want)
	}
	if !got[0].Time.Equal(base) {
		t.Errorf("downsample time = %v, want %v (5m slot start)", got[0].Time, base)
	}
}

func TestResolutionSelection(t *testing.T) {
	s := Open(Config{})
	defer s.Close()
	k := nodeKey("cpu_percent")
	s.Record(k, Point{Time: base, Value: 7})

	// A coarse step selects the down ring; a fine step the raw ring. Both hold
	// the single sample here.
	if got := s.Query(k, base, base.Add(DownStep), DownStep); len(got) != 1 {
		t.Errorf("down-resolution query returned %d points", len(got))
	}
	if got := s.Query(k, base, base, RawStep); len(got) != 1 {
		t.Errorf("raw-resolution query returned %d points", len(got))
	}
}

func TestKeysFilter(t *testing.T) {
	s := Open(Config{})
	defer s.Close()
	s.Record(SeriesKey{Metric: "cpu", Scope: "node", ScopeID: "n1"}, Point{Time: base, Value: 1})
	s.Record(SeriesKey{Metric: "rps", Scope: "env", ScopeID: "e1"}, Point{Time: base, Value: 1})
	s.Record(SeriesKey{Metric: "rps", Scope: "env", ScopeID: "e2"}, Point{Time: base, Value: 1})

	if all := s.Keys("", ""); len(all) != 3 {
		t.Errorf("Keys(all) = %d, want 3", len(all))
	}
	if env := s.Keys("env", ""); len(env) != 2 {
		t.Errorf("Keys(env) = %d, want 2", len(env))
	}
	e1 := s.Keys("env", "e1")
	if len(e1) != 1 || e1[0].ScopeID != "e1" {
		t.Errorf("Keys(env,e1) = %v, want single e1", e1)
	}
}

func TestPersistLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tsdb.bin")
	k := nodeKey("cpu_percent")

	// Timestamps near the (fake) wall clock so GC on flush keeps them.
	fake := clock.NewFake()
	lb := fake.Now().Add(-time.Minute)

	s := Open(Config{Path: path, Clock: fake})
	for i := 0; i < 5; i++ {
		s.Record(k, Point{Time: lb.Add(time.Duration(i) * RawStep), Value: float64(i)})
	}
	if err := s.Close(); err != nil { // Close flushes.
		t.Fatalf("close: %v", err)
	}

	s2 := Open(Config{Path: path, Clock: clock.NewFake()})
	defer s2.Close()
	got := s2.Query(k, lb, lb.Add(4*RawStep), RawStep)
	if len(got) != 5 {
		t.Fatalf("after reload want 5 points, got %d", len(got))
	}
	for i, p := range got {
		if p.Value != float64(i) {
			t.Errorf("reloaded point %d = %v, want %v", i, p.Value, float64(i))
		}
	}
}

func TestLoadCorruptStartsEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tsdb.bin")
	if err := os.WriteFile(path, []byte("not a valid tsdb file at all"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Must not panic; starts empty and is usable.
	s := Open(Config{Path: path, Clock: clock.NewFake()})
	defer s.Close()
	if keys := s.Keys("", ""); len(keys) != 0 {
		t.Fatalf("corrupt load should start empty, got %d keys", len(keys))
	}
	k := nodeKey("cpu_percent")
	s.Record(k, Point{Time: base, Value: 1})
	if got := s.Query(k, base, base, RawStep); len(got) != 1 {
		t.Fatalf("store unusable after corrupt load: %v", got)
	}
}

func TestGCDropsStaleSeries(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tsdb.bin")
	fake := clock.NewFake()

	// Record a sample "now", then two series whose newest sample is old.
	s := Open(Config{Path: path, Clock: fake})
	defer s.Close()

	old := fake.Now().Add(-49 * time.Hour)
	fresh := fake.Now().Add(-1 * time.Hour)
	s.Record(SeriesKey{Metric: "cpu", Scope: "node", ScopeID: "old"}, Point{Time: old, Value: 1})
	s.Record(SeriesKey{Metric: "cpu", Scope: "node", ScopeID: "fresh"}, Point{Time: fresh, Value: 1})

	if err := s.Flush(); err != nil { // Flush runs gc.
		t.Fatalf("flush: %v", err)
	}
	keys := s.Keys("", "")
	if len(keys) != 1 || keys[0].ScopeID != "fresh" {
		t.Fatalf("gc kept wrong series: %v", keys)
	}
}

func TestQueryUnknownSeries(t *testing.T) {
	s := Open(Config{})
	defer s.Close()
	if got := s.Query(nodeKey("nope"), base, base.Add(time.Hour), RawStep); got != nil {
		t.Fatalf("unknown series query = %v, want nil", got)
	}
}

func TestQueryInvertedRange(t *testing.T) {
	s := Open(Config{})
	defer s.Close()
	k := nodeKey("cpu")
	s.Record(k, Point{Time: base, Value: 1})
	if got := s.Query(k, base.Add(time.Hour), base, RawStep); got != nil {
		t.Fatalf("inverted range = %v, want nil", got)
	}
}
