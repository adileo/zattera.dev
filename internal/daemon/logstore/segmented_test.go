package logstore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

func newStore(t *testing.T, opts Options) *Segmented {
	t.Helper()
	if opts.Root == "" {
		opts.Root = t.TempDir()
	}
	if opts.Clock == nil {
		opts.Clock = clock.NewFake()
	}
	s, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func line(base time.Time, i int) Entry {
	return Entry{Time: base.Add(time.Duration(i) * time.Millisecond), Line: fmt.Sprintf("line-%03d", i)}
}

func TestAppendQueryAcrossRotation(t *testing.T) {
	root := t.TempDir()
	// Tiny segments force many rotations + compression.
	s := newStore(t, Options{Root: root, MaxSegmentBytes: 128})
	base := time.Unix(1_700_000_000, 0)

	for i := 0; i < 60; i++ {
		if err := s.Append("svc", []Entry{line(base, i)}); err != nil {
			t.Fatal(err)
		}
	}

	// At least one compressed segment was produced.
	segs := listSegments(filepath.Join(root, "svc"))
	if len(segs) == 0 {
		t.Fatal("expected rotation to compress segments")
	}

	got, err := s.Query(context.Background(), Query{Streams: []StreamID{"svc"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 60 {
		t.Fatalf("got %d entries, want 60", len(got))
	}
	for i, e := range got {
		if e.Line != fmt.Sprintf("line-%03d", i) {
			t.Fatalf("entry %d out of order: %q", i, e.Line)
		}
	}
}

func TestQueryTimeFilter(t *testing.T) {
	s := newStore(t, Options{})
	base := time.Unix(1_700_000_000, 0)
	for i := 0; i < 10; i++ {
		_ = s.Append("svc", []Entry{line(base, i)})
	}
	// [line-003, line-006]
	got, err := s.Query(context.Background(), Query{
		Streams: []StreamID{"svc"},
		Since:   base.Add(3 * time.Millisecond),
		Until:   base.Add(6 * time.Millisecond),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 || got[0].Line != "line-003" || got[3].Line != "line-006" {
		t.Fatalf("time filter wrong: %v", lines(got))
	}
}

func TestFollowReceivesLive(t *testing.T) {
	s := newStore(t, Options{})
	base := time.Unix(1_700_000_000, 0)
	_ = s.Append("svc", []Entry{line(base, 0)}) // history

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := s.Follow(ctx, Query{Streams: []StreamID{"svc"}})
	if err != nil {
		t.Fatal(err)
	}

	// First delivery is the history line.
	if e := recv(t, ch); e.Line != "line-000" {
		t.Fatalf("history line = %q", e.Line)
	}
	// Live appends arrive on the follow channel.
	_ = s.Append("svc", []Entry{line(base, 1)})
	_ = s.Append("svc", []Entry{line(base, 2)})
	if e := recv(t, ch); e.Line != "line-001" {
		t.Fatalf("live 1 = %q", e.Line)
	}
	if e := recv(t, ch); e.Line != "line-002" {
		t.Fatalf("live 2 = %q", e.Line)
	}
}

func TestRetentionDeletesOldest(t *testing.T) {
	root := t.TempDir()
	// Small segments + a tiny byte budget so old segments get reaped.
	s := newStore(t, Options{Root: root, MaxSegmentBytes: 128, RetentionBytes: 200})
	base := time.Unix(1_700_000_000, 0)
	for i := 0; i < 100; i++ {
		_ = s.Append("svc", []Entry{line(base, i)})
	}
	segs := listSegments(filepath.Join(root, "svc"))
	if len(segs) == 0 {
		t.Fatal("expected some segments")
	}
	// Total compressed size stays within (roughly) the budget: far fewer than
	// 100 lines survive, and the earliest are gone.
	got, _ := s.Query(context.Background(), Query{Streams: []StreamID{"svc"}, Limit: 1000})
	if len(got) == 0 {
		t.Fatal("retention deleted everything")
	}
	if got[0].Line == "line-000" {
		t.Fatal("oldest line should have been reaped by retention")
	}
}

func TestCrashRecoveryRecompressesRawSegment(t *testing.T) {
	root := t.TempDir()
	base := time.Unix(1_700_000_000, 0)

	// Write some lines, then simulate a crash mid-rotation: the active segment
	// was renamed to .raw but never compressed.
	s := newStore(t, Options{Root: root})
	for i := 0; i < 5; i++ {
		_ = s.Append("svc", []Entry{line(base, i)})
	}
	_ = s.Close()
	dir := filepath.Join(root, "svc")
	if err := os.Rename(filepath.Join(dir, activeSegmentName), filepath.Join(dir, "seg-00000000.raw")); err != nil {
		t.Fatal(err)
	}

	// Reopen: the .raw must be recovered (compressed) and its lines queryable.
	s2 := newStore(t, Options{Root: root})
	if _, err := os.Stat(filepath.Join(dir, "seg-00000000.zst")); err != nil {
		t.Fatalf("raw segment not recompressed on open: %v", err)
	}
	got, err := s2.Query(context.Background(), Query{Streams: []StreamID{"svc"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 5 {
		t.Fatalf("recovered %d lines, want 5: %v", len(got), lines(got))
	}
}

func TestDeleteStream(t *testing.T) {
	root := t.TempDir()
	s := newStore(t, Options{Root: root})
	_ = s.Append("svc", []Entry{{Time: time.Now(), Line: "x"}})
	if err := s.DeleteStream("svc"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "svc")); !os.IsNotExist(err) {
		t.Fatal("stream dir should be gone")
	}
	got, _ := s.Query(context.Background(), Query{Streams: []StreamID{"svc"}})
	if len(got) != 0 {
		t.Fatal("deleted stream should return no entries")
	}
}

func recv(t *testing.T, ch <-chan Entry) Entry {
	t.Helper()
	select {
	case e, ok := <-ch:
		if !ok {
			t.Fatal("follow channel closed unexpectedly")
		}
		return e
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for follow entry")
		return Entry{}
	}
}

func lines(es []Entry) []string {
	out := make([]string, len(es))
	for i, e := range es {
		out[i] = e.Line
	}
	return out
}
