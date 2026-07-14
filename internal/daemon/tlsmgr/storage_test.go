package tlsmgr

import (
	"context"
	"errors"
	"io/fs"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// memKV is an in-memory KV with CAS + TTL, mirroring the raft KV semantics.
type memKV struct {
	mu   sync.Mutex
	data map[string]memEntry
}
type memEntry struct {
	value   []byte
	version int64
	expires int64
}

func newMemKV() *memKV { return &memKV{data: map[string]memEntry{}} }

func (m *memKV) Get(key string) ([]byte, int64, int64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.data[key]
	if !ok {
		return nil, 0, 0, false
	}
	return append([]byte(nil), e.value...), e.version, e.expires, true
}

func (m *memKV) Put(_ context.Context, key string, value []byte, expectedVersion, expiresAtMs int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, exists := m.data[key]
	if expectedVersion >= 0 {
		curV := int64(0)
		if exists {
			curV = cur.version
		}
		if curV != expectedVersion {
			return 0, ErrConflict
		}
	}
	next := cur.version + 1
	m.data[key] = memEntry{value: append([]byte(nil), value...), version: next, expires: expiresAtMs}
	return next, nil
}

func (m *memKV) Delete(_ context.Context, key string, expectedVersion int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, exists := m.data[key]
	if !exists {
		return nil
	}
	if expectedVersion >= 0 && cur.version != expectedVersion {
		return ErrConflict
	}
	delete(m.data, key)
	return nil
}

func (m *memKV) ListPrefix(prefix string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for k := range m.data {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

func TestStorageRoundTrip(t *testing.T) {
	s := NewStorage(newMemKV(), clock.NewFake())
	ctx := context.Background()

	// Missing key → fs.ErrNotExist.
	if _, err := s.Load(ctx, "a/b.crt"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("load missing err = %v, want ErrNotExist", err)
	}
	if s.Exists(ctx, "a/b.crt") {
		t.Fatal("Exists should be false for missing key")
	}

	// Store / Load / Exists / Stat.
	val := []byte("PEM-DATA")
	if err := s.Store(ctx, "a/b.crt", val); err != nil {
		t.Fatal(err)
	}
	got, err := s.Load(ctx, "a/b.crt")
	if err != nil || string(got) != "PEM-DATA" {
		t.Fatalf("load = %q err=%v", got, err)
	}
	if !s.Exists(ctx, "a/b.crt") {
		t.Fatal("Exists should be true")
	}
	info, err := s.Stat(ctx, "a/b.crt")
	if err != nil || info.Size != int64(len(val)) || !info.IsTerminal || info.Key != "a/b.crt" {
		t.Fatalf("stat = %+v err=%v", info, err)
	}

	// A directory prefix exists once a child does.
	if !s.Exists(ctx, "a") {
		t.Fatal("directory prefix should exist")
	}
}

func TestStorageList(t *testing.T) {
	s := NewStorage(newMemKV(), clock.NewFake())
	ctx := context.Background()
	for _, k := range []string{"site/a.crt", "site/a.key", "site/sub/c.crt", "other/x"} {
		if err := s.Store(ctx, k, []byte("v")); err != nil {
			t.Fatal(err)
		}
	}

	// Recursive: every leaf under "site".
	rec, _ := s.List(ctx, "site", true)
	sort.Strings(rec)
	want := []string{"site/a.crt", "site/a.key", "site/sub/c.crt"}
	if strings.Join(rec, ",") != strings.Join(want, ",") {
		t.Fatalf("recursive list = %v, want %v", rec, want)
	}

	// Non-recursive: immediate children (two leaves + the "sub" directory).
	nonrec, _ := s.List(ctx, "site", false)
	sort.Strings(nonrec)
	wantImm := []string{"site/a.crt", "site/a.key", "site/sub"}
	if strings.Join(nonrec, ",") != strings.Join(wantImm, ",") {
		t.Fatalf("non-recursive list = %v, want %v", nonrec, wantImm)
	}
}

func TestStorageDeleteDirectory(t *testing.T) {
	s := NewStorage(newMemKV(), clock.NewFake())
	ctx := context.Background()
	_ = s.Store(ctx, "site/a.crt", []byte("v"))
	_ = s.Store(ctx, "site/sub/c.crt", []byte("v"))

	if err := s.Delete(ctx, "site"); err != nil {
		t.Fatal(err)
	}
	if s.Exists(ctx, "site/a.crt") || s.Exists(ctx, "site/sub/c.crt") {
		t.Fatal("directory delete should remove all children")
	}
}

func TestStorageLockMutualExclusion(t *testing.T) {
	s := NewStorage(newMemKV(), clock.Real{})
	s.lockTTL = 5 * time.Second // long enough that it won't expire during the test
	s.lockPoll = 5 * time.Millisecond
	ctx := context.Background()

	if err := s.Lock(ctx, "issue:example.com"); err != nil {
		t.Fatal(err)
	}
	acquired := make(chan time.Time, 1)
	go func() {
		_ = s.Lock(ctx, "issue:example.com")
		acquired <- time.Now()
	}()

	// The second lock must block while the first is held.
	select {
	case <-acquired:
		t.Fatal("second Lock acquired while the first was held")
	case <-time.After(50 * time.Millisecond):
	}

	release := time.Now()
	if err := s.Unlock(ctx, "issue:example.com"); err != nil {
		t.Fatal(err)
	}
	select {
	case at := <-acquired:
		if at.Before(release) {
			t.Fatal("second Lock acquired before Unlock")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second Lock never acquired after Unlock")
	}
}

func TestStorageLockStealAfterExpiry(t *testing.T) {
	s := NewStorage(newMemKV(), clock.Real{})
	s.lockTTL = 80 * time.Millisecond
	s.lockPoll = 5 * time.Millisecond
	ctx := context.Background()

	// Hold the lock and never release it — a crashed holder.
	if err := s.Lock(ctx, "x"); err != nil {
		t.Fatal(err)
	}
	acquired := make(chan struct{}, 1)
	go func() {
		_ = s.Lock(ctx, "x")
		acquired <- struct{}{}
	}()

	select {
	case <-acquired:
		// Stole the lock after its TTL elapsed.
	case <-time.After(2 * time.Second):
		t.Fatal("expired lock was not stealable")
	}
}
