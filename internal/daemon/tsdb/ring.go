package tsdb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// Slot counts derived from the resolutions in store.go: RawStep×rawSlots covers
// RawRetention, DownStep×downSlots covers DownRetention.
const (
	rawSlots  = int(RawRetention / RawStep)   // 5760 (24h at 15s)
	downSlots = int(DownRetention / DownStep) // 8640 (30d at 5m)

	// gcAge drops a series whose newest sample is older than this (bounded
	// cardinality: instances come and go).
	gcAge = 48 * time.Hour

	flushInterval = 60 * time.Second

	// Persistence framing.
	fileMagic   uint32 = 0x5A54_5344 // "ZTSD"
	fileVersion uint32 = 1
	emptySlot   int64  = -1 // sentinel for an unwritten ring position
)

// ring is one fixed-size resolution buffer. abs[i] holds the absolute slot
// number (unixSec/stepSec) currently stored at position i, or emptySlot; val[i]
// is its float32 value. cnt[i] counts samples averaged into a downsampled slot
// (nil for the raw ring, which keeps last-write-wins).
type ring struct {
	abs []int64
	val []float32
	cnt []uint16
}

func newRing(size int, downsampled bool) ring {
	r := ring{abs: make([]int64, size), val: make([]float32, size)}
	for i := range r.abs {
		r.abs[i] = emptySlot
	}
	if downsampled {
		r.cnt = make([]uint16, size)
	}
	return r
}

// series holds both resolutions for one SeriesKey plus the highest raw slot
// seen, which gates out-of-order drops and drives downsample feeding.
type series struct {
	raw    ring
	down   ring
	curAbs int64 // highest raw abs slot recorded; emptySlot if none
}

func newSeries() *series {
	return &series{raw: newRing(rawSlots, false), down: newRing(downSlots, true), curAbs: emptySlot}
}

// RingStore is the on-node ring TSDB (spec §3.10). Zero series persist to a flat
// file; a background goroutine flushes every 60s until Close.
type RingStore struct {
	path string
	clk  clock.Clock
	log  *slog.Logger

	mu     sync.RWMutex
	series map[SeriesKey]*series

	stop   chan struct{}
	stopWG sync.WaitGroup
}

// Config configures a RingStore.
type Config struct {
	// Path is the flat file the rings persist to. Empty disables persistence
	// (in-memory only, e.g. tests).
	Path   string
	Clock  clock.Clock
	Logger *slog.Logger
}

// Open builds a RingStore, loading any existing file (a missing or corrupt file
// starts empty with a warning) and starting the periodic flusher.
func Open(cfg Config) *RingStore {
	if cfg.Clock == nil {
		cfg.Clock = clock.Real{}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	s := &RingStore{
		path:   cfg.Path,
		clk:    cfg.Clock,
		log:    cfg.Logger,
		series: map[SeriesKey]*series{},
		stop:   make(chan struct{}),
	}
	if cfg.Path != "" {
		if err := os.MkdirAll(filepath.Dir(cfg.Path), 0o755); err != nil {
			s.log.Warn("tsdb: could not create data dir; disabling persistence", "path", cfg.Path, "err", err)
			s.path = ""
			return s
		}
		if err := s.load(); err != nil {
			s.log.Warn("tsdb: could not load store; starting empty", "path", cfg.Path, "err", err)
			s.series = map[SeriesKey]*series{}
		}
		s.stopWG.Add(1)
		go s.flushLoop()
	}
	return s
}

var _ Store = (*RingStore)(nil)

// rawStepSec and downStepSec are the resolutions in whole seconds; slot math is
// integer division so it never drifts.
var (
	rawStepSec  = int64(RawStep / time.Second)
	downStepSec = int64(DownStep / time.Second)
)

// Record adds a sample. Samples in the current or a newer raw slot are kept
// (same-slot re-samples overwrite last-write-wins); an older slot is dropped.
func (s *RingStore) Record(key SeriesKey, p Point) {
	unix := p.Time.Unix()
	rawAbs := unix / rawStepSec
	if unix < 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	se := s.series[key]
	if se == nil {
		se = newSeries()
		s.series[key] = se
	}
	if se.curAbs != emptySlot && rawAbs < se.curAbs {
		return // out of order, older than the current slot
	}
	v := float32(p.Value)
	// Raw ring: last write wins for the slot.
	rpos := int(rawAbs % int64(rawSlots))
	se.raw.abs[rpos] = rawAbs
	se.raw.val[rpos] = v
	// Feed the downsample only on a genuinely new raw slot so each raw slot
	// contributes to the 5m average exactly once.
	if rawAbs > se.curAbs {
		se.feedDown(unix/downStepSec, v)
		se.curAbs = rawAbs
	}
}

// feedDown folds v into the running average for its 5m slot.
func (se *series) feedDown(downAbs int64, v float32) {
	pos := int(downAbs % int64(downSlots))
	if se.down.abs[pos] != downAbs {
		se.down.abs[pos] = downAbs
		se.down.val[pos] = v
		se.down.cnt[pos] = 1
		return
	}
	c := se.down.cnt[pos]
	se.down.val[pos] += (v - se.down.val[pos]) / float32(c+1)
	if c < ^uint16(0) {
		se.down.cnt[pos] = c + 1
	}
}

// Query returns points in [since, until] at the resolution best fitting step:
// the 5m ring when step is at least DownStep, else the raw 15s ring.
func (s *RingStore) Query(key SeriesKey, since, until time.Time, step time.Duration) []Point {
	if until.Before(since) {
		return nil
	}
	stepSec, r, size := rawStepSec, "raw", rawSlots
	if step >= DownStep {
		stepSec, r, size = downStepSec, "down", downSlots
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	se := s.series[key]
	if se == nil {
		return nil
	}
	ring := se.raw
	if r == "down" {
		ring = se.down
	}
	fromAbs := since.Unix() / stepSec
	toAbs := until.Unix() / stepSec
	// Only the newest `size` slots survive in the ring; skip older requests so
	// a huge range doesn't loop uselessly.
	if lo := toAbs - int64(size) + 1; fromAbs < lo {
		fromAbs = lo
	}
	out := make([]Point, 0, toAbs-fromAbs+1)
	for abs := fromAbs; abs <= toAbs; abs++ {
		pos := int(((abs % int64(size)) + int64(size)) % int64(size))
		if ring.abs[pos] == abs {
			out = append(out, Point{Time: time.Unix(abs*stepSec, 0).UTC(), Value: float64(ring.val[pos])})
		}
	}
	return out
}

// Keys lists series matching a scope filter. Empty scope matches all; empty
// scopeID matches all ids within the scope.
func (s *RingStore) Keys(scope, scopeID string) []SeriesKey {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]SeriesKey, 0, len(s.series))
	for k := range s.series {
		if scope != "" && k.Scope != scope {
			continue
		}
		if scopeID != "" && k.ScopeID != scopeID {
			continue
		}
		out = append(out, k)
	}
	return out
}

// Flush garbage-collects stale series then persists the rings to disk. A no-op
// when persistence is disabled (empty Path).
func (s *RingStore) Flush() error {
	if s.path == "" {
		return nil
	}
	s.gc()
	return s.persist()
}

// Close stops the flusher and performs a final flush.
func (s *RingStore) Close() error {
	select {
	case <-s.stop:
	default:
		close(s.stop)
	}
	s.stopWG.Wait()
	return s.Flush()
}

// flushLoop persists on a fixed cadence until Close.
func (s *RingStore) flushLoop() {
	defer s.stopWG.Done()
	tick := s.clk.NewTicker(flushInterval)
	defer tick.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-tick.C():
			if err := s.Flush(); err != nil {
				s.log.Warn("tsdb: flush failed", "path", s.path, "err", err)
			}
		}
	}
}

// gc drops series whose newest sample is older than gcAge.
func (s *RingStore) gc() {
	cutoff := s.clk.Now().Add(-gcAge).Unix() / rawStepSec
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, se := range s.series {
		if se.curAbs == emptySlot || se.curAbs < cutoff {
			delete(s.series, k)
		}
	}
}

// persist writes every series to a temp file then renames it into place so a
// crash mid-write never corrupts the store.
func (s *RingStore) persist() error {
	tmp := s.path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("tsdb: create temp: %w", err)
	}
	w := bufio.NewWriter(f)
	if err := s.encode(w); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("tsdb: flush buffer: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("tsdb: sync: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("tsdb: close: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("tsdb: rename: %w", err)
	}
	return nil
}

func (s *RingStore) encode(w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	le := binary.LittleEndian
	hdr := make([]byte, 12)
	le.PutUint32(hdr[0:], fileMagic)
	le.PutUint32(hdr[4:], fileVersion)
	le.PutUint32(hdr[8:], uint32(len(s.series)))
	if _, err := w.Write(hdr); err != nil {
		return err
	}
	for k, se := range s.series {
		if err := writeString(w, k.Metric); err != nil {
			return err
		}
		if err := writeString(w, k.Scope); err != nil {
			return err
		}
		if err := writeString(w, k.ScopeID); err != nil {
			return err
		}
		if err := binary.Write(w, le, se.curAbs); err != nil {
			return err
		}
		if err := binary.Write(w, le, se.raw.abs); err != nil {
			return err
		}
		if err := binary.Write(w, le, se.raw.val); err != nil {
			return err
		}
		if err := binary.Write(w, le, se.down.abs); err != nil {
			return err
		}
		if err := binary.Write(w, le, se.down.val); err != nil {
			return err
		}
		if err := binary.Write(w, le, se.down.cnt); err != nil {
			return err
		}
	}
	return nil
}

// load reads the store file. A missing file is not an error (start empty); any
// other failure returns an error so Open can warn and start empty.
func (s *RingStore) load() error {
	f, err := os.Open(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer func() { _ = f.Close() }()
	return s.decode(bufio.NewReader(f))
}

func (s *RingStore) decode(r io.Reader) error {
	le := binary.LittleEndian
	hdr := make([]byte, 12)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return fmt.Errorf("tsdb: read header: %w", err)
	}
	if le.Uint32(hdr[0:]) != fileMagic {
		return errors.New("tsdb: bad magic")
	}
	if v := le.Uint32(hdr[4:]); v != fileVersion {
		return fmt.Errorf("tsdb: unsupported version %d", v)
	}
	n := le.Uint32(hdr[8:])
	loaded := make(map[SeriesKey]*series, n)
	for i := uint32(0); i < n; i++ {
		var k SeriesKey
		var err error
		if k.Metric, err = readString(r); err != nil {
			return err
		}
		if k.Scope, err = readString(r); err != nil {
			return err
		}
		if k.ScopeID, err = readString(r); err != nil {
			return err
		}
		se := newSeries()
		if err := binary.Read(r, le, &se.curAbs); err != nil {
			return err
		}
		if err := binary.Read(r, le, se.raw.abs); err != nil {
			return err
		}
		if err := binary.Read(r, le, se.raw.val); err != nil {
			return err
		}
		if err := binary.Read(r, le, se.down.abs); err != nil {
			return err
		}
		if err := binary.Read(r, le, se.down.val); err != nil {
			return err
		}
		if err := binary.Read(r, le, se.down.cnt); err != nil {
			return err
		}
		loaded[k] = se
	}
	s.mu.Lock()
	s.series = loaded
	s.mu.Unlock()
	return nil
}

func writeString(w io.Writer, str string) error {
	if err := binary.Write(w, binary.LittleEndian, uint32(len(str))); err != nil {
		return err
	}
	_, err := io.WriteString(w, str)
	return err
}

func readString(r io.Reader) (string, error) {
	var n uint32
	if err := binary.Read(r, binary.LittleEndian, &n); err != nil {
		return "", err
	}
	// Guard against a corrupt length claiming gigabytes.
	if n > 1<<20 {
		return "", fmt.Errorf("tsdb: implausible string length %d", n)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}
