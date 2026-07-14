package logstore

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// Defaults for segment rotation and retention.
const (
	defaultMaxSegmentBytes = 8 << 20
	defaultMaxSegmentAge   = time.Hour
	defaultQueryLimit      = 1000
	subscriberBuffer       = 256
	activeSegmentName      = "active.log"
)

// Options configures a Segmented store.
type Options struct {
	Root            string // <data-dir>/logs
	Clock           clock.Clock
	MaxSegmentBytes int64         // 0 = default 8MB
	MaxSegmentAge   time.Duration // 0 = default 1h
	RetentionBytes  int64         // per-stream compressed cap; 0 = unlimited
	RetentionAge    time.Duration // 0 = unlimited
}

// Segmented is a per-node, on-disk log store: an uncompressed active segment
// per stream, rotated (at size/age) into zstd-compressed segments with sparse
// time indexes. Reads binary-search segments by time; Follow tails live appends
// through an in-memory hub.
type Segmented struct {
	opts Options
	clk  clock.Clock

	mu      sync.Mutex
	streams map[string]*streamState
}

type streamState struct {
	dir string

	mu     sync.Mutex
	w      *bufio.Writer
	f      *os.File
	size   int64
	opened time.Time
	nextID int
	subs   map[int]chan Entry
}

// New opens (creating) the log root and prepares the store.
func New(opts Options) (*Segmented, error) {
	if opts.Clock == nil {
		opts.Clock = clock.Real{}
	}
	if opts.MaxSegmentBytes == 0 {
		opts.MaxSegmentBytes = defaultMaxSegmentBytes
	}
	if opts.MaxSegmentAge == 0 {
		opts.MaxSegmentAge = defaultMaxSegmentAge
	}
	if err := os.MkdirAll(opts.Root, 0o755); err != nil {
		return nil, fmt.Errorf("logstore: root: %w", err)
	}
	s := &Segmented{opts: opts, clk: opts.Clock, streams: map[string]*streamState{}}
	// Eagerly open existing streams so any interrupted rotation is recovered at
	// startup (a leftover .raw segment is recompressed).
	if entries, err := os.ReadDir(opts.Root); err == nil {
		for _, e := range entries {
			if e.IsDir() {
				if _, err := s.getStream(StreamID(e.Name()), true); err != nil {
					return nil, err
				}
			}
		}
	}
	return s, nil
}

// sanitizeStream maps a StreamID to a safe directory name.
func sanitizeStream(id StreamID) string {
	s := string(id)
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return "_"
	}
	return b.String()
}

func (s *Segmented) getStream(id StreamID, create bool) (*streamState, error) {
	name := sanitizeStream(id)
	s.mu.Lock()
	ss, ok := s.streams[name]
	s.mu.Unlock()
	if ok {
		return ss, nil
	}
	if !create {
		dir := filepath.Join(s.opts.Root, name)
		if _, err := os.Stat(dir); err != nil {
			return nil, nil // unknown stream
		}
	}
	ss = &streamState{dir: filepath.Join(s.opts.Root, name), subs: map[int]chan Entry{}}
	if err := ss.open(s); err != nil {
		return nil, err
	}
	s.mu.Lock()
	if existing, ok := s.streams[name]; ok { // lost a race
		s.mu.Unlock()
		_ = ss.close()
		return existing, nil
	}
	s.streams[name] = ss
	s.mu.Unlock()
	return ss, nil
}

// open prepares the stream directory, recovering any interrupted rotation.
func (ss *streamState) open(s *Segmented) error {
	if err := os.MkdirAll(ss.dir, 0o755); err != nil {
		return fmt.Errorf("logstore: stream dir: %w", err)
	}
	// Recover: finish any leftover .raw segments; drop stale .tmp files.
	entries, _ := os.ReadDir(ss.dir)
	for _, e := range entries {
		name := e.Name()
		switch {
		case strings.HasSuffix(name, ".tmp"):
			_ = os.Remove(filepath.Join(ss.dir, name))
		case strings.HasSuffix(name, ".raw"):
			seq := seqFromName(name)
			if seq >= 0 {
				_ = compressSegment(ss.dir, seq)
			}
		}
	}
	f, err := os.OpenFile(filepath.Join(ss.dir, activeSegmentName), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("logstore: open active: %w", err)
	}
	fi, _ := f.Stat()
	ss.f = f
	ss.w = bufio.NewWriter(f)
	ss.size = fi.Size()
	ss.opened = s.clk.Now()
	return nil
}

func (ss *streamState) close() error {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if ss.w != nil {
		_ = ss.w.Flush()
	}
	if ss.f != nil {
		return ss.f.Close()
	}
	return nil
}

// Append writes entries to a stream, rotating when the active segment is full,
// and notifies live followers.
func (s *Segmented) Append(stream StreamID, entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	ss, err := s.getStream(stream, true)
	if err != nil {
		return err
	}
	ss.mu.Lock()
	var buf []byte
	for _, e := range entries {
		buf = appendRecord(buf, e)
	}
	if _, err := ss.w.Write(buf); err != nil {
		ss.mu.Unlock()
		return fmt.Errorf("logstore: append: %w", err)
	}
	if err := ss.w.Flush(); err != nil {
		ss.mu.Unlock()
		return err
	}
	ss.size += int64(len(buf))
	rotate := ss.size >= s.opts.MaxSegmentBytes || s.clk.Now().Sub(ss.opened) >= s.opts.MaxSegmentAge
	// Notify followers (drop-and-close slow ones).
	for id, ch := range ss.subs {
		for _, e := range entries {
			select {
			case ch <- e:
			default:
				close(ch)
				delete(ss.subs, id)
			}
		}
	}
	if rotate {
		if err := ss.rotate(s); err != nil {
			ss.mu.Unlock()
			return err
		}
	}
	ss.mu.Unlock()
	return nil
}

// rotate closes the active segment, compresses it, and starts a new one. Called
// with ss.mu held.
func (ss *streamState) rotate(s *Segmented) error {
	if ss.size == 0 {
		ss.opened = s.clk.Now()
		return nil
	}
	if err := ss.w.Flush(); err != nil {
		return err
	}
	if err := ss.f.Close(); err != nil {
		return err
	}
	seq := nextSeq(ss.dir)
	raw := filepath.Join(ss.dir, fmt.Sprintf("seg-%08d.raw", seq))
	if err := os.Rename(filepath.Join(ss.dir, activeSegmentName), raw); err != nil {
		return fmt.Errorf("logstore: rotate rename: %w", err)
	}
	if err := compressSegment(ss.dir, seq); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(ss.dir, activeSegmentName), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	ss.f = f
	ss.w = bufio.NewWriter(f)
	ss.size = 0
	ss.opened = s.clk.Now()
	s.enforceRetention(ss.dir)
	return nil
}

// compressSegment turns seg-<seq>.raw into a zstd segment + sparse time index,
// then removes the raw file. Crash-safe via .tmp + rename.
func compressSegment(dir string, seq int) error {
	raw := filepath.Join(dir, fmt.Sprintf("seg-%08d.raw", seq))
	rf, err := os.Open(raw)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer func() { _ = rf.Close() }()

	zstTmp := filepath.Join(dir, fmt.Sprintf("seg-%08d.zst.tmp", seq))
	zf, err := os.Create(zstTmp)
	if err != nil {
		return err
	}
	enc, err := zstd.NewWriter(zf)
	if err != nil {
		_ = zf.Close()
		return err
	}

	// Scan records to build the index while streaming into the compressor.
	var minNano, maxNano int64
	var marks []indexMark
	var offset int64
	var lastMark int64 = -indexMarkStride
	br := bufio.NewReader(rf)
	first := true
	scanRecords(br, func(e Entry) bool {
		nano := e.Time.UnixNano()
		if first || nano < minNano {
			minNano = nano
		}
		if first || nano > maxNano {
			maxNano = nano
		}
		if offset-lastMark >= indexMarkStride {
			marks = append(marks, indexMark{offset: offset, nano: nano})
			lastMark = offset
		}
		first = false
		enc, err = writeRecordToEncoder(enc, e)
		offset += recordEncodedLen(e)
		return err == nil
	})
	if err == nil {
		err = enc.Close()
	}
	if cerr := zf.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(zstTmp)
		return fmt.Errorf("logstore: compress: %w", err)
	}

	idxTmp := filepath.Join(dir, fmt.Sprintf("seg-%08d.idx.tmp", seq))
	if err := writeIndex(idxTmp, minNano, maxNano, marks); err != nil {
		_ = os.Remove(zstTmp)
		return err
	}
	if err := os.Rename(zstTmp, filepath.Join(dir, fmt.Sprintf("seg-%08d.zst", seq))); err != nil {
		return err
	}
	if err := os.Rename(idxTmp, filepath.Join(dir, fmt.Sprintf("seg-%08d.idx", seq))); err != nil {
		return err
	}
	return os.Remove(raw)
}

// writeRecordToEncoder writes one record's bytes into the zstd stream.
func writeRecordToEncoder(enc *zstd.Encoder, e Entry) (*zstd.Encoder, error) {
	_, err := enc.Write(appendRecord(nil, e))
	return enc, err
}

func recordEncodedLen(e Entry) int64 {
	body := recordHeaderBytes + len(e.Line)
	return int64(uvarintLen(uint64(body)) + body)
}

func uvarintLen(v uint64) int {
	var b [binary.MaxVarintLen64]byte
	return binary.PutUvarint(b[:], v)
}

// Query returns matching entries in time order, capped to the most recent
// Limit.
func (s *Segmented) Query(ctx context.Context, q Query) ([]Entry, error) {
	limit := q.Limit
	if limit == 0 {
		limit = defaultQueryLimit
	}
	var all []Entry
	for _, sid := range q.Streams {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		ss, err := s.getStream(sid, false)
		if err != nil || ss == nil {
			continue
		}
		entries, err := ss.read(q.Since, q.Until)
		if err != nil {
			return nil, err
		}
		all = append(all, entries...)
	}
	sort.SliceStable(all, func(i, j int) bool { return all[i].Time.Before(all[j].Time) })
	if len(all) > limit {
		all = all[len(all)-limit:]
	}
	return all, nil
}

// read gathers a stream's records in [since, until], skipping segments whose
// index time range does not overlap.
func (ss *streamState) read(since, until time.Time) ([]Entry, error) {
	// Snapshot the active segment (flush + size) under lock, then read files
	// (closed segments are immutable) without holding the lock.
	ss.mu.Lock()
	if ss.w != nil {
		_ = ss.w.Flush()
	}
	activeSize := ss.size
	dir := ss.dir
	ss.mu.Unlock()

	segs := listSegments(dir)
	var out []Entry
	keep := func(e Entry) bool {
		if !since.IsZero() && e.Time.Before(since) {
			return false
		}
		if !until.IsZero() && e.Time.After(until) {
			return false
		}
		return true
	}
	for _, seq := range segs {
		minNano, maxNano, ok := readIndexRange(dir, seq)
		if ok {
			if !until.IsZero() && minNano > until.UnixNano() {
				continue
			}
			if !since.IsZero() && maxNano < since.UnixNano() {
				continue
			}
		}
		recs, err := readCompressedSegment(dir, seq)
		if err != nil {
			return nil, err
		}
		for _, e := range recs {
			if keep(e) {
				out = append(out, e)
			}
		}
	}
	// Active segment (up to the flushed size).
	af, err := os.Open(filepath.Join(dir, activeSegmentName))
	if err == nil {
		br := bufio.NewReader(io.LimitReader(af, activeSize))
		scanRecords(br, func(e Entry) bool {
			if keep(e) {
				out = append(out, e)
			}
			return true
		})
		_ = af.Close()
	}
	return out, nil
}

func readCompressedSegment(dir string, seq int) ([]Entry, error) {
	f, err := os.Open(filepath.Join(dir, fmt.Sprintf("seg-%08d.zst", seq)))
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	dec, err := zstd.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer dec.Close()
	var out []Entry
	br := bufio.NewReader(dec)
	scanRecords(br, func(e Entry) bool {
		out = append(out, e)
		return true
	})
	return out, nil
}

// Follow returns history then tails live appends until ctx is canceled.
func (s *Segmented) Follow(ctx context.Context, q Query) (<-chan Entry, error) {
	out := make(chan Entry, subscriberBuffer)

	type sub struct {
		ss *streamState
		id int
		ch chan Entry
	}
	var subs []sub
	for _, sid := range q.Streams {
		ss, err := s.getStream(sid, true)
		if err != nil {
			return nil, err
		}
		id, ch := ss.subscribe()
		subs = append(subs, sub{ss, id, ch})
	}

	go func() {
		defer close(out)
		defer func() {
			for _, sh := range subs {
				sh.ss.unsubscribe(sh.id)
			}
		}()

		hist, err := s.Query(ctx, q)
		if err == nil {
			for _, e := range hist {
				select {
				case out <- e:
				case <-ctx.Done():
					return
				}
			}
		}

		merged := make(chan Entry, subscriberBuffer)
		lag := make(chan struct{}, 1)
		for _, sh := range subs {
			go func(ch chan Entry) {
				for {
					select {
					case <-ctx.Done():
						return
					case e, ok := <-ch:
						if !ok {
							select {
							case lag <- struct{}{}:
							default:
							}
							return
						}
						select {
						case merged <- e:
						case <-ctx.Done():
							return
						}
					}
				}
			}(sh.ch)
		}
		for {
			select {
			case <-ctx.Done():
				return
			case <-lag:
				select {
				case out <- Entry{Time: s.clk.Now(), Line: "--- log stream lagged; some lines dropped ---"}:
				default:
				}
				return
			case e := <-merged:
				select {
				case out <- e:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

func (ss *streamState) subscribe() (int, chan Entry) {
	ch := make(chan Entry, subscriberBuffer)
	ss.mu.Lock()
	id := ss.nextID
	ss.nextID++
	ss.subs[id] = ch
	ss.mu.Unlock()
	return id, ch
}

func (ss *streamState) unsubscribe(id int) {
	ss.mu.Lock()
	delete(ss.subs, id)
	ss.mu.Unlock()
}

// DeleteStream removes a stream's directory and forgets it.
func (s *Segmented) DeleteStream(stream StreamID) error {
	name := sanitizeStream(stream)
	s.mu.Lock()
	ss, ok := s.streams[name]
	delete(s.streams, name)
	s.mu.Unlock()
	if ok {
		_ = ss.close()
	}
	return os.RemoveAll(filepath.Join(s.opts.Root, name))
}

// Close flushes and closes all active streams.
func (s *Segmented) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ss := range s.streams {
		_ = ss.close()
	}
	return nil
}

// enforceRetention deletes the oldest compressed segments beyond the size/age
// caps.
func (s *Segmented) enforceRetention(dir string) {
	segs := listSegments(dir)
	// Age cap: drop segments whose whole time range is older than the cutoff.
	if s.opts.RetentionAge > 0 {
		cutoff := s.clk.Now().Add(-s.opts.RetentionAge).UnixNano()
		for _, seq := range segs {
			if _, maxNano, ok := readIndexRange(dir, seq); ok && maxNano < cutoff {
				removeSegment(dir, seq)
			}
		}
		segs = listSegments(dir)
	}
	// Size cap: drop oldest until under the byte budget.
	if s.opts.RetentionBytes > 0 {
		total := int64(0)
		sizes := map[int]int64{}
		for _, seq := range segs {
			if fi, err := os.Stat(filepath.Join(dir, fmt.Sprintf("seg-%08d.zst", seq))); err == nil {
				sizes[seq] = fi.Size()
				total += fi.Size()
			}
		}
		for _, seq := range segs { // ascending = oldest first
			if total <= s.opts.RetentionBytes {
				break
			}
			total -= sizes[seq]
			removeSegment(dir, seq)
		}
	}
}

func removeSegment(dir string, seq int) {
	_ = os.Remove(filepath.Join(dir, fmt.Sprintf("seg-%08d.zst", seq)))
	_ = os.Remove(filepath.Join(dir, fmt.Sprintf("seg-%08d.idx", seq)))
}

// --- segment/index file helpers ---

const (
	indexMarkStride = 64 << 10 // sparse index mark every 64KB uncompressed
)

type indexMark struct {
	offset int64
	nano   int64
}

func writeIndex(path string, minNano, maxNano int64, marks []indexMark) error {
	buf := make([]byte, 0, 20+len(marks)*16)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(minNano))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(maxNano))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(marks)))
	for _, m := range marks {
		buf = binary.LittleEndian.AppendUint64(buf, uint64(m.offset))
		buf = binary.LittleEndian.AppendUint64(buf, uint64(m.nano))
	}
	return os.WriteFile(path, buf, 0o644)
}

func readIndexRange(dir string, seq int) (minNano, maxNano int64, ok bool) {
	data, err := os.ReadFile(filepath.Join(dir, fmt.Sprintf("seg-%08d.idx", seq)))
	if err != nil || len(data) < 16 {
		return 0, 0, false
	}
	return int64(binary.LittleEndian.Uint64(data[:8])), int64(binary.LittleEndian.Uint64(data[8:16])), true
}

// listSegments returns compressed segment sequence numbers in ascending order.
func listSegments(dir string) []int {
	entries, _ := os.ReadDir(dir)
	var seqs []int
	for _, e := range entries {
		if seq := seqFromName(e.Name()); seq >= 0 && strings.HasSuffix(e.Name(), ".zst") {
			seqs = append(seqs, seq)
		}
	}
	sort.Ints(seqs)
	return seqs
}

// nextSeq returns the next segment sequence number for a stream dir.
func nextSeq(dir string) int {
	max := -1
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if seq := seqFromName(e.Name()); seq > max {
			max = seq
		}
	}
	return max + 1
}

// seqFromName extracts N from "seg-<N>.<ext>", or -1.
func seqFromName(name string) int {
	if !strings.HasPrefix(name, "seg-") {
		return -1
	}
	rest := name[len("seg-"):]
	dot := strings.IndexByte(rest, '.')
	if dot < 0 {
		return -1
	}
	var n int
	if _, err := fmt.Sscanf(rest[:dot], "%d", &n); err != nil {
		return -1
	}
	return n
}
