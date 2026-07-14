package registry

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"sync"
	"time"

	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
)

// uploadTTL is how long an idle upload session survives before the janitor
// reaps it and deletes its temp file.
const uploadTTL = 24 * time.Hour

// Upload is one in-progress, resumable blob upload. Bytes are streamed to a
// temp file while a running sha256 is maintained, so finalize can verify the
// client-declared digest without re-reading the file. Uploads are sequential:
// a PATCH must continue exactly where the previous chunk ended.
type Upload struct {
	ID      string
	Started time.Time

	mu     sync.Mutex
	f      *os.File
	hash   hash.Hash
	offset int64 // bytes written so far
}

// Offset returns the number of bytes accepted so far (the next expected
// Content-Range start).
func (u *Upload) Offset() int64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.offset
}

// Uploads tracks live upload sessions and owns their lifecycle against the
// backing BlobStore.
type Uploads struct {
	store *BlobStore
	clk   clock.Clock

	mu       sync.Mutex
	sessions map[string]*Upload
}

// NewUploads constructs the upload manager. The clock drives session expiry.
func NewUploads(store *BlobStore, clk clock.Clock) *Uploads {
	return &Uploads{store: store, clk: clk, sessions: map[string]*Upload{}}
}

// Start opens a new upload session with an empty temp file.
func (m *Uploads) Start() (*Upload, error) {
	id := ids.New()
	f, err := m.store.newUploadFile(id)
	if err != nil {
		return nil, err
	}
	up := &Upload{ID: id, Started: m.clk.Now(), f: f, hash: sha256.New()}
	m.mu.Lock()
	m.sessions[id] = up
	m.mu.Unlock()
	return up, nil
}

// Get returns a live session by id.
func (m *Uploads) Get(id string) (*Upload, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	up, ok := m.sessions[id]
	return up, ok
}

// Append writes r to the session's temp file. When start >= 0 it is a declared
// Content-Range start and MUST equal the session's current offset (uploads are
// strictly sequential); start < 0 means "append at the current offset" (a
// monolithic PATCH with no Content-Range). Returns the new offset.
func (m *Uploads) Append(id string, r io.Reader, start int64) (int64, error) {
	up, ok := m.Get(id)
	if !ok {
		return 0, ErrUploadUnknown
	}
	up.mu.Lock()
	defer up.mu.Unlock()

	if start >= 0 && start != up.offset {
		return up.offset, ErrRangeNotSatisfiable
	}
	n, err := io.Copy(io.MultiWriter(up.f, up.hash), r)
	up.offset += n
	if err != nil {
		return up.offset, fmt.Errorf("registry: append upload: %w", err)
	}
	return up.offset, nil
}

// Finalize appends any trailing bytes, verifies the running digest against the
// client-declared expected digest, then commits the blob. On success the
// session is closed and removed. On a digest mismatch the session is discarded
// (its temp file deleted) so a bad upload can never linger or corrupt a blob.
func (m *Uploads) Finalize(id, expected string, trailing io.Reader) (string, error) {
	if _, err := parseDigest(expected); err != nil {
		return "", err
	}
	up, ok := m.Get(id)
	if !ok {
		return "", ErrUploadUnknown
	}
	up.mu.Lock()
	defer up.mu.Unlock()

	if trailing != nil {
		n, err := io.Copy(io.MultiWriter(up.f, up.hash), trailing)
		up.offset += n
		if err != nil {
			return "", fmt.Errorf("registry: finalize append: %w", err)
		}
	}
	if err := up.f.Close(); err != nil {
		return "", fmt.Errorf("registry: close upload: %w", err)
	}

	got := "sha256:" + hex.EncodeToString(up.hash.Sum(nil))
	if got != expected {
		m.drop(id)
		return "", ErrDigestMismatch
	}
	hexPart, _ := parseDigest(got) // already validated above
	if err := m.store.commit(up.f.Name(), hexPart); err != nil {
		return "", err
	}
	m.remove(id)
	return got, nil
}

// Cancel aborts an upload session, deleting its temp file.
func (m *Uploads) Cancel(id string) error {
	if _, ok := m.Get(id); !ok {
		return ErrUploadUnknown
	}
	m.drop(id)
	return nil
}

// Reap deletes sessions idle longer than uploadTTL and returns how many were
// removed. Intended to be called periodically by a janitor.
func (m *Uploads) Reap() int {
	cutoff := m.clk.Now().Add(-uploadTTL)
	m.mu.Lock()
	var stale []string
	for id, up := range m.sessions {
		if up.Started.Before(cutoff) {
			stale = append(stale, id)
		}
	}
	m.mu.Unlock()
	for _, id := range stale {
		m.drop(id)
	}
	return len(stale)
}

// drop closes and deletes a session's temp file, then forgets it.
func (m *Uploads) drop(id string) {
	m.mu.Lock()
	up, ok := m.sessions[id]
	delete(m.sessions, id)
	m.mu.Unlock()
	if !ok {
		return
	}
	name := up.f.Name()
	_ = up.f.Close()
	_ = os.Remove(name)
}

// remove forgets a session whose temp file was already consumed (renamed into
// the blob store), without touching the file.
func (m *Uploads) remove(id string) {
	m.mu.Lock()
	delete(m.sessions, id)
	m.mu.Unlock()
}

// Ingest performs a one-shot monolithic upload (POST ?digest= or a PUT with
// the full body): start → append → finalize in a single call.
func (m *Uploads) Ingest(r io.Reader, expected string) (string, error) {
	up, err := m.Start()
	if err != nil {
		return "", err
	}
	if _, err := m.Append(up.ID, r, -1); err != nil {
		_ = m.Cancel(up.ID)
		return "", err
	}
	return m.Finalize(up.ID, expected, nil)
}
