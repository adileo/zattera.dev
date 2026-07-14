// Package tlsmgr manages TLS certificates for the ingress: ACME issuance via
// certmagic backed by the raft KV (cluster-wide, one issuer serialized by a
// distributed lock), and a dev mode that mints self-signed certs from the
// cluster CA on demand (T-44).
package tlsmgr

import (
	"context"
	"encoding/binary"
	"errors"
	"io/fs"
	"strings"
	"time"

	"github.com/caddyserver/certmagic"

	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// KV is the key-value surface the certmagic storage needs. In production the
// daemon backs it with the raft KV (writes via applyAnywhere, reads from
// state); tests inject an in-memory implementation.
type KV interface {
	// Get returns the value, version and expiry (unix ms; 0 = none) for a key.
	Get(key string) (value []byte, version int64, expiresAtMs int64, ok bool)
	// Put writes value with a compare-and-set on expectedVersion (-1 =
	// unconditional; 0 = must-not-exist), returning ErrConflict on mismatch.
	Put(ctx context.Context, key string, value []byte, expectedVersion, expiresAtMs int64) (newVersion int64, err error)
	// Delete removes a key (idempotent); expectedVersion -1 = unconditional.
	Delete(ctx context.Context, key string, expectedVersion int64) error
	// ListPrefix returns all keys with the given prefix, sorted.
	ListPrefix(prefix string) []string
}

// ErrConflict signals a compare-and-set version mismatch.
var ErrConflict = errors.New("tlsmgr: kv conflict")

const (
	storePrefix     = "certmagic/"
	lockPrefix      = "certmagiclock/"
	defaultLockTTL  = 2 * time.Minute
	defaultLockPoll = 500 * time.Millisecond
)

// Storage implements certmagic.Storage over a cluster KV. Certificate assets
// live under storePrefix; distributed locks (CAS + TTL) under lockPrefix.
type Storage struct {
	kv  KV
	clk clock.Clock

	lockTTL  time.Duration
	lockPoll time.Duration
}

// NewStorage builds the certmagic storage over a KV.
func NewStorage(kv KV, clk clock.Clock) *Storage {
	if clk == nil {
		clk = clock.Real{}
	}
	return &Storage{kv: kv, clk: clk, lockTTL: defaultLockTTL, lockPoll: defaultLockPoll}
}

var _ certmagic.Storage = (*Storage)(nil)

// Store writes value at key (prefixing an 8-byte modified timestamp so Stat can
// report it).
func (s *Storage) Store(ctx context.Context, key string, value []byte) error {
	_, err := s.kv.Put(ctx, storePrefix+key, s.wrap(value), -1, 0)
	return err
}

// Load reads the value at key. fs.ErrNotExist if absent.
func (s *Storage) Load(_ context.Context, key string) ([]byte, error) {
	raw, _, _, ok := s.kv.Get(storePrefix + key)
	if !ok {
		return nil, fs.ErrNotExist
	}
	value, _ := unwrap(raw)
	return value, nil
}

// Delete removes key and any keys beneath it (directory semantics).
func (s *Storage) Delete(ctx context.Context, key string) error {
	if err := s.kv.Delete(ctx, storePrefix+key, -1); err != nil {
		return err
	}
	for _, child := range s.kv.ListPrefix(storePrefix + key + "/") {
		if err := s.kv.Delete(ctx, child, -1); err != nil {
			return err
		}
	}
	return nil
}

// Exists reports whether key exists as a leaf or a directory prefix.
func (s *Storage) Exists(_ context.Context, key string) bool {
	if _, _, _, ok := s.kv.Get(storePrefix + key); ok {
		return true
	}
	return len(s.kv.ListPrefix(storePrefix+key+"/")) > 0
}

// Stat returns metadata for key. fs.ErrNotExist if absent.
func (s *Storage) Stat(_ context.Context, key string) (certmagic.KeyInfo, error) {
	raw, _, _, ok := s.kv.Get(storePrefix + key)
	if !ok {
		return certmagic.KeyInfo{}, fs.ErrNotExist
	}
	value, mod := unwrap(raw)
	return certmagic.KeyInfo{Key: key, Modified: mod, Size: int64(len(value)), IsTerminal: true}, nil
}

// List enumerates keys under prefix. recursive=true returns every leaf beneath
// it; recursive=false returns only immediate children (leaves as their key,
// subdirectories as their directory path).
func (s *Storage) List(_ context.Context, prefix string, recursive bool) ([]string, error) {
	full := s.kv.ListPrefix(storePrefix + prefix)
	if recursive {
		out := make([]string, 0, len(full))
		for _, k := range full {
			out = append(out, strings.TrimPrefix(k, storePrefix))
		}
		return out, nil
	}
	seen := map[string]bool{}
	var out []string
	base := strings.TrimSuffix(prefix, "/")
	for _, k := range full {
		ck := strings.TrimPrefix(k, storePrefix)
		rest := strings.TrimPrefix(strings.TrimPrefix(ck, base), "/")
		if rest == "" {
			continue
		}
		var child string
		if i := strings.IndexByte(rest, '/'); i >= 0 {
			child = base + "/" + rest[:i] // subdirectory
		} else {
			child = base + "/" + rest // leaf
		}
		child = strings.TrimPrefix(child, "/")
		if !seen[child] {
			seen[child] = true
			out = append(out, child)
		}
	}
	return out, nil
}

// Lock acquires the named distributed lock, blocking until acquired or ctx is
// canceled. A lock whose TTL has elapsed is stealable (CAS on its version).
func (s *Storage) Lock(ctx context.Context, name string) error {
	key := lockPrefix + name
	for {
		_, ver, exp, ok := s.kv.Get(key)
		now := s.clk.Now().UnixMilli()
		ttl := now + s.lockTTL.Milliseconds()
		if !ok {
			if _, err := s.kv.Put(ctx, key, []byte("1"), 0, ttl); err == nil {
				return nil
			}
		} else if exp != 0 && exp <= now {
			if _, err := s.kv.Put(ctx, key, []byte("1"), ver, ttl); err == nil {
				return nil // stole an expired lock
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.clk.After(s.lockPoll):
		}
	}
}

// Unlock releases a named lock.
func (s *Storage) Unlock(ctx context.Context, name string) error {
	return s.kv.Delete(ctx, lockPrefix+name, -1)
}

// wrap prefixes an 8-byte modified timestamp to value.
func (s *Storage) wrap(value []byte) []byte {
	buf := make([]byte, 8+len(value))
	binary.BigEndian.PutUint64(buf, uint64(s.clk.Now().UnixNano()))
	copy(buf[8:], value)
	return buf
}

// unwrap splits a stored value into its payload and modified time.
func unwrap(raw []byte) ([]byte, time.Time) {
	if len(raw) < 8 {
		return raw, time.Time{}
	}
	nano := int64(binary.BigEndian.Uint64(raw[:8]))
	return raw[8:], time.Unix(0, nano)
}
