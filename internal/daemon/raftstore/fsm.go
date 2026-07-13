// Package raftstore wires hashicorp/raft to the in-memory state store
// (ADR-0004). The FSM decodes protobuf Commands and applies them; snapshots
// serialize the whole state as one Snapshot message.
package raftstore

import (
	"fmt"
	"io"
	"log/slog"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	"github.com/zattera-dev/zattera/internal/state"
)

// ApplyResult is what FSM.Apply returns through the raft ApplyFuture.
// Err is a *business* error (e.g. KV CAS conflict): the command was accepted
// by Raft and consumed, but the mutation was rejected deterministically.
type ApplyResult struct {
	Err error
	// Duplicate is true when the command's request_id was already applied;
	// the mutation was skipped (idempotency).
	Duplicate bool
}

// FSM implements raft.FSM over a state.Store.
type FSM struct {
	store *state.Store
	log   *slog.Logger
}

// NewFSM wraps a state store. The same store instance must be shared with
// every reader (API server, scheduler, route builder).
func NewFSM(store *state.Store, log *slog.Logger) *FSM {
	if log == nil {
		log = slog.Default()
	}
	return &FSM{store: store, log: log}
}

// Store exposes the underlying state for readers.
func (f *FSM) Store() *state.Store { return f.store }

// Apply decodes and applies one raft log entry. It must be deterministic and
// must never panic on malformed input (a poisoned entry would crash every
// node): unknown/undecodable commands are logged and skipped.
func (f *FSM) Apply(entry *raft.Log) any {
	var cmd clusterv1.Command
	if err := proto.Unmarshal(entry.Data, &cmd); err != nil {
		f.log.Error("fsm: undecodable command, skipping", "index", entry.Index, "err", err)
		return &ApplyResult{Err: fmt.Errorf("raftstore: undecodable command: %w", err)}
	}
	if rid := cmd.GetRequestId(); rid != "" {
		if !f.store.MarkApplied(rid, entry.Index) {
			return &ApplyResult{Duplicate: true}
		}
	}
	if err := f.apply(&cmd); err != nil {
		return &ApplyResult{Err: err}
	}
	return &ApplyResult{}
}

// Snapshot implements raft.FSM.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	// SnapshotProto clones under a read lock; hashicorp/raft guarantees no
	// concurrent Apply during this call, and Persist runs afterwards on the
	// cloned data, so applies may resume immediately.
	return &fsmSnapshot{snap: f.store.SnapshotProto(0)}, nil
}

// Restore implements raft.FSM: replaces all state with the snapshot's.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("raftstore: read snapshot: %w", err)
	}
	var snap clusterv1.Snapshot
	if err := proto.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("raftstore: decode snapshot: %w", err)
	}
	f.store.RestoreProto(&snap)
	return nil
}

type fsmSnapshot struct {
	snap *clusterv1.Snapshot
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	data, err := proto.Marshal(s.snap)
	if err != nil {
		sink.Cancel()
		return err
	}
	if _, err := sink.Write(data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
