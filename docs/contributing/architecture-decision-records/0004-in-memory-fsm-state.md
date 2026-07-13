# ADR-0004 — Raft FSM state lives in memory (Nomad model); bbolt only for raft log

**Status:** Accepted · **Date:** 2026-07-13

## Context

§3.2 stores cluster state in a Raft-replicated state machine. Two classic layouts: state resident in a
transactional store (bbolt) mutated per-apply, or a typed in-memory store rebuilt from snapshots+log.

## Decision

- FSM state = `internal/state`: plain Go maps of proto messages keyed by ULID, secondary indexes
  (by project, by node, by domain host, ...), one RWMutex, and a watch hub (prefix-subscription) feeding
  scheduler, route builder and cert manager.
- Raft commands are protobuf: one `Command` envelope with a `oneof` per mutation, `request_id` (ULID)
  for idempotency, `actor` for audit. Log entries are the marshaled envelope. Field numbers are frozen.
- bbolt is used only where hashicorp/raft expects it (`raft-boltdb/v2` log + stable store).
- Snapshots serialize the entire state as one streamed `Snapshot` proto (threshold: 8192 entries / 10 min).
- Reads are served on the leader from the in-memory store; followers transparently forward via a
  gRPC leader-forward interceptor. `raft.Barrier()` only where read-after-write matters.
- Durable observed state (instance/node status transitions) goes through Raft, debounced/batched;
  ephemeral live data (CPU samples, in-flight counts, WG endpoints) lives in leader memory
  (`livestate`) and is rebuilt as agents reconnect after failover.

## Rationale

Cluster state is small (thousands of objects). The in-memory model gives trivial invariant checks,
cheap indexes, easy testing, and no bbolt-transaction plumbing per command — the same trade Nomad makes.

## Consequences

- Restart/restore cost = replay snapshot + tail of log (fast at this scale).
- All state mutations MUST go through FSM apply — never mutate store objects in place outside it.
- Raft log volume is kept low by design (batch observed-state applies, ~1 per node per 2 s max).
