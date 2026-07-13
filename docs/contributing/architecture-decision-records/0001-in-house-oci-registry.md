# ADR-0001 — In-house minimal OCI registry instead of embedding distribution/distribution

**Status:** Accepted · **Date:** 2026-07-13

## Context

Spec F4/§3.4 requires an embedded image registry on control nodes. The original draft named
`distribution/distribution` v3 embedded in-process.

## Decision

Implement a minimal OCI Distribution-spec registry in `internal/daemon/registry` (~1.5–2k LOC):

- Content-addressed blob store on disk (`blobs/sha256/<2-char>/<digest>`), atomic tmp+rename writes.
- OCI push protocol: upload sessions (POST/PATCH/PUT, monolithic + chunked), digest verification on finalize.
- Manifest PUT with referenced-blob validation; tag → digest index in a registry-local bbolt file (not Raft).
- Pull: `GET/HEAD /v2/<name>/manifests|blobs/...`; OCI error JSON format; `Docker-Content-Digest` headers.
- **Ref-counted GC** driven by release retention: release deleted → decrement image refs → delete unreferenced blobs.
- Auth: basic auth (node credentials issued at join, user PATs); TLS from the cluster CA.

## Rationale

- `registry/handlers.NewApp` is embeddable in theory, but drags a very large dependency tree and a config
  model designed for a standalone daemon.
- Its GC is offline mark-and-sweep — directly against spec §9.3 ("ref-counted GC from day one").
- Docker and BuildKit clients only exercise a small, well-documented subset of the spec; registries such as
  zot started exactly this way.

## Consequences

- We own ~2k LOC of protocol code with conformance tests (real `docker push`/`pull` in integration tests).
- S3-backed blob storage is a straightforward later additive (same Store interface).
- `mount=` cross-repo blob mounts may be answered with 202 + new upload session (spec-legal fallback).
