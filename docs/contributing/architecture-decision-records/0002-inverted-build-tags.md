# ADR-0002 — Untagged build = full binary; tags exclude rather than include

**Status:** Accepted · **Date:** 2026-07-13

## Context

The spec draft proposed `go build -tags "cli server"` as the default full build. That breaks every tool
that doesn't pass tags: plain `go build ./...`, `go test ./...`, gopls, staticcheck, CI defaults.

## Decision

- **Untagged build = full binary** (CLI + server). This is what developers and tooling get by default.
- `-tags cli_only` produces the small CLI-only binary (darwin/windows/linux); `-tags server_only` the daemon-only one.
- Only two files carry build tags: `internal/commands/register_cli.go` (`//go:build !server_only`) and
  `internal/commands/register_server.go` (`//go:build !cli_only`). They are the *only* importers of
  `internal/cli` and `internal/daemon` respectively, so the linker drops the excluded tree entirely.

## Consequences

- gopls/CI/linters see all code by default; no "works only with tags" drift.
- CI asserts the `cli_only` binary stays small (< 30 MB) and contains no daemon symbols.
- Never import `internal/daemon/...` from CLI code or vice versa — `pkg/apiclient` and generated protos
  are the only shared surface.
