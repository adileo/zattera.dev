---
title: CLI overview
description: How the zattera CLI works — login, contexts, project resolution, JSON output, and conventions shared by every command.
---

# CLI overview

The `zattera` CLI (installed with a `zt` shorthand symlink — the two are interchangeable) is a **pure API client**: everything it does goes through the same public gRPC/REST API you can script against directly. No SSH, no local state beyond a config file with contexts.

```bash
zt login --server https://cp1.example.com:8443 --ca-pin <FINGERPRINT> --token zpat_…
zt projects create demo
zt deploy --prod
```

See the [command reference](reference) for every command and flag.

## Two kinds of commands

The same binary carries both, which is why one download works everywhere — but they're used from different places:

- **Client commands** (everything above, and most of the reference) talk to a cluster over the API. They run from your laptop and need a login.
- **Host commands** — `cluster init`, `cluster join`, `cluster teardown`, `server`, and `restore` — configure the machine they run on. They need root on a Linux server, not a login, and they're the only ones the macOS/Windows builds omit.

`cluster upgrade` looks like the second kind but is the first: it drives the rollout through the API, so it runs from your workstation.

## Logging in

```bash
zt login --server https://<host>:8443 --token zpat_… [--context prod]
```

Three ways to trust the cluster's TLS certificate:

- **`--ca-pin <sha256>`** (recommended) — pass the CA fingerprint printed at cluster boot; the CLI fetches the CA over TLS, verifies it matches the pin, and stores it (trust-on-first-use, no file copying).
- **`--ca-cert <path>`** — point at a copy of `ca.crt` (dev clusters).
- **Nothing** — when the API serves a public Let's Encrypt certificate, system roots just work.

`login` verifies the token with a `WhoAmI` call **before** saving anything — a bad login never disturbs your existing config.

## Contexts

Each login is stored as a named **context** (server + token + CA) in `~/.config/zattera/config.toml`, so you can manage several clusters:

```bash
zt context               # list contexts, * marks the active one
zt context use prod      # switch
```

## Shared conventions

- **`--project`** — most commands are project-scoped. Precedence: `--project` flag → the context's default project → error asking you to pick one.
- **`--app`** — defaults to the `name` in `./zattera.toml` when you're inside an app directory, so `zt deploy`, `zt logs -f`, `zt ps` work with no arguments.
- **`--env` / `--prod`** — deploy-family commands default to `staging`; `--prod` is shorthand for `--env production`. (Exception: `zt env …` and `zt jobs run` default to `production`.)
- **`--json`** — every command supports machine-readable output for scripting. In JSON mode the decorated `✓`/`●` lines are suppressed and stdout carries exactly one JSON document; progress and informational lines go to stderr, so `zt … --json | jq` is always safe.
- **Exit codes** — non-zero on failure; `attach`, `fs`, and `jobs run` propagate the *remote* command's exit code, so they compose in shell scripts.
- **Errors** — shown as plain messages (`project demo not found`), no gRPC noise.

## Auditing what happened

Two cluster-wide logs, both queryable from the CLI and both project-scoped unless you're an org admin:

```bash
zt events --since 1h              # deploys, node health, certificates
zt audit --method Deploy          # who called what, with the outcome
```

Both are capped rings in cluster state, so old entries age out; `--archive` also reads what was swept to object storage.
