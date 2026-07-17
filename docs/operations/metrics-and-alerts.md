---
title: Metrics & alerts
description: Historical metrics and webhook/Slack/email alerting — work in progress.
---

# Metrics & alerts

::: callout warning Work in progress
The historical stats API and CLI (T-60) and the alert engine (T-74) are on the [roadmap](../roadmap/tasks) and not implemented yet. The metrics store and sampler underneath them (T-59) have landed.
:::

**What it will do:** a built-in ring TSDB with per-app and per-node history (`zattera stats --history`), and alert rules firing to webhook/Slack/email channels.

**What works today:** every node runs an embedded ring TSDB (T-59) — a metrics sampler records node CPU/memory/disk/net, per-instance CPU/memory/network, and per-env proxy series every 15s into a two-resolution ring buffer (15s for 24h, 5m for 30d) that survives restarts (`<data-dir>/metrics/tsdb.bin`). [`zattera stats`](../cli/reference) shows live node CPU/memory from agent heartbeats, and [`zattera ps`](../cli/reference) shows per-instance health. See [Logs](logs) for log tailing and retention.

**Under the hood:** the store is `internal/daemon/tsdb` (`tsdb.Store` — per-series float32 rings, downsample-on-write, 48h GC of idle series, flat-file persistence). The sampler is `internal/daemon/agent/metrics.go`, wired into the node agent. Serving this history over the API and CLI is T-60.
