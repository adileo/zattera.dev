---
title: Scale to zero & serverless
description: Idle apps scale to zero replicas and wake on the next request.
---

# Scale to zero & serverless

Idle **scale-down** and **wake-on-request** both work — an idle app cools to 0 replicas and the next request transparently starts it back up — and **serverless concurrency mode** (`max_concurrency`) scales replicas on in-flight request volume.

Turn it on per environment in `zattera.toml`:

```toml
[env.production]
scale_to_zero = true
idle_timeout  = "15m"   # cool down after 15 minutes with no requests
```

**`idle_timeout` is required.** There is no default: `scale_to_zero = true` on its own is silently a no-op — the environment never cools, and nothing warns you. (The exception is `max_concurrency`, which uses a different rule — see [below](#scale-to-zero-serverless-serverless-concurrency-mode).)

## Scaling down

The leader tracks each environment's request activity from the ingress proxies (last request time and in-flight count, carried on node heartbeats). When a `scale_to_zero` environment has seen no traffic for `idle_timeout`, it sets the environment's effective replica count to 0, emits a `scaletozero.cooled` event, and the scheduler stops the instances.

- **Never cools while busy** — any in-flight request, or a request within the window, keeps it warm.
- **Conservative on failover** — a newly elected leader grants every environment a full idle window before cooling it, and never cools during a heartbeat blackout (missing data ≠ idle).
- **Deploys hold it warm** — a running deployment owns the environment; cooling is skipped until it finishes.
- **Stateful is excluded** — `scale_to_zero` cannot be combined with `stateful` (rejected at `zt apply`), since a stateful instance holds a single-writer volume lease.
- A fresh deploy or `zt apply` brings a cooled app back up to `max(min_replicas, 1)` — never to 0, even if `min_replicas` is 0.

The loop evaluates every 15 seconds, so an environment actually cools somewhere between `idle_timeout` and `idle_timeout + 15s`.

## Waking up

When a request arrives for a cooled env, the ingress **holds** it instead of failing: it asks the control plane to wake the env (restoring the replica count to `max(min_replicas, 1)`), waits for the new instance to become healthy, then proxies the held request through — the caller just sees a slower first response (the cold start). Concurrent requests for the same env share one activation and are flushed together once an endpoint appears.

Guardrails:
- **Bounded queue** — at most 100 requests wait per env during a cold start; beyond that the proxy sheds with `503` and `Retry-After: 2`.
- **Deadline** — a held request that sees no endpoint within 60s gets `504`.
- **Body cap** — requests with a body larger than 10 MiB are refused with `503` during cold start, so a slow upload can't tie up a wake slot. The cap reads `Content-Length`, so a **chunked** request (no declared length) is not counted against it.

Waking requires the ingress node to have an activator wired to the control plane. Without one, a request to a cooled environment gets `502 no healthy endpoint` rather than being held.

WebSockets and other upgrades are not special-cased: the request is parked like any other and the upgrade completes after the wake, so the client absorbs the full cold-start latency before the connection is established.

## Serverless concurrency mode

Set `max_concurrency` to scale on **in-flight requests per replica** instead of CPU/memory/RPS — the model for request-bound workloads:

```toml
[env.production]
max_concurrency = 20     # target in-flight requests per replica
scale_to_zero   = true   # optional: cool to zero when idle
min_replicas    = 0
max_replicas    = 10     # required — see below
```

- **Scaling** — the leader targets `ceil(total_in_flight / max_concurrency)` replicas, re-evaluated every 5s (tighter than the resource autoscaler), clamped to `[max(min,1), max]` — or `[0, max]` when `scale_to_zero` is set. A `max_concurrency` env is owned by this loop, not the CPU/RPS autoscaler, and not by the `idle_timeout` loop above.
- **Backpressure** — the ingress skips any replica already at `max_concurrency` when load-balancing. When *all* replicas are at the cap, the request is held (reusing the wake queue) until one frees capacity or a new replica comes up — the same 100-request / 60s / `503`-shed guardrails as cold start.

### Things to get right

**Always set `max_replicas`.** With `min_replicas = 0` and `max_replicas` omitted, `max` defaults to `min` — so both are 0, the clamp pins the target at 0, and the environment can never scale up at all. A wake would be undone within 5 seconds.

**`idle_timeout` does not apply here.** A serverless env with `scale_to_zero` cools whenever in-flight reaches zero — on the next 5s tick, not after an idle window. Traffic with gaps of more than a few seconds will cold-start repeatedly. If you want an idle grace period instead, use `scale_to_zero` + `idle_timeout` **without** `max_concurrency`.

**The concurrency cap is per ingress node.** Each proxy counts only its own in-flight requests, so with N nodes serving traffic a replica can reach roughly `max_concurrency × N` concurrent requests before every proxy considers it full. Size it for one node's share. [Sticky sessions](../networking/ingress) also bypass the cap — a request with a valid affinity cookie reuses its pinned replica regardless of load.

**Backpressure does not force a scale-up without `scale_to_zero`.** When all replicas are capped, the ingress holds the request and asks the control plane to activate the env — but that call only does something for a `scale_to_zero` env. Otherwise relief comes solely from the 5s scaling tick or from capacity freeing up.

## Under the hood

Idle cooling is `internal/daemon/scheduler/scaletozero.go`; concurrency scaling is `internal/daemon/scheduler/serverless.go`; the hold/wake/flush queue is `internal/daemon/proxy/activator.go`, driven from the L7 proxy (`internal/daemon/proxy/l7.go`). Cold-start count and latency are exported as proxy metrics.
