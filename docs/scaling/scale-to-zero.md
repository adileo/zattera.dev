---
title: Scale to zero & serverless
description: Idle apps scale to zero replicas and wake on request — work in progress.
---

# Scale to zero & serverless

::: callout warning Partly implemented
Idle **scale-down** and **wake-on-request** both work: an idle app cools to 0 replicas and the next request transparently starts it back up. Serverless concurrency mode (`max_concurrency`) is still on the roadmap (T-71).
:::

Turn it on per environment in `zattera.toml`:

```toml
[env.production]
scale_to_zero = true
idle_timeout  = "15m"   # cool down after 15 minutes with no requests
```

## Scaling down (available now)

The leader tracks each environment's request activity from the ingress proxies (last request time and in-flight count, carried on node heartbeats). When a `scale_to_zero` environment has seen no traffic for `idle_timeout`, it sets the environment's effective replica count to 0 and the scheduler stops the instances.

- **Never cools while busy** — any in-flight request, or a request within the window, keeps it warm.
- **Conservative on failover** — a newly elected leader grants every environment a full idle window before cooling it, and never cools during a heartbeat blackout (missing data ≠ idle).
- **Stateful is excluded** — `scale_to_zero` cannot be combined with `stateful` (rejected at `zt apply`), since a stateful instance holds a single-writer volume lease.
- A fresh deploy or `zt apply` brings the app back up to `replicas.min`.

## Waking up

When a request arrives for a cooled env, the ingress **holds** it instead of failing: it asks the control plane to wake the env (which restores the replica count to `replicas.min`), waits for the new instance to become healthy, then proxies the held request through — the caller just sees a slower first response (the cold start). Concurrent requests for the same env share one activation and are flushed together once an endpoint appears.

Guardrails:
- **Bounded queue** — at most 100 requests wait per env during a cold start; beyond that the proxy sheds with `503 Retry-After: 2`.
- **Deadline** — a held request that sees no endpoint within 60s gets `504`.
- **Body cap** — requests with a body larger than 10 MiB are refused during cold start so a slow upload can't tie up a wake slot.
