---
title: Deploying
description: How a Zattera deploy works end to end — build, release, health-gated red/green rollout, and instant rollback.
---

# Deploying

Every deploy in Zattera follows the same path, whatever the source:

```
source / image  →  Release vN (immutable)  →  red/green rollout  →  traffic switch  →  old version drains
```

## How to use it

From an app directory containing a `Dockerfile` or anything Nixpacks can detect (see [Builds](builds)):

```bash
zt deploy --prod                 # build from cwd, deploy to production
zt deploy                        # same, to staging
zt deploy --image nginx:alpine   # skip the build, deploy a prebuilt image
```

Watch and manage what happened:

```bash
zt ps                    # instances + health
zt releases              # release history
zt rollback              # back to the previous release
zt rollback --to v41     # or to a specific one
```

Configuration comes from [`zattera.toml`](zattera-toml) (replicas, ports, health checks, resources), and secrets from [environment variables](environment-variables).

## How it works

### Releases are immutable

Each deploy produces a **Release**: an auto-incrementing version that freezes the image reference *and* a full copy of the service spec (replicas, ports, health checks, resources) plus a deterministic **config hash**. The scheduler works only from that frozen contract — changing the environment's config later never mutates what a running release does. This is what makes rollback trivial and exact.

### The red/green state machine

This is the path for **stateless** services — the default. Services marked `stateful` take a different one, [described below](#deploying-how-it-works-stateful-deploys-stop-then-start).

A deploy never touches the running version ("blue") until the new one ("green") has proven itself. The orchestrator drives each Deployment through explicit phases, every transition recorded in replicated state:

1. **Placing** — green instances are scheduled *alongside* blue. Placement filters nodes by liveness, architecture, labels, and capacity, then spreads replicas across nodes and regions. Red/green needs room for both versions at once: if the cluster is short on capacity, the deploy places what it can and **waits**, retrying each tick until room appears — blue keeps serving throughout.
2. **Starting** — agents on the chosen nodes pull the image and start containers.
3. **Health checking** — every green instance must pass its health check (HTTP/TCP/exec, with the grace period from your spec). Any failure aborts the deploy: green is torn down, **blue keeps serving, traffic never moved**. A green that crash-loops (starts, dies, restarts) is detected and fails the deploy within seconds — it doesn't burn the whole health deadline.
4. **Promoting** — one atomic state change flips the routing generation. All ingress proxies switch traffic to green together.
5. **Draining** — blue stays warm for **~10 minutes**, then stops.

Because every phase transition lives in raft-replicated state (not in any process's memory), a control-plane failover mid-deploy resumes exactly where it left off.

### Stateful deploys: stop-then-start

A service with `stateful = true` in [`zattera.toml`](zattera-toml) owns a [volume](../data/volumes), and a volume is pinned to one node with exactly one writer. Red/green would need the old and new instances running at once, against the same data — so stateful services get a different state machine:

```
stopping old  →  starting  →  health checking  →  promoting
```

1. **Stopping old** — the current instance is stopped and the orchestrator *waits for its container to actually be gone*. This barrier is the point: there is never a moment with two instances holding the volume.
2. **Starting** — exactly one new instance starts, on the **same node** as the volume. On a first deploy the volume doesn't exist yet, so it's created on the chosen node and pinned there.
3. **Health checking** — same checks as red/green, same deadline.
4. **Promoting** — the release becomes active. There's **no drain window** — the old instance is already stopped.

::: callout warning Stateful updates take brief downtime
Between "stopping old" and the new instance passing its health check, the service is down. This is inherent to one-writer-per-volume, not a limitation of the rollout. Zattera signals it explicitly: a `deploy.maintenance_start` warning event when the old instance stops, `deploy.maintenance_end` when the new one is healthy. Watch with `zt events -f`.
:::

**If the new instance fails** to start or to become healthy, the deploy tears it down and **restarts the previous instance** (best effort) rather than leaving the service down, then fails the deployment with `deploy.rolled_back`. The window where you're down is the failed attempt, not until you notice.

Two consequences worth planning around:

- **`zt nodes drain` stops stateful services** rather than migrating them — the volume can't move. Use [`cordon`](../setup/nodes) for maintenance on a database node.
- **A dead node doesn't self-heal a stateful service.** Stateless replicas reschedule elsewhere in seconds; a pinned instance waits for its node (and its data) to come back.

### Rollback

```bash
zt rollback [--to vN]
```

Rollback is just a deployment whose target is a previous release — same machinery, same safety. Within the drain window the old instances are still warm, so rollback skips placement and health checking entirely and re-promotes in seconds. After the window, or for a stateful service, it runs as a normal deploy of that release.

Because a release freezes the service spec *and* an env-var fingerprint, rolling back restores the configuration that shipped with that version — not today's config running yesterday's image.

### Superseding

Deploying again while a deploy is in flight marks the older one **superseded** and reaps its green instances. You never end up with two half-finished rollouts fighting.

### Failure recovery

If a node dies, the scheduler notices missing replicas (heartbeat-based liveness) and places replacements on healthy nodes — stateless replicas reschedule in seconds without operator action. Stateful instances are the exception: their volume lives on the dead node, so they wait for it rather than starting elsewhere on empty storage.

Deployment state lives in raft, so this survives a control-plane failover too: a deploy interrupted mid-rollout resumes from its recorded phase rather than restarting or stalling.

## Next steps

::: grids
::: grid
::: card zattera.toml icon:file-text
Replicas, ports, health checks, resources, volumes, and placement.

[Configuration reference →](zattera-toml)
:::
:::
::: grid
::: card Builds icon:package
Nixpacks and Dockerfile builds, BuildKit, the embedded registry, multi-arch.

[How builds work →](builds)
:::
:::
::: grid
::: card Volumes icon:hard-drive
Node-pinned storage for stateful services, snapshots, and restores.

[Volumes →](../data/volumes)
:::
:::
::: grid
::: card Custom domains icon:globe
Attach hostnames with automatic Let's Encrypt certificates.

[Custom domains →](custom-domains)
:::
:::
:::
