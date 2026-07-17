---
title: Volumes
description: Node-pinned persistent volumes for stateful apps.
---

# Volumes

Zattera gives stateful services (Postgres, Redis, …) a **node-pinned** persistent
volume — honest single-writer semantics, no fake distributed storage. A volume
lives on exactly one node; the service that mounts it is pinned to that node.

::: callout warning Work in progress
Snapshots and `volume browse`/`cp` (T-64/T-65/T-77) are still on the
[roadmap](../roadmap/tasks). Volume lifecycle, pinning, the fencing lease and the
`zattera volume` CLI (T-62) have landed.
:::

## Declare a volume

Mark the service `stateful` and declare a mount in
[`zattera.toml`](../deploy/zattera-toml):

```toml
[env.production]
stateful = true

[[env.production.volumes]]
name = "data"
mount_path = "/var/lib/postgresql/data"
```

The scheduler auto-creates the volume the first time it places the service,
pinning it to the least-used healthy node. From then on the service always runs
on that node. You can also manage volumes explicitly:

```bash
zattera volume create data --app api --env production  # picks a node, or --node <id>
zattera volume ls                                       # ID, NAME, ENV, NODE, STATUS
zattera volume rm <id>                                  # refused while the service runs
```

Deleting a volume removes its record and best-effort deletes the underlying
docker volume on its node (a down node leaves it to be reaped later).

## Single-writer fencing

A stateful service must never run twice against the same volume — that would
corrupt the data. Two mechanisms guarantee it (spec §9.1):

- **Pinning** — a stateful+volume service is only ever placed on the volume's
  node. If that node goes down the volume is marked `NODE_LOST` and the service
  **stops** rather than moving; it resumes when the node returns (the data is on
  that node's disk and cannot follow it).
- **A fencing lease** — the leader grants the volume a 60-second lease naming the
  node and instance allowed to mount it, renewed every ~20s. The agent **refuses
  to start** a container unless the lease names it. During a network partition an
  isolated node's lease can't be renewed, so no other node can acquire one until
  it expires — closing any double-run window.

## Under the hood

`VolumeService` (`internal/daemon/api/volumes.go`) is the CRUD API; the scheduler
owns auto-create, pinning, `NODE_LOST` tracking and lease renewal
(`internal/daemon/scheduler/volumes.go`); the agent enforces the lease before
starting a container (`internal/daemon/agent/executor.go`). Stateful deploys use
stop-then-start (T-63), and snapshots back up to S3 (T-64/T-65).
