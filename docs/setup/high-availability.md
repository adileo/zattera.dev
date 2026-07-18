---
title: High availability
description: Run 3–5 control nodes as a raft quorum so the control plane and the mesh survive node loss.
---

# High availability

Zattera runs the control plane as a **raft quorum** of 3–5 control nodes. The
cluster survives losing a minority of them: the survivors re-elect a leader and
keep accepting writes, the WireGuard mesh reroutes through a surviving hub, and
running workloads never stop. A single-control cluster (the default) works fine
and can run workloads too — HA is what you turn on when control-plane downtime is
unacceptable.

Both halves are exercised against real infrastructure, not only in unit tests:
raft failover with gossip failure detection (`test/cloud/ha_test.go`) and mesh
hub + worker failover (`test/cloud/multihub_test.go`).

## How many control nodes

Raft needs a **majority** to commit writes, so run an **odd** number and size for
the failures you want to tolerate:

| Control nodes | Majority | Tolerates | Notes |
|---|---|---|---|
| 1 | 1 | none | default; control-plane writes pause if it's down (data plane keeps running) |
| 2 | 2 | **none** | strictly worse than 1 — see below |
| 3 | 2 | 1 | the usual HA choice |
| 5 | 3 | 2 | for larger or more failure-prone fleets |

Even counts buy you nothing: 4 tolerates the same single failure as 3, at a
higher write-quorum cost. **Two is actively worse than one** — the majority is
still 2, so either node failing stops writes, and you've doubled the chance of
that happening. Go from 1 straight to 3.

Worker nodes are unlimited and add no quorum cost — add as many as you like.

## Adding a control node

Mint a **control** join token on any control node and bring the new node up with
it — exactly like a worker, but with the control role:

```bash
# On an existing control node: mint a single-use control+worker join token.
zattera nodes join-token create --control
```

Configure the new node with that token and the control role, then start it:

```toml
# /etc/zattera/config.toml on the new node
node_name = "control-2"
data_dir  = "/var/lib/zattera"
roles     = ["control", "worker"]

[join]
addr  = "<existing-control-ip>:8443"
token = "<the-token>"

[api]
advertise_addr = "<this-node-public-ip>:8443"

[mesh]
public_endpoints = ["<this-node-public-ip>:51820"]
```

```bash
zattera server
```

The new node joins the mesh, receives the handed-over cluster CA + data key,
starts its own raft and control stack, and the leader adds it as a voter once its
raft transport is reachable. `zattera nodes ls` shows it as a control node once it
reaches ALIVE. Adding the node triggers a brief raft re-election (the membership
change) during which mutating calls momentarily leader-forward — clients retry
transparently.

::: callout note Cluster secret
The cluster **root CA private key and data key travel to every control node** at
join (this is what lets a joined control node sign certs and auto-unseal within a
live cluster). Treat every control node as holding cluster-wide secret material —
give them the same protection you'd give the bootstrap node.
:::

## Removing a control node

```bash
zt nodes drain control-2      # move its workloads off first
zt nodes rm control-2
```

A control node leaves the **raft quorum first**, then its record is deleted — if
the quorum removal fails you get a retryable error with the node intact, rather
than an orphaned voter that no longer maps to a node.

Two refusals protect you, both overridable with `--force`:

- the node isn't `DRAINED` — drain it first, or force if it's already gone;
- it's the **last** control node — forcing that leaves the cluster with no
  quorum, so only do it while tearing a cluster down.

Shrink one node at a time and wait for `zt nodes ls` to settle in between: going
from 3 to 1 in one step passes through 2, where a single failure stops writes.

## The mesh survives too — hub failover

Every control node is a WireGuard **hub**: it enables IP forwarding and workers
route worker↔worker traffic through it. A worker routes the whole overlay
(`10.90.0.0/16`) through **one active hub** — the ALIVE control node with the
lowest mesh IP, i.e. the bootstrap `10.90.0.1` by default — and keeps warm
standby tunnels to the others.

When the active hub is marked DOWN (gossip catches this in seconds), the control
plane re-pushes every worker a peer set with the overlay route **re-pointed to the
next live hub**, and workers whose control-plane connection was pinned to the dead
node **reconnect to a surviving control node**. Hub failover is decoupled from raft
leadership: the data-plane hub can move independently of who leads the quorum.

## When quorum is lost

Losing the raft majority (e.g. 2 of 3 control nodes down) stops the control plane
from accepting **writes** — deploys, config changes, and scheduling all pause, and
the API refuses mutations rather than risk a split-brain. The **data plane keeps
running** on its own:

- **Ingress proxies keep serving the last routes.** Every proxy caches the most
  recent `RouteSnapshot` to disk and reloads it on start, so traffic to
  already-running instances is unaffected even if no control node is reachable — a
  restarted proxy still routes from its cache.
- **Running containers stay up.** Agents only reconcile against a leader; with
  none, they hold their current assignments rather than tearing anything down.
  Heartbeats buffer until a leader returns.
- **Hub / relay failover.** A worker routing or relaying mesh traffic through a
  control node that dies re-points to a surviving hub within seconds.

When quorum is restored the cluster resumes accepting writes and the scheduler
reconciles any change requested during the outage. These autonomy properties are
verified by the chaos suite (`go test -tags chaos ./test/chaos/ -run TestQuorum`).

## Operational notes

- **Public endpoints.** Give each control node a reachable `[mesh] public_endpoints`
  so workers and the other control nodes can dial it — a control node that can't be
  reached can't serve as a hub or a failover target.
- **Firewall.** Open `51820/udp` (WireGuard) and `8443/tcp` (API). That's it:
  raft (`7480`) and gossip (`7946`) bind the node's **mesh IP**, so they travel
  inside the encrypted tunnel and must *not* be exposed publicly — opening them
  puts the quorum's transport on the internet for no benefit.
- **DNS / traffic.** Point your ingress DNS at all control nodes (or a load balancer
  across them); ingress runs on every control node, and scale-to-zero wakes forward
  to the leader from whichever control node receives the request.
- **Upgrades.** [`zt cluster upgrade`](../operations/upgrades) rolls nodes one at
  a time and takes the raft **leader last**, which is a correctness requirement,
  not a preference. Don't hand-upgrade a leader ahead of its followers.
- **HA is not backup.** Raft replicates state to every control node, including
  your mistakes — a bad `state apply` replicates just as reliably as a good one.
  Keep [backups](../data/backup-restore) running.

## Next steps

::: grids
::: grid
::: card Nodes icon:server
Join tokens, cordon and drain, labels and placement.

[Manage nodes →](nodes)
:::
:::
::: grid
::: card Mesh icon:network
How the WireGuard overlay, hubs, and NAT traversal fit together.

[Networking →](../networking/mesh)
:::
:::
::: grid
::: card Backup & restore icon:life-buoy
Snapshot platform state and volumes; rebuild onto fresh infrastructure.

[Disaster recovery →](../data/backup-restore)
:::
:::
::: grid
::: card Upgrades icon:refresh-cw
Rolling cluster upgrades, leader last, with checksum-pinned binaries.

[Upgrade guide →](../operations/upgrades)
:::
:::
:::
