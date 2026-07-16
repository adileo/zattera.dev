---
title: High availability
description: Multi-control-node HA with raft quorum — work in progress.
---

# High availability

::: callout warning Work in progress
The raft HA core (T-55) and the daemon join-as-control bring-up (T-55b) have landed: the control plane replicates over an mTLS raft transport, a node joined with a `--control` token installs the handed-over cluster CA + data key, joins the raft quorum, and runs its own control stack, and leader failover keeps writes flowing. What remains before this is production-ready is real multi-host verification and multi-hub mesh — a joined control node is currently a mesh *spoke* (its raft + API work over the mesh, but workers don't yet route through it as a hub), and hub/ingress bring-up on a later leadership change isn't wired. This page will be completed when that ships.
:::

**What it will do:** run 3–5 control nodes as a raft quorum, so the control plane survives node loss. Joining a node with a `--control` join token will add it to the quorum; memberlist gossip will speed up failure detection between nodes. Today a cluster has exactly one control node (which can also run workloads), and worker nodes can already be added freely — see [Nodes](nodes).
