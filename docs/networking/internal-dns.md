---
title: Internal DNS & service discovery
description: Services reach each other at stable .internal names across nodes, with per-environment isolation.
---

# Internal DNS & service discovery

Services talk to each other by name — `db.production.myshop.internal` — wherever their containers actually run. No service registry to query, no IPs in config, and **staging can never accidentally reach production**.

## How to use

From inside any container, address a sibling service as:

```
<app>.<env>.<project>.internal      # e.g. db.production.myshop.internal
<app>.internal                       # shorthand, same thing
```

Both forms address a service **in your own project and environment** — the long
form is not a way to reach another environment. A lookup for a service in a
different project or environment returns NXDOMAIN, which is what makes the
isolation below absolute.

Example: your API's `DATABASE_URL` in production is simply

```bash
zt env set DATABASE_URL=postgres://user:pass@db.production.myshop.internal:5432/app --app api --env production
```

Names resolve to a stable **virtual IP** per environment that load-balances across that service's instances — nothing to change when replicas move between nodes. Ordinary internet DNS keeps working normally.

## How it works

### Per-environment networks

Each (project, environment) pair gets its own container network with a dedicated subnet from `10.201.0.0/16`. Containers are attached only to their environment's network — isolation between environments and projects is enforced at the network layer, not by convention.

### The resolver

Each environment network's gateway runs a tiny authoritative DNS server for `.internal` (containers are pointed at it via Docker's DNS settings):

- `<app>.<env>.<project>.internal` → the service's VIP (A record, 5s TTL).
- Names in **other projects return NXDOMAIN** — even if they exist. The resolver scopes answers to the asking environment's project.
- Anything not ending in `.internal` is forwarded to the host's normal upstream resolvers.

### VIPs and cross-node traffic

Every service/environment gets a cluster-unique VIP from `10.97.0.0/16`. Each node programs the VIPs locally and splices connections to a healthy instance — on the same node directly to the container, or across the [WireGuard mesh](mesh) to the instance's node (P2C balancing, health-gated, same machinery as the [public ingress](ingress)). Internal traffic between nodes is therefore always encrypted.

Current limitations: VIP forwarding is **TCP-only** (UDP internal ports are skipped) and **Linux-only**. Separately, **`--dev` mode does not run the internal service mesh at all** — neither the VIP proxy nor the resolver starts, so `.internal` names do not resolve on a single-node dev cluster on any platform. Use [`zt port-forward`](../operations/remote-debug) locally, and test service-to-service naming on a real cluster.
