<div align="center">

# ⛵ Zattera

**The single-binary PaaS. Your servers, the Vercel experience.**

_Zattera — Italian for "raft". It runs on Raft._

[zattera.dev](https://zattera.dev) · [Docs](https://zattera.dev/docs) · [Quickstart](https://zattera.dev/docs/getting-started/quickstart)

![Status](https://img.shields.io/badge/status-pre--alpha-orange)
![License](https://img.shields.io/badge/license-Apache--2.0-blue)
![Go](https://img.shields.io/badge/go-%3E%3D1.24-00ADD8)
![Dependencies](https://img.shields.io/badge/host_dependencies-docker_only-green)

</div>

---

Turn any pool of machines — bare metal, VPS, multi-cloud, the server under your desk — into a Heroku/Vercel-grade platform. **One Go binary** that is the CLI, the control plane, the scheduler, the proxy, the cert manager, and the registry. The only thing your servers need is Docker.

```bash
# on your first server
curl -fsSL https://get.zattera.dev | sh

# on every other machine, anywhere in the world
curl -fsSL https://get.zattera.dev | sh -s -- --join zattera.example.com --token <JOIN_TOKEN>

# on your laptop
zattera deploy --prod
```

```
✓ Built api (nixpacks, 34s)
✓ Released v42 → production (red/green, 2 replicas healthy)
● https://api.example.com
```

## Why Zattera

Every alternative makes you choose: a web panel bolted on Docker with no real orchestration (Coolify, Dokploy), a bare CLI that leaves scheduling and state to you (Kamal), or the full cloud experience _if_ you operate Kubernetes first (Kubero, Cozystack). Zattera takes the untaken quadrant: **real multi-server orchestration with zero platform dependencies.**

- **No Kubernetes.** No etcd, no CNI/CSI/Ingress zoo, no YAML sprawl.
- **No external database.** State lives in embedded Raft — the platform that runs your Postgres doesn't die when _its_ Postgres dies. It doesn't have one.
- **No web-stack panel on your servers.** Workers run an agent measured in tens of MB, not a 2GB dashboard.
- **No bundled nginx/Traefik/certbot.** Proxy and ACME live in-process. No config generation, no version skew, no "cert renewed but proxy didn't reload".
- **No vendor anything.** Builds, images, logs, metrics — all on your metal. Works air-gapped.

## Features

- **Nodes anywhere** — WireGuard mesh + gossip: multi-region, multi-cloud, NAT'd home servers, all first-class. Starts at **one node**; grow with a single `--join`, drain and remove nodes freely.
- **Deploy anything** — Nixpacks auto-detection or Dockerfile, built on your own builders, stored in the embedded registry.
- **Vercel-style flow** — `zattera deploy --prod`, GitHub push-to-deploy, staging/production/preview environments, env vars & secrets, custom domains + automatic Let's Encrypt.
- **Red/green releases** — new version fully healthy before traffic switches; instant rollback.
- **Scale** — replica autoscaling, load balancing across nodes, scale-to-zero with wake-on-request, serverless concurrency mode.
- **Internal DNS** — services talk cross-node via `db.production.myproject.internal` over the encrypted mesh; staging never sees production.
- **Stateful apps** — pinned volumes for Postgres/Redis/…, browsable from the CLI, snapshotted to S3.
- **Disaster recovery** — full platform restore (state + volumes + images) onto fresh infrastructure with one command.
- **Operate it** — logs, metrics, alerts, jobs & cron, `zattera attach/top/fs/port-forward`, audit log, RBAC, API-first (the CLI is a pure API client).
- **Coming later** — node autoprovisioning: Zattera buys Hetzner/DO/AWS machines when the pool is full and destroys them when idle, with budget caps.

## How it works

```
 CLI ──HTTPS──▶ Control plane (1–5 nodes: API · Raft · Scheduler · Builder · ACME)
                      │ mTLS over WireGuard mesh
        ┌─────────────┴─────────────┐
   Worker node                 Worker node
   agent · proxy · docker      agent · proxy · docker
   (Hetzner, eu)               (home server, NAT)
```

Desired state is declared, replicated via Raft, and continuously reconciled. Kill a node: stateless replicas reschedule in seconds. Export the whole platform as YAML with `zattera state export`. Read the full [specification](./paas-specification.md).

## Status

⚠️ **Pre-alpha — spec stage.** The [specification](./paas-specification.md) is complete; implementation is starting with the [M1 milestone](./paas-specification.md#8-roadmap) (single control node, builds, red/green deploys, proxy + ACME, CLI, GitHub integration). Star the repo to follow along, open a Discussion to influence the design.

## Non-goals

Multi-container pods, service mesh, plugins/CRDs, template catalogs, Windows containers, being a general-purpose orchestrator. The subtraction is the product — see [What we deliberately don't do](./paas-specification.md#101-what-we-deliberately-dont-do).

## Contributing

Design discussions happen in Issues/Discussions; architecture decisions are recorded as [ADRs](./docs/contributing/architecture-decision-records/). Dev setup in [CONTRIBUTING.md](./CONTRIBUTING.md).

## License

[Apache-2.0](./LICENSE)
