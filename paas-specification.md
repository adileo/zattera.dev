# Zattera — Open Source Single-Binary PaaS
### Specification & Architecture Document

> **Status:** Draft v0.1 · **Name:** Zattera ("raft" in Italian — it runs on Raft) · **Domain:** zattera.dev
> **Language:** Go · **Runtime dependency on hosts:** Docker Engine only
> **License suggestion:** Apache-2.0

---

## 1. Vision

A self-hosted, open-source PaaS with the developer experience of Vercel/Heroku that runs on **any pool of machines** — bare metal, VPS, multi-cloud, home servers — with **no Kubernetes** and **no external dependencies** beyond Docker on each host.

One binary. It can act as a **control-plane node**, a **worker node**, and a **CLI** — roles selected at runtime (and optionally trimmed at build time via build tags).

```
curl -fsSL https://get.zattera.dev | sh          # installs binary + joins pool
zattera deploy --prod                            # deploys the current directory
```

### Design principles

1. **Single static binary.** No Postgres, no Redis, no etcd, no nginx, no certbot on the host. Everything embedded.
2. **Docker is the only host dependency.** The binary talks to the local Docker socket; everything else (state, proxy, builder coordination, ACME, metrics, logs) lives inside the binary.
3. **Declarative, reconciled state.** Users declare desired state (apps, scale, domains); the system continuously reconciles reality toward it. The entire cluster state is exportable/importable as a single snapshot.
4. **Secure by default.** All CLI↔server and node↔node traffic is mutually authenticated and encrypted. No unauthenticated ports, ever.
5. **Minimal but powerful.** Small feature surface, no plugin sprawl; each feature complete and production-grade.

### Non-goals (v1)

- Multi-container pods / sidecars (one container per service instance)
- Service mesh, network policies between apps
- Windows containers
- Being a general-purpose container orchestrator — this is an *app platform*

---

## 2. Mandatory Features (requirements)

| # | Feature | Requirement |
|---|---------|-------------|
| F1 | One-line install & pool join | `curl … \| sh` installs the binary, registers with the pool using a join token. Must work across regions, clouds, and NAT'd home servers. |
| F2 | Beautiful CLI | `zattera deploy --prod`-class UX. Manages apps, DNS, environments (staging/production), env variables, domains/subdomains. |
| F3 | Projects & multi-user | Project scoping, per-project membership and roles. |
| F4 | Builds | Nixpacks and Dockerfile builds (CI/CD in-platform). |
| F5 | Traffic routing | L7 HTTP(S) routing by host/path across the pool; L4 TCP passthrough for databases. |
| F6 | TLS | Automatic Let's Encrypt issuance + renewal (HTTP-01 per hostname in M1–M3; wildcard via DNS-01 in M4). |
| F7 | Autoscale & load balancing | Replica autoscaling on CPU/RAM/RPS; requests load-balanced across replicas on any node. |
| F8 | High availability | Control plane survives node loss (Raft quorum); apps rescheduled off dead nodes. |
| F9 | Push to deploy | GitHub webhook / GitHub App: push → build → deploy. Branch → environment mapping. |
| F10 | API | Everything the CLI does goes through a versioned public API. Mandatory authentication. |
| F11 | Deployments & logs | Deployment history, build logs, runtime logs, `zattera logs -f`. |
| F12 | Red/green deploy | New release fully started & health-checked before traffic switches; instant rollback. |
| F13 | Inspectable state | Cluster state easy to view (`zattera state export`) and restore; human-readable. |
| F14 | Single binary, modular | Server + CLI in one binary; build tags can produce CLI-only or server-only binaries. |
| F15 | Backup/recovery to S3 | Full disaster recovery: state + volumes + images restorable onto fresh infrastructure with one command. |
| F16 | Stateful apps | Docker volumes / data directories for Postgres, Redis, etc. Volumes browsable/accessible from the CLI. |
| F17 | Secure API mediation | Every CLI↔server interaction is API-mediated, authenticated (token + mTLS), encrypted. |
| F18 | Remote attach | CLI can exec into containers, inspect processes, logs, and filesystem remotely. |
| F19 | Sleeping containers | Scale-to-zero on idle; wake on incoming request. |
| F20 | Serverless containers | Request-driven container instances with concurrency-based scaling (superset of F19). |
| F21 | Resource monitoring | Per-app and per-node CPU/RAM/disk/network metrics, historical. |
| F22 | Jobs & cronjobs | One-shot and scheduled containers with logs and retry policy. |
| F23 | Alerts & notifications | Deploy events, health failures, resource thresholds → webhook/Slack/email. |
| F24 | SSO (future) | OIDC-based SSO; local users first. |
| F25 | Single-node & elastic pool | Fully functional on a single node (control+worker+ingress on one machine). Nodes can be added or removed at any time with no platform failure: workloads are drained/rescheduled automatically. Services pinned to a removed node (stateful volumes) stop, by design — everything else keeps running. |
| F26 | Internal service discovery (DNS) | Containers reach each other **across nodes** via an internal DNS name (e.g. `redis.production.myproject.internal`), over the encrypted mesh, without exposing ports publicly. |
| F27 | Node autoprovisioning (future) | The platform can **autonomously buy and join worker nodes** via cloud provider APIs (Hetzner, DigitalOcean, AWS, …) when the pool is saturated, and drain + destroy them after a cooldown when idle. Provider-independent via a driver interface. |

---

## 3. High-Level Architecture

```
                        ┌────────────────────────────────────────────┐
                        │                CLI (zattera)                 │
                        │  deploy · env · domains · logs · attach    │
                        └───────────────┬────────────────────────────┘
                                        │ HTTPS/gRPC (token + TLS)
                 ┌──────────────────────▼──────────────────────────┐
                 │              CONTROL PLANE (3–5 nodes)          │
                 │  API Server │ Raft Store │ Scheduler │ Builder  │
                 │  Cert Mgr   │ DNS Mgr    │ Autoscaler│ Backup   │
                 └───────┬────────────────────────────┬────────────┘
                         │  mTLS over WireGuard mesh  │
        ┌────────────────▼───────┐        ┌───────────▼────────────┐
        │      WORKER NODE       │        │      WORKER NODE       │
        │ Agent │ Proxy │ Docker │  ....  │ Agent │ Proxy │ Docker │
        │ Logs  │ Metrics│Volumes│        │ (any cloud / home lab) │
        └────────────────────────┘        └────────────────────────┘
```

Every node runs the **same binary** (`zatterad`). Roles:

- **`control`** — participates in Raft, runs API/scheduler/builder-coordinator. 1 node for dev, 3–5 for HA.
- **`worker`** — runs workloads. Any number, anywhere.
- A node can be both (small clusters).

**Single-node mode is first-class (F25):** one machine running control+worker+ingress is a complete, fully functional Zattera — same binary, same features (deploys, TLS, logs, cron, backups), zero extra configuration. A single node is simply a cluster of one; growing to N nodes later is only `--join`, never a migration.

### 3.1 Cluster membership & networking (F1, F8)

- **Overlay mesh:** embedded **WireGuard** (userspace `wireguard-go`, kernel WG when available). Every node gets a stable mesh IP (e.g. `10.90.0.0/16`). This is what makes *cross-region, cross-cloud, and NAT'd home servers* first-class: nodes only need outbound UDP to at least one publicly reachable node (control nodes act as rendezvous/relay).
- **Membership & failure detection:** agent heartbeats over the control-plane gRPC stream (M1: 10 s interval, dead after 30 s — judged by the leader); from **M2**, gossip via `hashicorp/memberlist` (SWIM protocol) over the mesh adds quorum-independent, seconds-fast suspicion. (Gossip adds nothing over heartbeats with a single control node, and it depends on the mesh being up — so it ships with multi-control HA.)
- **Join flow:**
  ```
  # on any new machine
  curl -fsSL https://get.zattera.dev | sh -s -- --join zattera.example.com --token <JOIN_TOKEN>
  ```
  The installer: installs Docker if absent → downloads binary → node generates a keypair → presents join token → control plane signs its certificate and WireGuard peer entry → node appears in `zattera nodes ls` with labels (region, provider, arch) auto-detected.
- **Elastic pool (F25):** nodes come and go freely, at any time, with no platform failure.
  - `zattera nodes drain <node>` → replicas migrated away gracefully, then `zattera nodes rm` — zero downtime for stateless services.
  - Abrupt removal (machine dies, is deleted, home server unplugged) → gossip detects it, stateless replicas are rescheduled elsewhere, routes update automatically.
  - **Pinned/stateful services on a removed node stop — by design.** Their state (desired) is preserved; they resume when the node returns, or can be re-pointed to a restored volume snapshot on another node. The platform itself never fails from membership changes (control nodes only require Raft quorum among themselves).

### 3.2 State store (F8, F13)

- Embedded **Raft** (`hashicorp/raft`) replicating a single state machine across control nodes. Log + snapshots stored in **BoltDB** files. No external database.
- The state machine holds the full desired + observed model (Section 4) — small, structured data only (logs/metrics/images live elsewhere).
- **Inspectable:** `zattera state export` emits the entire desired state as human-readable YAML/JSON; `zattera state apply` restores it. This doubles as GitOps-lite: the whole platform config can live in a repo.

### 3.3 Scheduler & reconciler (F7, F8)

- Control loop: compares desired state (releases, replica counts, volumes, jobs) with observed state (agent heartbeats) and issues placement decisions.
- Placement: bin-packing with spread-by-default across nodes/regions; constraints & affinity via node labels (`region=eu`, `disk=ssd`, `home=true`); stateful services pinned to their volume's node.
- On node failure: stateless replicas rescheduled immediately; stateful services follow their volume policy (Section 3.8).
- **Autoscaler:** horizontal scaling per service between `min`/`max` replicas, driven by CPU %, memory %, or requests-per-replica (RPS metric from proxies). Evaluation loop ~15s, scale-down cooldown.

### 3.4 Builds & CI/CD (F4, F9)

- **Build inputs:** `git push` (GitHub webhook/App), `zattera deploy` (uploads a source tarball), or a pre-built image reference.
- **Builders:** any worker labeled `builder=true` runs builds inside BuildKit containers via the local Docker daemon:
  - **Nixpacks** — auto-detect language, generate plan, build (nixpacks binary/logic vendored or run as container).
  - **Dockerfile** — direct BuildKit build.
- **Registry:** embedded, content-addressed image store — a **minimal in-house OCI Distribution-spec registry** (pull/push/manifests/tags subset, ~2k LOC) running in-process on control nodes, backed by local disk and optionally S3, with **ref-counted GC from day one** (driven by release retention). We deliberately do *not* embed `distribution/distribution`: its dependency tree is huge, its config model fights embedding, and its GC is offline mark-and-sweep — the opposite of §9.3. Workers pull over the mesh with node credentials. No Docker Hub dependency.
- **Pipeline:** commit → build → image → release → red/green rollout. Build logs streamed live to CLI/API and retained per deployment.
- **Environments:** map branches to environments (`main → production`, `develop → staging`, optional `PR → preview-*` ephemeral environments with auto-generated subdomains).

### 3.5 Routing & load balancing (F5, F7)

- Every worker runs the **embedded proxy** (Go, in-process — same binary): L7 HTTP/1.1+HTTP/2+WebSocket reverse proxy and L4 TCP proxy.
- Routing table (host/path → service → healthy replica endpoints) is pushed from the control plane to all proxies sub-second after a change (full versioned snapshots streamed over gRPC, debounced ~200 ms).
- **Any node can accept traffic for any app** (routes over the mesh to wherever replicas run), so DNS can point at one, several, or all public nodes. Recommended: a set of `ingress=true` labeled nodes with round-robin/GeoDNS.
- Load balancing: P2C (power-of-two-choices) with health checks, per-replica concurrency awareness (needed for serverless), optional sticky sessions via cookie.
- Middleware (built-in, per-route flags): redirects HTTP→HTTPS, gzip/brotli, basic auth, IP allowlists, request size limits.

**Internal service discovery — cross-node container-to-container traffic (F26):**

- Containers of the same project reach each other by name, regardless of which node runs them:
  ```
  postgres://db.production.myproject.internal:5432
  redis://cache.production.myproject.internal:6379
  ```
- **How:** each agent runs an embedded DNS resolver (in-process, same binary). Containers are attached to a per-node bridge network whose DNS points at the node-local resolver; it answers `*.internal` queries from the routing table replicated from the control plane, and forwards everything else upstream.
- A name resolves to healthy replica endpoints; traffic to a replica on another node transits the **WireGuard mesh** — encrypted by default, no ports ever exposed publicly. TCP/UDP pass through the node-local L4 proxy for load balancing across replicas.
- Scoping = isolation: names are per project+environment (`<service>.<env>.<project>.internal`); a container cannot resolve services of other projects. Staging talks to staging, never to production, with the same service names.
- Works identically single-node and multi-node — apps never need to know the topology.

### 3.6 TLS & DNS (F2, F6)

- **ACME embedded** (Let's Encrypt / ZeroSSL): HTTP-01 solved by the proxies themselves; DNS-01 for wildcards via DNS provider credentials (**M4** — until then app subdomains get per-hostname on-demand HTTP-01 certs; mind Let's Encrypt rate limits, ~50 certs/registered domain/week — use the LE staging endpoint in dev and cap preview environments). Certificates stored encrypted in Raft state, distributed to proxies. Auto-renewal at 30 days remaining.
- **DNS management from the CLI:** provider integrations (Cloudflare, Route53, Hetzner, DigitalOcean, …) through a small provider interface:
  ```
  zattera domains add api.example.com --app api --env production
  # → creates/updates records at the provider, provisions cert, wires route
  ```
- Built-in `*.<cluster-domain>` app URLs (`myapp-staging.apps.example.com`) — zero-config from day one via on-demand per-hostname HTTP-01; a true wildcard cert (DNS-01) replaces this in M4.

### 3.7 Deployment model — red/green (F12)

1. New release `v42` scheduled alongside `v41` (full replica set, capacity permitting; rolling batches when constrained).
2. Health checks pass (HTTP/TCP/exec probe, configurable grace period).
3. Proxies switch traffic atomically to `v42`.
4. `v41` kept warm for `rollback_window` (default 10 min): `zattera rollback` is instant. Then reaped.
5. Failed health checks → automatic abort, traffic never moved, alert fired.

### 3.8 Stateful workloads (F16)

- **Volumes** are first-class objects: created explicitly or by a service definition, bound to a node, mounted into containers as Docker volumes/bind dirs.
- Service `stateful: true` → exactly-one semantics, pinned to volume's node, never blue/green'd with double-run (stop-then-start with configurable maintenance strategy).
- **CLI access to data (mandatory):**
  ```
  zattera volume ls / inspect
  zattera volume browse pg-data          # remote file browser TUI (read-only nav + download; writes go through `volume cp`)
  zattera volume cp pg-data:/backups/x.dump ./     # copy in/out
  zattera attach postgres --env production          # exec into the container
  ```
- **Snapshots:** periodic volume snapshots to S3 (Section 3.11). For databases, optional pre-snapshot hooks (`pg_dump`, `redis BGSAVE`) declared per service.
- v1 explicitly does **not** do synchronous volume replication (that's a distributed-storage problem); HA for data comes from snapshots + app-level replication (e.g. Postgres streaming replication between two pinned services). Documented pattern, not magic.

### 3.9 Sleeping & serverless containers (F19, F20)

- Per-service `scale_to_zero: true` + `idle_timeout` (e.g. 15m).
- The proxy is the **activator**: request for a sleeping service → hold the connection, ask scheduler to start a replica, flush the request when ready (cold start = container start, typically 1–3 s).
- **Serverless mode:** `max_concurrency` per replica; proxies report in-flight counts; autoscaler adds/removes replicas to keep concurrency at target, down to zero. Same mechanism, tighter loop.
- Sleeping never applies to stateful services.

### 3.10 Logs, metrics, alerts (F11, F21, F23)

- **Logs:** agent tails Docker container stdout/stderr → local segmented, compressed log store (embedded, size/time-based retention) → indexed by app/env/deploy. Control plane fans out queries; `zattera logs -f app` streams live across all replicas. Optional shipping to external sinks (Loki/S3) later.
- **Metrics:** agent samples cgroups (CPU, RAM, disk, net) per container + node stats; proxies emit RPS/latency/error-rate. Stored in an embedded time-series ring (e.g. 15s resolution, 30d retention, downsampled). Exposed via API/CLI (`zattera stats`) and a Prometheus-compatible `/metrics` endpoint for users who want Grafana.
- **Alerts:** rule engine on metrics + events (deploy failed, health flapping, node down, disk > 90%, cert renewal failed, backup failed). Notifiers: webhook, Slack, email (SMTP), more later.

### 3.11 Backup & disaster recovery (F15)

- **What:** Raft state snapshot (the full desired state) + registry images (or rebuild instructions) + volume snapshots + secrets (encrypted with a cluster key derived from a user-held recovery passphrase).
- **Where:** any S3-compatible endpoint. Incremental, content-addressed (rsync/restic-style chunking).
- **Recovery contract:**
  ```
  zatterad restore --from s3://backups/zattera --passphrase-file key.txt
  ```
  on a fresh machine recreates the control plane; as workers rejoin (or new ones join), the scheduler re-deploys everything and restores volumes. Target: full platform recovery on brand-new infrastructure with **one command + DNS update**.
- Backup verification job (periodic restore-test of state snapshot) built in.

### 3.12 Jobs & cronjobs (F22)

- `zattera jobs run app -- ./migrate.sh` — one-shot container from the app's image, logs attached.
- Cron: declared in app config (`cron: "0 3 * * *"`), scheduler-managed with jitter, concurrency policy (forbid/replace/allow), retry with backoff, per-run logs and exit status history.

### 3.13 API & security (F10, F17, F24)

- **API:** gRPC internally; public REST+JSON (OpenAPI-generated) on the same port. Versioned (`/v1`). *The CLI is a pure API client* — no hidden channels; anything the CLI does, a script can do.
- **AuthN:** personal access tokens & short-lived session tokens (CLI login via token paste; browser-based login flow in M4); node identity via mTLS certs issued by the embedded cluster CA at join.
- **AuthZ:** RBAC — org → project → environment. Roles: `owner`, `admin`, `developer` (deploy, logs, env vars), `viewer`. Production environments can require elevated role.
- **Secrets:** env vars encrypted at rest (AES-GCM, cluster key in Raft, sealed by recovery passphrase); injected into containers at start; never logged; API returns redacted values unless explicitly requested with permission.
- **Transport:** everything TLS. Node↔node additionally inside WireGuard. Zero plaintext ports.
- **Audit log:** every mutating API call recorded (who/what/when), queryable.
- **SSO (future):** OIDC provider support (Google, GitHub, Okta, Keycloak); local users remain for bootstrap/recovery.

### 3.14 Node autoprovisioning — cluster autoscaler (F27, future)

Horizontal autoscaling of *replicas* (F7) eventually hits the ceiling of available nodes. F27 extends the same reconciliation loop to the **node pool itself**: the cloud-provider equivalent of "the cluster grows when full, shrinks when idle."

- **Trigger (scale-up):** the scheduler has replicas it cannot place (pending > threshold for N minutes) or pool-wide utilization exceeds a target (e.g. CPU/RAM reservations > 85%).
- **Action:** call a **provider driver** to create a machine from a pool template (provider, region, instance type, max count, price cap). Cloud-init runs the standard one-line join (F1) with a short-lived, single-use join token — the new machine appears as a worker with labels `autoprovisioned=true, provider=hetzner, region=fsn1` within minutes.
- **Scale-down:** when utilization stays below a floor for the **cooldown period** (e.g. 20 min), the autoscaler picks the emptiest autoprovisioned node, **drains it gracefully** (same path as `zattera nodes drain`, §3.1), then destroys the machine via the provider API. Nodes joined manually are never touched; stateful/pinned services make a node ineligible for scale-down.
- **Provider-independent by design:** a minimal driver interface — `Create(spec) → machine`, `Destroy(id)`, `List()`, price/quota hints — implemented per provider (Hetzner Cloud first, then DigitalOcean, AWS, Scaleway, …). Drivers are compiled in (no plugins), credentials stored as cluster secrets. The core autoscaler never contains provider-specific logic.
- **Safety rails:** hard `max_nodes` and monthly budget cap per pool, dry-run mode, every provision/destroy action audited and alertable (F23). Failure to provision degrades gracefully — pending replicas simply wait, nothing crashes.

```toml
[node_pools.burst-eu]
provider = "hetzner"
region = "fsn1"
type = "cpx31"
min = 0
max = 10
cooldown = "20m"
labels = { tier = "burst" }
budget_monthly_eur = 150
```

This composes with F19/F20: sleeping apps scale to zero replicas → nodes empty out → the pool itself scales to `min = 0`. True pay-per-use on rented infrastructure, with the bare-metal base pool always on.

### 3.15 Remote attach & debugging (F18)

```
zattera ps --app api                    # replicas, nodes, health, restarts
zattera attach api                      # interactive shell (docker exec) via API tunnel
zattera top api                         # live processes & resource usage
zattera logs -f api --env staging
zattera fs ls api:/app/data             # filesystem inspection without shell
zattera port-forward postgres 5432      # temporary secure tunnel to a service
```
All tunneled through the control-plane API over the mesh — works even when the target node has no public IP (home server case).

---

## 4. Data Model

```
Org ─── User (role) 
 └── Project ─── Member (role)
      └── App
           ├── Environment (production, staging, preview-*)
           │    ├── EnvVars / Secrets
           │    ├── Domains (+ TLS cert refs)
           │    ├── Release (image, config hash) ── Deployment (status, logs ref)
           │    ├── Service spec: image/build, replicas{min,max}, resources,
           │    │     ports, healthcheck, scale_to_zero, max_concurrency,
           │    │     stateful, volumes[], cron[]
           │    └── Volume ── Snapshots
           └── Webhooks (GitHub)
Node { id, roles, labels{region,provider,...}, mesh_ip, capacity, status, autoprovisioned }
NodePool { provider, region, type, min, max, cooldown, budget_cap }   # F27
Cert, DNSProvider, CloudProviderCredential, AlertRule, NotificationChannel, Token, AuditEntry, Backup
```

App config as code — `zattera.toml` in the repo (optional; everything also settable via CLI/API):

```toml
[app]
name = "api"

[build]
type = "nixpacks"            # or "dockerfile"

[deploy]
healthcheck = { path = "/healthz", timeout = "5s" }

[env.production]
replicas = { min = 2, max = 10 }
autoscale = { target_cpu = 70 }
domains = ["api.example.com"]

[env.staging]
replicas = { min = 0, max = 2 }   # scale_to_zero
idle_timeout = "15m"

[[cron]]
schedule = "0 3 * * *"
command = "./cleanup.sh"
```

---

## 5. CLI UX Sketch (F2)

```
zattera login
zattera init                          # detect app, write zattera.toml
zattera deploy                        # → staging (current branch env)
zattera deploy --prod                 # → production, red/green
zattera rollback [--to v41]
zattera env pull / set KEY=V --env production
zattera domains add api.example.com
zattera dns ls / set
zattera logs -f / ps / top / attach / fs / port-forward
zattera projects create / members add dev@x.com --role developer
zattera nodes ls / drain / rm
zattera volume ls / browse / cp / snapshot
zattera jobs run / cron ls
zattera state export > cluster.yaml
zattera backup now / restore
```

Polish requirements: color + spinner output with `--json` on every command, actionable error messages, `--watch` where meaningful, shell completions, and a deploy output that ends with the URL — the Vercel moment:

```
✓ Built api (nixpacks, 34s)
✓ Released v42 → production (red/green, 2 replicas healthy)
● https://api.example.com
```

---

## 6. Binary & Module Layout (F14)

One Go module, one `main`, cobra subcommands. `zattera` = CLI entry; `zatterad` = daemon entry (same binary, symlink or subcommand `zattera server`).

```
/cmd/zattera            main; routes to cli/ or daemon/ 
/internal/cli         all user commands (excluded by -tags server_only)
/internal/daemon      node runtime (excluded by -tags cli_only)
   /api               gRPC + REST, authn/z, audit
   /raftstore         hashicorp/raft + boltdb state machine
   /scheduler         reconciler, placement, autoscaler
   /mesh              wireguard-go, memberlist
   /agent             docker driver, health, log tailer, metrics sampler
   /proxy             L7/L4 proxy, activator, ACME
   /builder           nixpacks + buildkit orchestration
   /registry          embedded distribution
   /volumes           volume mgmt, snapshots
   /backup            S3 chunked backup/restore
   /dns               provider drivers
   /provision         node autoscaler + cloud provider drivers (F27)
   /notify            alert engine, channels
/pkg/apiclient        generated client (used by CLI and third parties)
```

- `go build` (untagged) → full binary, CLI + server (default — plain builds and tooling like gopls just work)
- `go build -tags cli_only` → tiny CLI-only binary (macOS/Windows/Linux); `-tags server_only` → daemon-only
- Tags *exclude* code rather than include it: only the two cobra registration files carry build tags; the linker drops `internal/daemon` from `cli_only` builds because nothing references it.
- CGO disabled, static build; targets: linux/amd64, linux/arm64, darwin, windows (CLI only).

Key libraries (all pure-Go, embeddable): `hashicorp/raft`, `hashicorp/memberlist`, `bbolt`, `wireguard-go`, `docker/docker` client, `moby/buildkit` client, `caddyserver/certmagic` (ACME), `grpc-go` + `grpc-gateway`, `cobra` + `charmbracelet` (CLI polish), `miekg/dns`, `minio-go`. (The registry is an in-house minimal OCI implementation — see §3.4.)

---

## 7. Failure Modes & HA Summary (F8)

| Failure | Behavior |
|---|---|
| Single-node cluster | Fully supported mode: everything runs on one machine; no HA, but no degraded features. |
| Node added | Joins mesh + gossip; schedulable within seconds; scheduler may rebalance opportunistically. |
| Node drained/removed (graceful) | Stateless replicas migrated before removal — zero downtime. Pinned stateful services on it stop (by design), state preserved. |
| Worker dies | Detected via gossip (~seconds); stateless replicas rescheduled; routes updated; internal DNS converges; alert. |
| Control node dies | Raft re-elects (3+ nodes); API stays up on survivors; zero workload impact. |
| Quorum lost | Workloads keep running (proxies/agents autonomous with last-known state); no changes possible until quorum restored or `--force-new-cluster` from snapshot. |
| Whole infra lost | `zatterad restore --from s3://…` on fresh machines (Section 3.11). |
| Network partition (region) | Islands keep serving local routes with last-known config; reconcile on heal. |

---

## 8. Roadmap

**M1 — Core (MVP):** single control node, workers join over mesh, Dockerfile+nixpacks builds, deploy/rollback red-green, embedded proxy + ACME, env vars, logs, CLI (`deploy/logs/env/ps/attach`), GitHub push-to-deploy, projects/users/tokens.

**M2 — Reliability:** 3-node Raft HA, autoscaler, volumes + stateful services, S3 backup/restore, metrics + `zattera stats`, jobs/cron.

**M3 — Platform polish:** scale-to-zero + serverless concurrency scaling, DNS providers, alerts/notifications, preview environments, `state export/apply`, volume browse TUI, audit log.

**M4 — Enterprise-ish:** SSO/OIDC, wildcard DNS-01, GeoDNS guidance, Prometheus endpoint, external log sinks.

**M5 — Elastic infrastructure:** node autoprovisioning (F27) — provider driver interface, Hetzner Cloud driver first, then DigitalOcean/AWS/Scaleway; budget caps, cooldown drain, audited provision/destroy.

---

## 9. Hard Problems (be honest with ourselves)

1. **Scheduler correctness under partitions** — the reconciler must never double-run a stateful service. Fencing via volume-node leases; extensive chaos tests required.
2. **Cross-region traffic** — routing over WireGuard adds latency; keep replicas region-local by default (`spread: region`, route to nearest healthy replica).
3. **Embedded registry GC & distribution** — image storage grows fast; content-addressed store with ref-counted GC from day one.
4. **Cold-start UX for serverless** — honest target is **1–3 s** (container start with the image pre-pulled on candidate nodes). Pause/unpause pre-warming and checkpointing are M4+ experiments, not promises.
5. **Data durability honesty** — no distributed storage in v1; make snapshot RPO explicit in docs so nobody mistakes it for synchronous replication.
6. **Raft + BoltDB ops** — snapshots, compaction, disk-full handling: boring, critical.

## 10. Comparison — Zattera vs the Field

Zattera's edge is not a longer feature list. It is **laser focus on the deploy-and-run path** and a deliberate refusal to carry what makes the alternatives heavy, fragile, or hard to trust. The differentiator is the subtraction.

### 10.1 What we deliberately DON'T do

| We don't… | Who does | Why refusing wins |
|---|---|---|
| **Run Kubernetes** | Kubero, Cozystack, Sealos, Devtron | No etcd tuning, no CNI/CSI/Ingress controller zoo, no YAML sprawl, no cluster upgrades that eat weekends. K8s is a platform *for building platforms*; we ship the platform. |
| **Require an external database / cache** | Coolify (Postgres + Redis), Kubero (K8s API) | The PaaS that deploys databases shouldn't die because *its own* database died. State is embedded Raft — the control plane is self-contained and HA by construction. |
| **Install a web-stack control panel on your servers** | Coolify (~2GB RAM Laravel panel), CapRover, Dokploy | No PHP/Node runtime to patch, no dashboard as attack surface on every host, no panel consuming a worker's RAM. Workers run an agent measured in tens of MB. |
| **Ship 280+ one-click app templates** | Coolify, CapRover | Template catalogs are a maintenance treadmill and a false promise (stale versions, broken configs). Any app is `zattera deploy` away; curated recipes belong in docs, not the binary. |
| **Bundle Traefik/Nginx/certbot as separate moving parts** | Coolify, Dokploy, CapRover, Dokku | Our proxy and ACME live in-process. No config-file generation for a third-party proxy, no version skew between panel and proxy, no "the cert renewed but the proxy didn't reload" class of bugs. |
| **Depend on Docker Swarm's orchestration** | Dokploy, CapRover | Swarm is in maintenance mode, weak across regions/NAT, and has no scale-to-zero or serverless primitives. Our scheduler is small, owned, and built for exactly our feature set. |
| **Treat multi-server as an afterthought** | Coolify (independent servers), Dokku (single server) | Pooling, mesh networking, spread scheduling, and HA are the *core* of Zattera, not an experimental flag. |
| **Require a public IP or same-LAN nodes** | Practically everyone | WireGuard mesh + rendezvous means a home server behind NAT is a worker like any other. |
| **Be a GUI-first product** | Coolify, Dokploy, CapRover, Kubero | CLI + API first (a UI can come later, as a pure API client). GUIs hide state; `zattera state export` shows all of it. Everything is scriptable on day one. |
| **Do service mesh, multi-container pods, operators, plugins/CRDs** | K8s ecosystem | 80% of the complexity, 5% of the use cases. One container per service instance is the honest common case. |
| **Pretend to do distributed storage** | (various, badly) | Synchronous volume replication done wrong loses data. v1 gives pinned volumes + S3 snapshots + documented app-level replication patterns — explicit RPO instead of magic. |
| **Vendor-host your builds or images** | Vercel, Railway, Fly | Builds run on your builders; images live in the embedded registry on your metal. Nothing phones home; the platform works air-gapped. |

### 10.2 Head-to-head

| | **Zattera** | Coolify | Dokploy | CapRover | Dokku | Kamal 2 | Kubero | Cozystack |
|---|---|---|---|---|---|---|---|---|
| Host dependency | **Docker only** | Docker + panel stack | Docker + Swarm | Docker + Swarm | Docker + herokuish | Docker + gem on client | Kubernetes | Bare metal (owns the OS) |
| Install | **1 line, joins pool** | 1 line, per server | 1 line + Swarm join | 1 line + Swarm | 1 line, single server | gem install + config | Helm + K8s expertise | PXE/ISO datacenter bootstrap |
| Control-plane footprint | **1 binary, embedded state** | ~2GB panel + Postgres + Redis | Node panel + Swarm | Node panel + Swarm | none (CLI on server) | none (client-side) | full K8s cluster | full K8s + Talos |
| True multi-server scheduling | **Yes (own scheduler)** | No (independent servers) | Swarm | Swarm (basic) | No | Manual (per-host config) | Yes (K8s) | Yes (K8s) |
| Cross-region / NAT / home servers | **First-class (mesh)** | SSH-reachable only | Swarm struggles cross-region | Same | n/a | SSH-reachable only | K8s networking pain | Datacenter-oriented |
| HA control plane | **Raft, 3 nodes** | No (single panel) | No (single manager UI) | Swarm managers | n/a | n/a (stateless CLI) | K8s HA (heavy) | K8s HA (heavy) |
| Scale-to-zero / serverless | **Built in** | No | No | No | No | No | Partial (via K8s add-ons) | No (IaaS layer) |
| Red/green + instant rollback | **Built in** | Rolling | Rolling | Rolling | Zero-downtime plugin | Yes (kamal-proxy) | K8s rollouts | n/a |
| Nixpacks + Dockerfile | **Yes** | Yes | Yes | Dockerfile+ | Buildpacks | Dockerfile (BYO image) | Buildpacks | n/a |
| One-command full DR from S3 | **Yes (state+volumes+images)** | Partial (panel backup) | Partial | Partial | Manual | Git-recreatable config | Velero (add-on) | Manual |
| Single binary CLI+server | **Yes** | No | No | No | No | CLI only | No | No |
| API-first, everything scriptable | **Yes** | Partial | Partial | Partial | CLI-only | CLI-only | Partial | REST API |

### 10.3 The pitch in one paragraph

Coolify and Dokploy bolt a web panel onto Docker and stop at the edge of real orchestration; Kamal strips everything to a CLI but leaves scheduling, scaling, and state to you; Kubero and Cozystack deliver the full cloud experience by making you operate Kubernetes first. Zattera takes the untaken quadrant: **real multi-server orchestration with zero platform dependencies** — one Go binary that is the CLI, the control plane, the scheduler, the proxy, the cert manager, and the registry, on nothing but Docker. Every feature in Section 2 is on the critical path from `git push` to a healthy URL; everything that isn't, we left out on purpose.

---

## 11. Prior Art to Study

- **Kamal / kamal-proxy** — zero-downtime swap mechanics, minimal host footprint
- **Dokploy / Coolify** — feature checklists, what users ask for
- **Nomad** — single-binary scheduler + Raft + gossip architecture (closest architectural cousin)
- **Fly.io (flyd, corrosion)** — cross-region mesh PaaS patterns
- **Knative Activator** — scale-to-zero request holding
- **rqlite / dqlite** — embedded Raft-replicated state alternatives

---

## 12. Documentation Structure (to be created)

Docs are part of the product: same repo (`/docs`), Markdown, versioned with releases, built as a static site (e.g. Astro Starlight/Docusaurus). Every page ≤ 5 min read; every feature page starts with a working example. CLI reference and API reference are **auto-generated** from cobra commands and the OpenAPI spec — never hand-written.

```
docs/
├── getting-started/
│   ├── what-is-zattera.md              # concetti in 2 minuti, quando (non) usarlo
│   ├── quickstart.md                 # 1 server → app deployata + HTTPS in 5 min
│   ├── install.md                    # requisiti, install script, air-gapped
│   ├── first-cluster.md              # 3 control + N worker, join token, labels
│   └── deploy-your-app.md            # zattera init → deploy --prod, nixpacks/Dockerfile
│
├── guides/                           # task-oriented, uno scenario per pagina
│   ├── github-push-to-deploy.md
│   ├── environments-and-previews.md  # staging/production, branch mapping, preview-*
│   ├── domains-dns-tls.md            # domini, provider DNS, wildcard, ACME
│   ├── env-vars-and-secrets.md
│   ├── scaling-and-autoscale.md
│   ├── scale-to-zero-serverless.md
│   ├── stateful-apps.md              # Postgres/Redis: volumi, pinning, pattern HA
│   ├── jobs-and-cron.md
│   ├── logs-metrics-alerts.md
│   ├── backup-and-disaster-recovery.md   # il "one command restore", test periodici
│   ├── multi-region-and-home-servers.md  # mesh, NAT, rendezvous, spread per region
│   ├── internal-networking.md        # DNS interno *.internal, service-to-service cross-node
│   ├── single-node-to-cluster.md     # partire con 1 nodo, aggiungere/drenare/rimuovere nodi
│   ├── node-autoprovisioning.md      # node pools, driver cloud, budget cap, cooldown (F27)
│   ├── zero-downtime-deploys.md      # red/green, healthcheck, rollback
│   ├── debugging.md                  # attach, top, fs, port-forward
│   └── migrate-from/                 # guide di migrazione (SEO + adozione)
│       ├── heroku.md
│       ├── coolify.md
│       ├── dokploy.md
│       └── kamal.md
│
├── concepts/                         # come funziona, per costruire fiducia
│   ├── architecture.md               # control plane, worker, ruoli
│   ├── state-and-raft.md             # desired state, export/apply, quorum
│   ├── scheduler.md                  # placement, spread, constraints, failure
│   ├── networking-mesh.md            # WireGuard, gossip, routing
│   ├── builds-and-registry.md
│   ├── proxy-and-routing.md          # LB, activator, middleware
│   ├── security-model.md             # mTLS, token, RBAC, secrets, audit
│   └── failure-modes.md              # tabella §7 espansa: cosa succede se…
│
├── reference/                        # esaustivo, generato dove possibile
│   ├── cli/                          # auto-gen da cobra (una pagina per comando)
│   ├── api/                          # auto-gen da OpenAPI + guida auth/paginazione
│   ├── zattera-toml.md                 # schema completo config app
│   ├── server-config.md              # zatterad flags/env, tuning
│   ├── glossary.md
│   └── errors.md                     # codici errore con rimedi (linkati dalla CLI)
│
├── operations/                       # per chi gestisce il cluster
│   ├── production-checklist.md       # hardening, sizing, 3 control nodes, backup
│   ├── upgrades.md                   # upgrade del binario, rolling, compatibilità
│   ├── monitoring-the-platform.md    # /metrics, Grafana, cosa allarmare
│   ├── capacity-planning.md
│   └── troubleshooting.md            # sintomo → diagnosi → fix
│
└── contributing/
    ├── development-setup.md
    ├── architecture-decision-records/    # ADR: perché no-K8s, perché Raft, ecc.
    ├── release-process.md
    └── docs-style-guide.md
```

Regole editoriali: (1) la quickstart deve portare a un URL HTTPS funzionante in <5 minuti reali, testata in CI; (2) ogni promessa fatta nel marketing/README linka la pagina docs che la dimostra; (3) le pagine "concepts" dichiarano esplicitamente i limiti (es. RPO dei volumi) — l'onestà di §9 è parte del brand; (4) esempi copy-paste sempre completi, mai frammenti; (5) changelog + upgrade notes obbligatori a ogni release.
