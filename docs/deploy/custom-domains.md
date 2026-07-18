---
title: Custom domains
description: Attach your own hostnames to any environment, with automatic Let's Encrypt certificates.
---

# Custom domains

Every environment is reachable out of the box at its **cluster subdomain** — `<app>-<env>.<cluster-domain>` (e.g. `api-production.apps.example.com`), with TLS included. Custom domains put your own hostname in front of the same environment.

## Every address a service can have

| Address | Reachable from | Set up by |
| ------- | -------------- | --------- |
| `https://<app>-<env>.<cluster-domain>` | the internet | automatic — every environment gets one |
| `https://<your-hostname>` | the internet | `zt domains add` or `domains` in [`zattera.toml`](zattera-toml) |
| `https://<your-hostname><path-prefix>` | the internet | `zt domains add --path /admin` |
| `https://<app>-preview-<pr>.<cluster-domain>` | the internet | automatic for [preview environments](preview-environments) |
| `<host>:<public-l4-port>` (raw TCP) | the internet | `public_l4_port` on a port — **API-only**, see below |
| `<app>.<env>.<project>.internal` | inside the mesh | automatic — [internal DNS](../networking/internal-dns) |
| `http://127.0.0.1:<local>` | your laptop | `zt port-forward` ([remote debugging](../operations/remote-debug)) — needs the mesh, so it does not work in `--dev` ([T-106](../roadmap/tasks)) |
| `http://<app>-<env>.apps.127.0.0.1.sslip.io:8080` (and `:9443` HTTPS) | your machine | `zattera server --dev` — [dev mode ports](../setup/configuration#configuration-dev-mode-defaults) |

The first four are HTTP(S) through the ingress on `:80`/`:443`. The internal FQDN is what services should use to call each other — it stays on the encrypted mesh and never leaves the cluster.

::: callout note Raw TCP ports are API-only today
A port with `public_l4_port` set is exposed by the ingress as **raw L4 TCP passthrough** (for Postgres, Redis, SMTP — anything that isn't HTTP). There is no `zattera.toml` key and no CLI flag for it yet: the field exists on the `PortSpec` proto, so setting it means calling `ApplyAppConfig` directly.
:::

## How to use

Point DNS at your cluster first — an `A` record (or `CNAME`) from your hostname to any ingress node's public IP. Traffic can enter through **any** node; it routes over the mesh to wherever the app runs.

```bash
zt domains add api.mycompany.com --app api --prod
# Added api.mycompany.com → api (production)
# certificate: pending

zt domains ls        # HOSTNAME + certificate status (pending / issued / failed)
zt domains rm api.mycompany.com
```

Options:

- `--env NAME | --prod` — target environment (default: staging).
- `--path /admin` — only route requests whose path starts with this prefix. The prefix is **passed through unchanged**, so the app still sees `/admin/...` and must serve it.
- `--port NAME` — target a specific service port (default: the first HTTP port).

You can also declare domains per environment in [`zattera.toml`](zattera-toml) (`domains = ["api.mycompany.com"]`).

::: callout warning One hostname, one environment
A hostname can be attached **once**, cluster-wide: adding `shop.example.com` a second time fails with `hostname "shop.example.com" is already in use`, even with a different `--path`. So `--path` narrows which requests reach *that* environment — it cannot currently split one hostname across several apps (e.g. `/` → web, `/api` → api). The proxy already resolves longest-prefix per hostname; it's the uniqueness check that stands in the way, tracked as [T-104](../roadmap/tasks).

A request that matches the hostname but not the prefix has no route and gets a **404**.
:::

The certificate is issued automatically on the first HTTPS request once DNS resolves to the cluster — usually within seconds. `zt domains ls` shows the status.

## How it works

### Routing

The control plane builds a routing table from desired state: each hostname (+ optional path prefix) maps to the healthy instances of its environment. Every ingress node streams this table and serves `:80`/`:443` — hostnames match exactly, then the longest path prefix wins. Requests balance across instances (P2C — power of two choices, preferring node-local instances) and only ever reach **healthy** ones.

Custom hostnames may not collide with the reserved `<app>-<env>.<cluster-domain>` namespace — the API rejects those.

### Per-route middleware

Each domain carries a middleware set the ingress applies before proxying:

| Middleware | Effect |
| ---------- | ------ |
| `redirect_https` | 308 plaintext requests to HTTPS (on by default) |
| `basic_auth` | HTTP basic auth against a stored bcrypt/argon2id hash — useful for staging |
| `ip_allowlist` | Reject requests from outside the listed CIDRs |
| `max_body_bytes` | Reject request bodies over this size |
| `compress` | gzip/brotli responses when the client accepts it |
| `sticky_sessions` | Cookie-based affinity to one instance |

These are enforced by the proxy today, but like raw L4 ports they are **API-only**: `SetMiddleware` exists on `DomainService` (developer role), and `zt domains` has no flag for any of them. Configuring middleware currently means calling the API directly — tracked as [T-105](../roadmap/tasks).

### TLS certificates

Zattera embeds an ACME client (Let's Encrypt) — no certbot, no nginx reload dance:

- **On-demand issuance**: the first TLS handshake for a hostname triggers issuance, but *only* for hostnames present in the routing table. Random strangers pointing DNS at your cluster can't mint certificates.
- **HTTP-01** challenges are answered on the `:80` listener (which otherwise 308-redirects to HTTPS). The hostname must publicly resolve to a cluster node for issuance to succeed.
- **Certificates live in replicated cluster state**, not on any single node's disk — every ingress node can serve every certificate, issuance is serialized cluster-wide, and renewal is automatic.
- The Let's Encrypt **staging** endpoint can be selected in the [server config](../setup/configuration) (`[acme] staging = true`) while testing, to avoid rate limits.

Wildcard certificates (DNS-01) are on the [roadmap](../roadmap/tasks) (T-72/T-73, M4).
