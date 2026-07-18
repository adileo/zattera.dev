---
title: Multiple services & monorepos
description: Run an API, a worker and a database together — one zattera.toml per service, wired by internal DNS.
---

# Multiple services & monorepos

Real applications are rarely one process. This page covers running several
services side by side — a web API, a background worker, and a Postgres database
— including the parts that are not obvious.

## The model: one app per `zattera.toml`

A `zattera.toml` describes **one app**. Its `[env.*]` sections are
**environments** (production, staging), not services. So three services means
three apps, each with its own file:

```
myrepo/
├── api/zattera.toml       [app] name = "api"
├── worker/zattera.toml    [app] name = "worker"
└── db/zattera.toml        [app] name = "db"
```

Put them all in **one project**. That is not just tidiness: internal DNS only
resolves between services in the same project *and* the same environment.

```bash
zt projects create shop
zt context use shop        # or pass --project shop to every command below
```

There is no workspace file and no `deploy --all`: each service is applied and
deployed on its own. Ordering is up to you.

## The database

A database is an off-the-shelf image with a volume — no Dockerfile, nothing to
build. Declare the image and Zattera pulls it directly:

```toml
# db/zattera.toml
[app]
name = "db"

[build]
type  = "image"
image = "postgres:16-alpine"

[deploy.healthcheck]
type = "tcp"
port = 5432

[env.production]
stateful = true

[[env.production.ports]]
name           = "postgres"
container_port = 5432
protocol       = "tcp"

[[env.production.volumes]]
name       = "pgdata"
mount_path = "/var/lib/postgresql/data"
```

Three details matter here:

- **`stateful = true`** pins the service to the node holding its volume and
  switches deploys to [stop-then-start](../data/volumes#volumes-deploys-are-stop-then-start),
  because a volume has exactly one writer.
- **The volume must exist before the first deploy.** It is not auto-created for
  an explicit `zt deploy`.
- **Declare a healthcheck.** With a TCP-only port and no `[deploy.healthcheck]`,
  Zattera has nothing to probe and reports the instance healthy the moment the
  container starts — which is well before Postgres accepts connections. A `tcp`
  check on 5432 fixes that.

Bring it up:

```bash
cd db
zt apply
zt volume create pgdata --app db --env production
zt env set POSTGRES_PASSWORD=<generated> --app db --env production
zt deploy --prod
```

The declared TCP port is **internal only**. There is no TOML key for public L4
exposure, which is the right default for a database — reach it with
[`zt port-forward`](../operations/remote-debug) when you need a psql session.

## The API and the worker

These build from source as usual. The API talks to the database by name:

```toml
# api/zattera.toml
[app]
name = "api"

[env.production]
min_replicas = 2
max_replicas = 6

[env.production.autoscale]
target_cpu_percent = 70
```

```bash
cd api
zt env set DATABASE_URL="postgres://postgres:<generated>@db.production.shop.internal:5432/postgres" \
  --app api --env production
zt deploy --prod
```

The name is `<app>.<env>.<project>.internal`, and the port is the container
port — no remapping. Nothing needs configuring inside the container: its
resolver is pointed at the environment's network gateway automatically. Full
details in [Internal DNS](../networking/internal-dns).

The worker is the same shape, minus the ports.

## Sharp edges

These are behaviours you will hit, so they are worth knowing before you do.

**`zt deploy` uses the current directory.** Unlike `zt apply`, which takes
`-f/--file`, deploy always reads `./zattera.toml` and tars `.` as the build
context. Deploying a monorepo means `cd`-ing into each service:

```bash
(cd db && zt deploy --prod) && (cd api && zt deploy --prod) && (cd worker && zt deploy --prod)
```

**Secrets are per-app.** Environment variables are scoped to one app's
environment; there is no project-wide scope and no references between apps. The
database password is set twice — once on `db` as `POSTGRES_PASSWORD`, once
inside the API's `DATABASE_URL`. Rotating it means updating both and
redeploying both.

**There is no dependency ordering.** Deploying `db` before `api` does not make
the API wait for it, and nothing retries on your behalf. **Your app must retry
its database connection at startup.** This is worth doing regardless — it is
also what makes a database restart survivable.

**Domains in the toml are ignored.** A `domains = [...]` key parses fine and is
then dropped. Attach hostnames with `zt domains add` — see
[Custom domains](custom-domains).

**Internal DNS does not run in `--dev`.** Single-node dev mode skips the
internal service mesh entirely, so `db.production.shop.internal` will not
resolve there. For local multi-service work, use `zt port-forward` and point
services at `127.0.0.1`, or test service-to-service naming on a real cluster.

**The node pulls public images directly.** There is no registry mirroring for
`type = "image"` — every node that runs it needs egress to Docker Hub (or
wherever the ref points). Air-gapped clusters should push the image into the
[built-in registry](builds) first and reference it there.

## Putting it together

```bash
zt projects create shop && zt context use shop

# database first — it has no dependencies
cd db
zt apply
zt volume create pgdata --app db --env production
zt env set POSTGRES_PASSWORD=s3cr3t --app db --env production
zt deploy --prod

# then the services that use it
cd ../api
zt env set DATABASE_URL="postgres://postgres:s3cr3t@db.production.shop.internal:5432/postgres" \
  --app api --env production
zt deploy --prod

cd ../worker
zt env set DATABASE_URL="postgres://postgres:s3cr3t@db.production.shop.internal:5432/postgres" \
  --app worker --env production
zt deploy --prod

zt apps ls
```

From here: [custom domains](custom-domains) to put the API on a real hostname,
[volumes](../data/volumes) for snapshots and backups of the database, and
[GitHub push-to-deploy](github) to deploy each service on push.
