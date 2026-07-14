# Minimal single-node example

Boot a one-node Zattera in dev mode, deploy the `go-hello` fixture from the CLI,
and have it served on an `sslip.io` hostname over **HTTP and HTTPS**.

Everything runs on your machine; app URLs resolve to `127.0.0.1` via sslip.io
(no `/etc/hosts` editing needed).

## Prerequisites

- **Docker** running (Docker Desktop is fine — the build and the app run in it).
- Free TCP ports on `127.0.0.1`: **8443** (API), **8080** (ingress HTTP),
  **9443** (ingress HTTPS), **5001** (embedded registry).
  If something already holds `:8080` (a stray dev server), free it first:
  ```bash
  lsof -nP -iTCP:8080 -sTCP:LISTEN     # find the PID, then: kill <PID>
  ```
- Go toolchain (to build the binary).

## 1. Build the binary

```bash
cd /path/to/zattera.dev
go build -o zt ./cmd/zattera
export PATH="$PWD:$PATH"          # so `zt` is on your PATH
```

## 2. Terminal A — start the node

```bash
zt server --dev \
  --data-dir /tmp/zattera-dev \
  --domain apps.127.0.0.1.sslip.io
```

On first boot it prints a startup banner. Copy the ready-to-run **login command**
from it — it looks like:

```
  Log in:  zattera login --server https://127.0.0.1:8443 \
             --ca-cert /tmp/zattera-dev/ca/ca.crt --token zpat_XXXXXXXX
```

(the admin token is shown only once, on first boot).

Leave this terminal running.

## 3. Terminal B — log in and deploy

Paste the login command from the banner, but call the local binary `zt`:

```bash
export PATH="/path/to/zattera.dev:$PATH"

zt login \
  --server https://127.0.0.1:8443 \
  --ca-cert /tmp/zattera-dev/ca/ca.crt \
  --token zpat_XXXXXXXX            # <-- from the banner

zt projects create smoke

# Deploy the fixture app. Its zattera.toml sets name = "hello" and a
# Dockerfile build; --prod targets the "production" environment.
cd test/fixtures/apps/go-hello
zt deploy --prod --project smoke
```

The first deploy is the slow one (cold BuildKit start + image build). You should
see it progress through:

```
  uploaded source; deployment ...
  building
  ✓ Built hello (dockerfile, ~15s)
  starting → health checking → promoting
  ✓ Released v1 → production (red/green, 1 replica(s) healthy)
```

## 4. Hit the app

The app is reachable at **`hello-<env>.<domain>`**, i.e.
`hello-production.apps.127.0.0.1.sslip.io`.

### HTTP (port 8080)

```bash
curl http://hello-production.apps.127.0.0.1.sslip.io:8080/
# -> Hello from Zattera fixture
```

(or open it in a browser). If your resolver blocks sslip.io, force the Host header:

```bash
curl -H 'Host: hello-production.apps.127.0.0.1.sslip.io' http://127.0.0.1:8080/
```

### HTTPS (port 9443, dev CA)

The dev cluster signs certs with its own CA, so point curl at that CA:

```bash
curl --cacert /tmp/zattera-dev/ca/ca.crt \
  https://hello-production.apps.127.0.0.1.sslip.io:9443/
# -> Hello from Zattera fixture
```

In a browser you'll get a self-signed warning unless you import
`/tmp/zattera-dev/ca/ca.crt` into your trust store.

## 5. Inspect and iterate

```bash
zt ps --app hello --project smoke            # running instances + health
zt stats --nodes                             # live node CPU/mem
zt attach hello --project smoke -- /bin/sh   # shell into the container

# change an env var and redeploy (config-hash bump)
zt env set FIXTURE_MESSAGE="hello v2" --app hello --env production --project smoke
zt deploy --prod --project smoke
```

## 6. Tear down

Stop the node with `Ctrl-C` in Terminal A, then clean up the containers,
networks and data it created:

```bash
docker ps -aq   --filter label=dev.zattera/managed=true | xargs -r docker rm -f
docker network ls --filter name=zt- -q                  | xargs -r docker network rm
docker rm -f zt-system-buildkitd 2>/dev/null
docker volume rm zt-buildkit-cache 2>/dev/null
rm -rf /tmp/zattera-dev
```

## Notes

- Dev-mode defaults (all overridable): app domain `apps.127.0.0.1.sslip.io`,
  ingress HTTP `:8080` / HTTPS `:9443`, API `:8443`, registry `:5001`
  (anonymous, plain HTTP). Production uses `:80`/`:443`, `:5000`, ACME TLS.
- In dev the built image is loaded straight into your local Docker (no registry
  pull), so it works on Docker Desktop without configuring insecure registries.
- The exact effective URLs/ports are printed as `DEVBANNER:` lines in the
  startup banner if you need to script against them.
