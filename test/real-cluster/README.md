# Real 3-node test cluster

End-to-end test of Zattera on the three Hetzner test servers (see
`TEST_SERVER.md`): **mario** (Ubuntu, control+worker), **luigi** (Debian,
worker), **peach** (CentOS, worker). Deploys the `go-hello` fixture from
source and publishes it on **https://app.zatteratest.adileo.org/** with a
Let's Encrypt certificate.

## Usage

```bash
cd test/real-cluster
./setup.sh              # everything: SSH keys → docker → cluster → deploy → verify
./teardown.sh           # back to bare hosts (Docker stays installed)
./setup.sh deploy verify   # re-run individual stages
```

Stages: `keys docker build install control login workers catrust deploy verify`.

First run installs a dedicated SSH key (`.state/id_ed25519`) on the nodes using
the passwords from `servers.env` (via `expect`); every later run is key-based.

## After setup

- App: `https://app.zatteratest.adileo.org/` (Let's Encrypt, issued on first request)
- CLI: `../../zt` — logged in as context `zatteratest`, e.g.

  ```bash
  ../../zt nodes ls
  ../../zt ps --app hello --project demo
  ../../zt logs hello --project demo
  ```

## State

`.state/` (gitignored) holds: the SSH key, per-node arch, cross-compiled
binaries, `bootstrap.env` (admin token + CA fingerprint), `join.token`
(reusable worker token), `cluster-ca.crt`.

Losing `bootstrap.env` while the cluster is alive means the admin token is
gone (it is printed exactly once) — run `./teardown.sh && ./setup.sh`.

## Design notes

- Registry: nodes pull cluster-built images from `10.90.0.1:5000` (control
  mesh IP); `catrust` installs the cluster CA under
  `/etc/docker/certs.d/10.90.0.1:5000/` on every node.
- Join token is **not** single-use: at Phase 5.1 a worker re-runs Join on
  every daemon restart, so the token must stay valid (this also means each
  worker restart registers a duplicate node entry — known pre-M2 limitation).
- Host firewalls (firewalld/ufw) are disabled on the test nodes; needed ports:
  80, 443, 8443, 5000/tcp and 51820/udp.
