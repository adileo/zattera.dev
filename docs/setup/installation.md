---
title: Installation
description: Install the Zattera binary on servers and workstations — one command, zero dependencies beyond Docker.
---

# Installation

Zattera ships as **one static Go binary** that contains the CLI, the control plane, the scheduler, the proxy, the cert manager, and the registry. Servers additionally need **Docker Engine** running — that's the entire dependency list.

## One-line install

```bash
curl -sfL https://get.zattera.dev | sudo sh -
```

This installs `/usr/local/bin/zattera` plus a `zt` symlink (the short alias used throughout these docs). The same command **upgrades** in place — it's idempotent, and it stops/restarts a running `zattera.service` around the binary swap, keeping the outgoing binary as `zattera.prev`.

The default install dir needs root. Without `sudo`, install somewhere you own instead:

```bash
curl -sfL https://get.zattera.dev | INSTALL_ZATTERA_BIN_DIR=$HOME/.local/bin sh -
```

### Environment variables

| Variable | Default | Purpose |
| -------- | ------- | ------- |
| `INSTALL_ZATTERA_VERSION` | latest release | Pin a release, e.g. `v0.1.0` |
| `INSTALL_ZATTERA_BIN_DIR` | `/usr/local/bin` | Where to install the binary and `zt` symlink |
| `INSTALL_ZATTERA_BASE_URL` | GitHub Releases | Asset base URL, for private mirrors or testing |

```bash
curl -sfL https://get.zattera.dev | INSTALL_ZATTERA_VERSION=v0.1.0 sudo sh -
```

### Integrity

The installer downloads `sha256sums.txt` alongside the binary and refuses to install if the digest doesn't match — or if the asset isn't listed in the checksum file at all. This is the same verification the in-cluster [`zt cluster upgrade`](../operations/upgrades) performs, against the same artifacts.

## Per-platform binaries

| Platform | Binary | Contains |
| -------- | ------ | -------- |
| Linux amd64 / arm64 | `zattera-linux-{amd64,arm64}` | Full: server + CLI |
| macOS amd64 / arm64 | `zattera-darwin-{amd64,arm64}` | CLI only |
| Windows amd64 | `zattera-windows-amd64.exe` | CLI only |

Servers run Linux. On macOS and Windows you install the CLI to manage clusters remotely; to run a full **dev-mode node** on macOS, build from source (`go build ./cmd/zattera`) — see the [Quickstart](../getting-started/quickstart).

## How it works

There is no dynamic backend behind the installer: GitHub Actions builds and tests each tagged release, GitHub Releases hosts the binaries with `sha256sums.txt`, and GitHub Pages serves the install script at `get.zattera.dev`. "Latest" resolves through GitHub's built-in `releases/latest/download/…` redirect. Every node upgrades with the same `curl | sh` one-liner.

## Next steps

::: grids
::: grid
::: card Quickstart icon:rocket
Bootstrap a control node with `zattera cluster init` and deploy your first app.

[Get started →](../getting-started/quickstart)
:::
:::
::: grid
::: card Nodes icon:server
Mint join tokens, add machines to the cluster, drain and remove them.

[Manage nodes →](nodes)
:::
:::
::: grid
::: card Configuration icon:sliders
The `config.toml` reference: roles, domain, API, registry, mesh, and ACME.

[Configuration →](configuration)
:::
:::
::: grid
::: card Upgrades icon:refresh-cw
Roll the whole cluster to one version, leader last, with checksum-pinned binaries.

[Upgrade guide →](../operations/upgrades)
:::
:::
:::
