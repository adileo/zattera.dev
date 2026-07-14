# Binary distribution (get.zattera.dev)

Zero-cost pipeline: GitHub Actions builds, GitHub Releases hosts the binaries,
GitHub Pages serves the installer script behind `get.zattera.dev`. Everything
deploys from `git push` — no Vercel, no paid hosting.

## How it flows

```
git tag v0.1.0 && git push origin v0.1.0
        │
        ▼
.github/workflows/release.yml
  go test ./... → make cross VERSION=v0.1.0 → sha256sums.txt
  → GitHub Release with assets:
      zattera-linux-amd64        (full: server + CLI)
      zattera-linux-arm64        (full: server + CLI)
      zattera-darwin-amd64       (CLI only)
      zattera-darwin-arm64       (CLI only)
      zattera-windows-amd64.exe  (CLI only)
      sha256sums.txt

git push origin main   (touching install/**)
        │
        ▼
.github/workflows/pages.yml
  → GitHub Pages: install.sh served at https://get.zattera.dev/
```

The installer resolves "latest" through GitHub's built-in
`releases/latest/download/…` redirect — no dynamic backend, no version file to
maintain.

## Usage

Install (or upgrade — same command, idempotent; it stops/restarts a running
`zattera.service` around the binary swap):

```bash
curl -sfL https://get.zattera.dev | sh -
```

Pin a version / custom dir:

```bash
curl -sfL https://get.zattera.dev | INSTALL_ZATTERA_VERSION=v0.1.0 sh -
curl -sfL https://get.zattera.dev | INSTALL_ZATTERA_BIN_DIR=$HOME/.local/bin sh -
```

Installs `/usr/local/bin/zattera` plus a `zt` symlink. Linux gets the full
binary, macOS the CLI-only build, Windows users download the `.exe` from the
releases page.

## One-time setup (repo owner)

1. **Visibility** — the repo `github.com/adileo/zattera.dev` must be **public**
   (release assets on private repos are not anonymously downloadable, which
   breaks `curl | sh`).
2. **Enable Pages** — repo → Settings → Pages → Source: **GitHub Actions**.
3. **Custom domain** — in the same Pages settings, set `get.zattera.dev`;
   at the DNS provider add:
   ```
   get.zattera.dev.  CNAME  adileo.github.io.
   ```
   Then tick **Enforce HTTPS** once the certificate provisions (minutes).
4. **First release** — `git tag v0.1.0 && git push origin v0.1.0`.

## Cutting a release

```bash
git tag v0.2.0 && git push origin v0.2.0
```

That's it: the workflow gates on unit tests, builds, checksums, publishes.
Every node then upgrades with the same `curl | sh` one-liner.
