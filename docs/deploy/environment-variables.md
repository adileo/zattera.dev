---
title: Environment variables
description: Set per-environment variables and secrets — encrypted at rest, injected at start, never stored in plaintext.
---

# Environment variables

Every environment (production, staging, …) has its own set of variables. They are treated as secrets by default: encrypted at rest, redacted in output unless you explicitly reveal them.

## How to use

```bash
zt env set DATABASE_URL=postgres://… STRIPE_KEY=sk_live_… --app api --env production
zt env pull --app api --env production              # keys only, values redacted
zt env pull --reveal --app api --env production     # KEY=value lines (developer+ role)
zt env unset STRIPE_KEY --app api --env production
```

- `zt env …` defaults to `--env production` (unlike `deploy`, which defaults to staging).
- **`--app` is required.** Unlike `deploy`, `logs`, `ps` and the rest, the `env` commands do *not* read the app name from `./zattera.toml` — omitting it fails with `--app is required`.
- `env pull` prints sorted `KEY=value` lines. Without `--reveal` the values are **empty**, not hidden-but-present, so redirecting a plain `env pull` into a `.env` file gives you keys with blank values — use `--reveal` for that.
- Listing keys at all needs the **developer** role; a viewer cannot see even the names.

### Loading a local `.env` file

There is no `--from-file` flag yet ([T-103](../roadmap/tasks)), so uploading a `.env` means feeding its lines to `zt env set` as arguments. Split on **newlines**, never on whitespace:

```bash
# bash / zsh — one argument per line, values kept verbatim
IFS=$'\n' read -r -d '' -a VARS < <(grep -vE '^\s*(#|$)' .env && printf '\0')
zt env set "${VARS[@]}" --app api --env production
```

The obvious one-liner is the wrong one:

```bash
zt env set $(cat .env | xargs) --app api --env production   # ✗ don't
```

`xargs` splits on spaces, so `GREETING="hello world"` becomes two arguments and the command fails with `invalid KEY=VALUE: "world"` — or worse, succeeds having mangled something.

::: callout warning The shell is not a `.env` parser
Even the newline-safe version above passes lines through **literally**, which is not what dotenv tools do:

| In your `.env` | What Zattera stores | What you probably meant |
| -------------- | ------------------- | ----------------------- |
| `QUOTED="hello world"` | `"hello world"` — quotes included | `hello world` |
| `export FOO=bar` | key `export FOO` — with a space | key `FOO` |
| `FOO=bar # trailing note` | `bar # trailing note` | depends on your parser |

Multi-line values (PEM keys, certificates) can't come from this at all — the API stores them fine, but a shell line-splitter can't reassemble them, and `zt env pull` prints them across multiple lines, so its output no longer round-trips.

**So:** use the loop for simple, unquoted files, and set anything quoted, prefixed, or multi-line explicitly:

```bash
zt env set "TLS_KEY=$(cat server.key)" --app api --env production
```
:::

### Changes apply on the next deploy

Setting a variable does **not** hot-restart running instances. The change is folded into the next release's config hash and takes effect on the next `zt deploy` (or rollback):

```bash
zt env set FEATURE_FLAG=on --app api --env production
zt deploy --prod --app api        # instances restart with the new value
```

This is deliberate: a running release is immutable, so what's live is always exactly what `zt releases` says was deployed.

### Variables Zattera injects

At container start, alongside your variables:

| Variable | Value | If you set it yourself |
| -------- | ----- | ---------------------- |
| `PORT` | The first container port | **Your value wins** |
| `ZATTERA_ENV` | Environment name (`production`, `staging`, …) | **Silently overridden** |
| `ZATTERA_APP` | App name | **Silently overridden** |

`ZATTERA_ENV` and `ZATTERA_APP` are the platform's identity for the instance, so they're applied *after* your variables and always win. `zt env set ZATTERA_ENV=…` is accepted without complaint and then ignored at container start — if you need your own value, pick a different name.

These apply anywhere the environment's release runs: services, [jobs and cron runs](../operations/jobs), and [preview environments](preview-environments) (whose variables are cloned from `staging` when the preview is created).

## How it works

Values are protected with **envelope encryption**:

1. At bootstrap the cluster generates a random 32-byte **data key**, sealed with a key derived (argon2id) from the recovery passphrase printed once at first boot. Only the sealed form is stored in replicated state.
2. Each variable value is encrypted with **AES-GCM** under the data key *before* it enters the raft log — plaintext secrets never persist anywhere on disk.
3. The plaintext data key lives only in control-node memory. When an agent needs to start a container, the control plane decrypts the variables at that moment and sends them over the **mTLS agent stream** — they exist in plaintext only inside that frame and in the container's process environment.

Reading variables at all (`GetEnvVars`, with or without `--reveal`) requires the **developer** role, and setting them requires an unsealed cluster key — otherwise `zt env set` fails with `cluster key is not unsealed; cannot store secrets` rather than storing anything in the clear.

The release config hash covers a fingerprint of the environment's sealed variables (an FNV-1a hash over the sorted key/ciphertext pairs), which is how the platform knows a redeploy is needed to pick up changes.

### Where else the ciphertext travels

Variables are part of replicated cluster state, so the sealed form goes wherever state goes:

- **`zt state export`** includes an `env_vars:` map of base64-encoded ciphertext per environment. It is not plaintext, but it is your secrets — treat an export as sensitive, and note that applying it to a different cluster leaves values undecryptable, since the data key differs.
- **Backups** carry the same sealed values, and the [restore](../data/backup-restore) path needs the recovery passphrase to unseal them.
- **Every control node** holds the plaintext data key in memory while running, which is one more reason to give control nodes the protection described in [high availability](../setup/high-availability).
