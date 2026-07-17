---
title: Backup & disaster recovery
description: Incremental S3 snapshots and one-command full-platform restore — work in progress.
---

# Backup & disaster recovery

Both volume snapshots (T-64/T-65) and full-platform backup + `zatterad restore`
(T-66) have landed.

## Volume snapshots

Configure a destination bucket once (cluster-wide `BackupConfig`: S3 endpoint,
bucket, and credentials, which are encrypted at rest), then snapshot on demand or
on a schedule:

```bash
zattera volume snapshot <id>            # take one now, waits for completion
zattera volume snapshots <id>           # id, status, size, created
zattera volume restore <id> --snapshot <snap-id>   # service must be stopped first
```

Scheduled snapshots and retention come from the volume's `SnapshotPolicy`
(settable when creating the volume):

- **`schedule`** — a 5-field cron expression; the leader fires a snapshot each
  due slot. An optional **`pre_hook`** command runs inside the mounting container
  first (e.g. `pg_dump` to quiesce the database).
- **`keep_last`** (default 7) — older snapshots beyond this count are deleted and
  their now-orphaned chunks garbage-collected.

A snapshot runs on the volume's pinned node: the control plane dials that node,
which streams progress back. Restore refuses while the volume is mounted — stop
the service (scale its environment to 0) first.

## The snapshot engine (T-64)

Volume snapshots are **content-addressed and deduplicated**, so an incremental
snapshot only uploads what changed:

1. The volume's directory is serialized to a **deterministic tar** (sorted walk,
   zeroed access/change times, preserved uid/gid/mode) — byte-identical trees
   produce byte-identical tars.
2. The tar stream is split into ~1 MB **content-defined chunks** (FastCDC), so a
   small edit re-chunks only locally (a one-byte change touches one or two
   chunks, not the whole file).
3. Each chunk is keyed by `sha256(plaintext)` — if that object already exists it
   is **skipped** (dedup across all snapshots) — otherwise compressed (zstd),
   **encrypted** (AES-256-GCM with the cluster data key and a random per-object
   nonce), and stored as `chunks/<hash>`.
4. A per-snapshot **manifest** lists the ordered chunk hashes (encrypted too).
   Restore streams the chunks back through a tar extract; a prune pass refcounts
   every manifest and deletes only orphaned chunks (shared chunks survive).

The engine (`internal/daemon/volumes`) operates on an already-quiesced path;
quiescing a live database with a pre-hook is the scheduling layer's job (T-65).

## Disaster recovery (T-66)

A full backup captures the whole control plane to the same S3 bucket:

- the **raft state** (all projects, apps, environments, volumes, …), encrypted
  with the cluster data key;
- the **cluster CA** cert + key (encrypted) — so restored nodes' certificates
  stay valid;
- the **data key itself**, sealed under a recovery **passphrase** — the only way
  back in;
- an **index** referencing each volume's latest snapshot.

To rebuild onto fresh infrastructure:

```bash
zatterad restore --from s3://my-bucket/zattera \
  --passphrase-file /secure/passphrase \
  --data-dir /var/lib/zattera         # must be empty
zatterad server --data-dir /var/lib/zattera
```

Restore unseals the data key with the passphrase, decrypts the state and CA into
the fresh data dir, and bootstraps a new single-node raft holding the restored
state (old node records are kept but marked **DOWN**, their mesh IPs preserved so
rejoining nodes reclaim the same addresses). As workers rejoin they reclaim their
volumes and restore the referenced snapshots. **RPO** is the age of the latest
backup.

For a lighter-weight, GitOps-style export of just the desired state (no volumes),
[`zattera state export`](../operations/state-export) remains available.
