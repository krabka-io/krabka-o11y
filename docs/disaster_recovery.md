# Disaster recovery

`krabka-storage-admin` creates checksummed, tenant-scoped object-store backup
sets. It reads and writes through the same AWS S3, Google Cloud Storage, Azure,
and local object-store implementations as the services.

## Consistent cut

Stop writers and compaction for the tenant, record the next consumer offset for
every WAL partition, and take the broker snapshot before copying object state.
The object prefix must include blocks, indexes and manifests, rule state,
delete requests, profile symbols, and any signal-specific sidecars. A backup
without the matching broker snapshot is not a restorable cut.

Create the object half under a new, dedicated prefix:

```bash
krabka-storage-admin backup \
  --source-url s3://live/tenant-a \
  --backup-url s3://backups/cut-2027-02-01/tenant-a \
  --tenant tenant-a \
  --cut-id cut-2027-02-01 \
  --wal-offset krabka.metrics.wal:0:120 \
  --wal-offset krabka.logs.wal:0:84 \
  --wal-offset krabka.traces.wal:0:73 \
  --wal-offset krabka.profiles.wal:0:55 \
  --apply --report backup-report.json
```

The command hashes every source object, copies with create-if-absent semantics,
and writes `.krabka-recovery/manifest.json` last. Interrupted copies are not
complete backups. Repeating the same command resumes matching objects and
refuses conflicting or unrelated content.

## Audit and restore

Audit is read-only and exits unsuccessfully for missing, corrupt, or orphaned
objects:

```bash
krabka-storage-admin audit \
  --backup-url s3://backups/cut-2027-02-01/tenant-a \
  --target-url s3://backups/cut-2027-02-01/tenant-a \
  --report audit.json
```

Restore the broker snapshot first, then restore object state into a dedicated
empty tenant prefix:

```bash
krabka-storage-admin restore \
  --backup-url s3://backups/cut-2027-02-01/tenant-a \
  --target-url s3://restore/tenant-a \
  --apply --report restore-report.json
```

Restore validates the complete backup before writing. It never overwrites or
deletes an object; matching objects make retries resumable, while corrupt or
unlisted target objects stop the operation before new bytes are copied. Keep
the backup manifest, command output, broker snapshot identity, and post-restore
query-equivalence results together as the recovery evidence bundle.

## Offline audit and repair

`audit-store` reads a live store in place. It lists every object under the
store URL, classifies each key by the grammar of the signal that owns it, and
checks the blocks, sidecars, indexes, manifests, symbols, delete state,
checksums, and the WAL offset bounds that block keys refer to. It does not
write or delete an object:

```bash
krabka-storage-admin audit-store \
  --store-url s3://krabka-o11y \
  --signal profiles --tenant tenant-a \
  --report store-audit.json
```

Omit `--signal` and `--tenant` to audit every signal and tenant. Add
`--verify-data` to read every row of every block, not only the footer. The
report is JSON with `schema_version: 1`. Each finding has a `kind`, a
`signal`, a `tenant`, a `path`, a `severity`, and a `repairable` flag. The
`detail` text is for a person and is not a stable contract. The command exits
unsuccessfully when a finding has the `damage` severity.

| Kind | Severity | Meaning |
| --- | --- | --- |
| `orphan` | damage | A block older than the grace window that no index names. Repairable. |
| `orphan_sidecar` | damage | A profiles `.symdb` sidecar whose block is absent. Repairable. |
| `pending` | warning | An unnamed block inside the grace window. A writer can still publish it. |
| `missing_sidecar` | damage | A live block without its sidecar. |
| `dangling_index_entry` | damage | An index, manifest, or metrics `.index` sidecar names a block that the store does not hold. |
| `corrupt_block` | damage | A block that does not decode as Parquet. |
| `unsupported_format` | damage | A block or manifest with a future format version. |
| `unreadable_manifest` | damage | An index snapshot or logs manifest that does not decode, or blocks with no index. |
| `unreadable_delete_state` | damage | A deletion marker or erasure request that does not decode, or that names another tenant. |
| `checksum_mismatch` | damage | An index shard payload whose content hash is not the one its manifest records. |
| `wal_overlap` | warning | Two blocks of one tenant cover overlapping WAL offsets of one partition. |
| `stale_frontier` | warning | The logs compaction frontier is ahead of the blocks that the store holds. |

The audit decides liveness as the owning service does. A metrics block is
live when its `.index` sidecar exists. A logs block is live when the global,
tenant, or shard manifest names it. A traces or profiles block is live when
the latest index snapshot names it. When the audit cannot read the index of a
tenant, it reports no orphans for that tenant, so a broken index never makes
live blocks look like orphans.

`repair` is report-first. It needs an explicit tenant, one signal, and an
allowlist of finding kinds. Only `orphan` and `orphan_sidecar` are
repairable; every other kind needs a person. Without `--apply` it only plans:

```bash
krabka-storage-admin repair \
  --store-url s3://krabka-o11y \
  --tenant tenant-a --signal profiles \
  --finding-kind orphan --finding-kind orphan_sidecar \
  --audit-log repair-audit.jsonl --report repair-plan.json
```

Read the plan, then run the same command with `--apply`. The repair obeys
these rules:

- It audits the tenant again at the start of the run. It does not act on an
  old report.
- It acts only on findings whose tenant is exactly `--tenant`. It never acts
  on shared state such as a fleet-wide index or the logs frontier.
- Before each delete it reads the object head again. It keeps the object when
  the entity tag or modification time changed after the audit listed it, or
  when the object is no longer older than `--grace` (default `1h`). The
  object store has no conditional delete, so the grace window is what keeps a
  writer that can still publish the object safe. Use a grace that is longer
  than the slowest block publication.
- An object that is already absent counts as `already_absent`, not as a
  failure. A delete that fails counts as `failed`, and the run continues with
  the next object. The command then exits unsuccessfully.

A second run with the same scope finds nothing to do, and a run that stopped
part of the way continues from where it stopped. Each action goes to
`--audit-log` as one JSON line, flushed before the next action, and the file
is opened for append. Each line records the run start time, the scope, the
kind, the path, and the outcome: `planned`, `deleted`, `already_absent`,
`skipped_changed`, or `failed`. Keep the audit report, the repair report, and
the audit log together.

After an applied repair, run `audit-store` again for the same tenant and
signal, and run the lifecycle and query suites of the signal before you
return the tenant to service.

Native Prometheus TSDB block import remains separate from backup restore. The
Mimir upload endpoint continues to reject Prometheus index/chunk encodings until
their complete histogram, exemplar, tombstone, checksum, and atomic-publication
contract is implemented and qualified.
