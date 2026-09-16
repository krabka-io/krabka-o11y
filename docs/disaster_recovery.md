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

Native Prometheus TSDB block import remains separate from backup restore. The
Mimir upload endpoint continues to reject Prometheus index/chunk encodings until
their complete histogram, exemplar, tombstone, checksum, and atomic-publication
contract is implemented and qualified.
