# Disaster recovery

A Krabka deployment keeps its durable state in three places: the broker, one object-store bucket for each signal, and the logs `data-root` directory. A backup is restorable only when all three come from one point in time. This page calls that point a *cut*.

Two pieces take and restore a cut:

- `krabka-o11y-recovery` copies every bucket and local state directory of the deployment, records the broker offsets at the cut, and seals the set. It restores the set into empty stores, and it refuses a broker restored from another time.
- A copy of the stopped broker's log directory, such as a volume snapshot, is the broker half. It holds the WAL topics, the compacted state topics, and the committed group offsets.

`krabka-storage-admin backup`, `restore`, and `audit` copy one object-store prefix. They remain for a single-tenant prefix copy. They do not record or check broker state, so they do not make a deployment cut.

## What a cut holds

### Broker

| State | Where |
| --- | --- |
| The four WAL topics: `__krabka_metrics_wal`, `__krabka_observability_logs_wal`, `__krabka_traces_wal`, `__krabka_profiles_wal` | Broker log directory |
| Metrics ruler state: last group evaluation and pending or firing alerts | Compacted topic `__krabka_metrics_ruler_state` |
| HA tracker elections | Compacted topic `__krabka_metrics_ha` |
| Committed offset of every block builder and consumer group | `__consumer_offsets` |
| Cluster id, topic ids, topic configuration, and ACLs | `__cluster_metadata` |

All of it is in the broker's log directory. The topic contract does not tier the WAL topics, and a compacted topic cannot be tiered, so a `krabka restore` from a tiered-storage archive does not hold this state. `krabka-backup capture` holds the committed group offsets and the metadata checkpoint but not the records.

### Object store and local state

| Part | Store | State |
| --- | --- | --- |
| `metrics` | `s3://krabka-metrics` | Blocks and `.index` manifests, compacted blocks, Mimir ruler and Alertmanager configuration (`mimir-configs/`), metric erasure requests (`metric-erasure-requests/`), tenant deletion markers, Mimir block uploads |
| `logs` | `s3://krabka-logs` | Blocks, tenant shard catalogs and shard manifests, the compaction frontier |
| `logs-block-builder-data-root` | The logs block-builder `--data-root` volume | Log delete requests (`log-delete-requests.json`). The block builder serves the Loki delete API and applies the requests to blocks. |
| `logs-querier-data-root` | The logs querier `--data-root` volume | Loki rule groups (`loki-ruler-rules.json`), and the querier's copy of the delete requests, which it applies as query filters |
| `traces` | `s3://krabka-traces` | Span blocks, compacted blocks, the trace index snapshots and shard payloads |
| `profiles` | `s3://krabka-profiles` | Profile blocks and their SymbolDB `.symdb` objects, the profile index, tenant settings and recording rules (`profiles-admin/`), debuginfo uploads (`debug-info/`) |
| `traces-overrides` | The `--traces-api-overrides-file` directory | API-written trace overrides. Only when the file is configured. |

The two logs `data-root` volumes are separate parts because a deployment mounts one volume for each role. An all-in-one logs role has one `data-root`; back it up as one part.

### State that a cut does not need

| State | Why it is not in the cut |
| --- | --- |
| Metrics, traces, and profiles hot stores, and the traces live store | Memory only. Each role replays it from the WAL. |
| Query-frontend result caches | A cache. A miss reads the blocks again. |
| Metrics-generator edge checkpoints | Memory only. |
| The ruler lease on `__coordination_state` | A lease. A new ruler takes it after the lease expires. |
| Runtime overrides YAML for metrics, logs, and profiles | Configuration from the deployment, not state. |

## Take a cut

1. Stop the distributors of all four signals, and the metrics ruler. A writer that moves during the copy makes the cut fail.
2. Let every block builder drain. Each one must commit every record of its WAL. A block builder that stops between a block write and its offset commit leaves records that a restored block builder writes again under other block keys.
3. Stop the block builders, compactors, symbolizers, and the profiles recording-rule exporter. Nothing may write to a bucket or a `data-root` during the copy.
4. Copy the object and local parts, and seal the cut. The broker must be up for this step:

```bash
krabka-o11y-recovery backup \
  --bootstrap broker:9092 \
  --backup-url s3://backups/cut-2027-02-01 \
  --cut-id cut-2027-02-01 \
  --broker-capture broker-volume-snapshot-2027-02-01 \
  --part metrics=s3://krabka-metrics \
  --part logs=s3://krabka-logs \
  --part logs-block-builder-data-root=file:///mnt/logs-block-builder \
  --part logs-querier-data-root=file:///mnt/logs-querier \
  --part traces=s3://krabka-traces \
  --part profiles=s3://krabka-profiles \
  --apply --report backup-report.json
```

The command reads the broker before and after the copy. It refuses to seal the cut when the two reads differ, or when a block-builder group has a record after its committed offset. It copies every object with create-if-absent semantics, writes one manifest for each part under `parts/<name>/`, and writes `krabka-recovery/cut.json` last. The cut names every part by the SHA-256 of its manifest, and it records the next offset of every partition and every committed group offset.

5. Stop the broker, and take the snapshot of its log directory. Record the snapshot name; `--broker-capture` stores it in the cut.

A set without `krabka-recovery/cut.json` is not a backup. Repeat the command with the same arguments to resume an interrupted copy. After the broker moves, start again under a new backup URL.

`krabka-o11y-recovery` names the default block-builder groups: `krabka-metrics-block-builder`, `krabka-observability-block-builder`, `krabka-traces-block-builder`, and `krabka-profiles-block-builder`.

## Audit a cut

Audit is read-only:

```bash
krabka-o11y-recovery audit \
  --backup-url s3://backups/cut-2027-02-01 \
  --report audit.json
```

The command fails when the cut is absent, a part is absent, a part manifest has a digest that the cut does not name, a part belongs to another cut, or an object belongs to no part. It reports missing, corrupt, and unlisted objects in each part.

## Restore into an empty deployment

1. Restore the broker log directory from the snapshot, and start the broker with `bootstrap-mode` `rejoin`. Start no Krabka role.
2. Restore the object and local parts into empty buckets and an empty `data-root`:

```bash
krabka-o11y-recovery restore \
  --bootstrap restored-broker:9092 \
  --backup-url s3://backups/cut-2027-02-01 \
  --part metrics=s3://restore-metrics \
  --part logs=s3://restore-logs \
  --part logs-block-builder-data-root=file:///mnt/logs-block-builder \
  --part logs-querier-data-root=file:///mnt/logs-querier \
  --part traces=s3://restore-traces \
  --part profiles=s3://restore-profiles \
  --apply --report restore-report.json
```

3. Start the roles.

Before it writes one object, the restore:

- audits the set, and refuses any damage or mix;
- refuses a target list that does not name exactly the parts of the cut;
- reads the restored broker, and refuses it when any partition offset or committed group offset differs from the cut;
- refuses a target that holds an object that the part does not hold.

The restore never overwrites or deletes an object, so a retry resumes. A broker that has just started can report offset 0 for a partition whose log it has not opened yet. The restore then refuses with a `wal_offset` finding whose `actual` is 0. Wait until the broker is ready, and run the restore again. No index needs a manual edit: every index and manifest is an object in its part, and each role loads it at start.

After the restore, each block builder resumes at its committed offset. No record of its WAL at the cut lies after that offset, so it reads no record of a block that the cut holds and publishes no record twice.

## Recovery evidence

Keep these together: the backup, audit, and restore reports; `krabka-recovery/cut.json`; the broker snapshot name; and query answers from before the cut and after the restore.

The `recovery` qualification gate runs `//crates/integration:backup_restore_test`. That suite writes all four signals for two tenants, plus rules, ruler state, delete requests, profile symbols, and trace-id correlations, through an in-process broker. It seals a cut, copies the stopped broker's log directory, and restores into empty stores and a broker that starts on the copy. It compares every query answer before and after, then ingests more records and checks that every WAL record is in exactly one block. The ruler state and a recording-rule sample go in one Kafka transaction, as the ruler writes them. The suite writes its reports and answers with a `SHA256SUMS` manifest into `KRABKA_RECOVERY_EVIDENCE_DIR`, or into Bazel's undeclared test outputs. The gate archives those outputs and the test log as `milestone-21-recovery-evidence-<commit>`.

## Limits

- The qualified broker half is a copy of the stopped broker's log directory on a single-node broker. A multi-node broker restore is not qualified here.
- The metrics ruler writes the metrics WAL and its state topic in Kafka transactions. A committed transaction ends with a marker that no block covers, so the drained check counts the records that a `read_committed` reader finds after the committed offset, not the offsets. A ruler that stops with an open transaction holds back that reader, and the backup refuses the cut. Stop the ruler before the copy.
- No suite restores a Kubernetes deployment end to end. The `recovery` gate runs the roles' write and read paths in one process.
- `--traces-api-overrides-file` is not a default part. Add it as a `file://` part when a deployment sets it.

Native Prometheus TSDB block import is separate from backup restore. The Mimir
block-upload endpoint imports Prometheus blocks with one commit point for each
block. See [Migrating Prometheus TSDB Blocks](prometheus_tsdb_migration.md).
An imported block is an ordinary metric block with an ordinary manifest. The
import records are under `mimir-block-uploads/<tenant>/` in the metrics store.
The `metrics` part copies that whole store, so the cut holds them, and a
restored cluster does not import the same block again.
