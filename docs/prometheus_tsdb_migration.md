# Migrating Prometheus TSDB Blocks

This guide moves historical data from a Prometheus server into Krabka. Krabka imports each Prometheus TSDB block through the Mimir block-upload API, so a block keeps its samples, stale markers and deletions.

## What the import accepts

| Part of the block | Supported | Notes |
| --- | --- | --- |
| `index` | Format versions 2 and 3 | Version 1 is from Prometheus 1.x. Rewrite such a block with a current Prometheus first. |
| `chunks/NNNNNN` | Segment format version 1 | XOR float chunks, integer histogram chunks and float histogram chunks. |
| Native histograms | Schemas -4 to 8, and custom buckets (schema -53) | Integer and float histograms, gauge histograms, and counter-reset hints. |
| Stale markers | Yes | A stale histogram becomes a float stale marker. The query path then hides the series as Prometheus does. |
| `tombstones` | Format version 1 | The import drops every deleted sample. Mimir rejects a block that has this file, so this is a Krabka extension. |
| Exemplars and metric metadata | Not applicable | A TSDB block does not hold them. See [Metadata and exemplars](#metadata-and-exemplars). |

The import validates the whole block before it writes anything:

- It checks the CRC32C of the index table of contents, every index section, every series entry and every chunk.
- It checks that series are in label order and that sample timestamps increase in each series.
- It checks that every sample is in the range `[minTime, maxTime)` of `meta.json`.
- It accepts the same `thanos.labels` external labels as Mimir: `__compactor_shard_id__` with a value such as `1_of_4`, and the deprecated `__org_id__`, `__ingester_id__` and `__shard_id__`. The `start` request fails with `400 unsupported external label` for any other label. As in Mimir, the import does not add an external label to the series.
- If `__org_id__` is present, it must be the tenant of the request. Mimir ignores the value of this label, so this check is a Krabka addition.
- It applies the limits in the table below. A block over a limit fails with the name of that limit.

| Limit | Value |
| --- | --- |
| Index, chunk segment and tombstones bytes | 1 GiB |
| Series | 1,000,000 |
| Symbols | 4,000,000 |
| Labels per series | 256 |
| Chunks per series | 100,000 |
| Chunks of all series | 25,000,000 |
| Samples, before tombstones | 25,000,000 |
| Spans, buckets or custom bounds of one histogram side | 65,536 |

## Atomicity and re-import

An upload that fails leaves no index entry. The import writes the Parquet blocks first, then one import record, then the `.index` manifests that make the blocks queryable. The import record is the commit point. Before the record exists, a failure deletes what the import wrote. After the record exists, a retry completes the import.

The manifests do not become live at the same time. A block with float and native-histogram samples has two manifests, and the import writes the float manifest first. A query that runs between the two writes can return the float samples without the histogram samples. If the querier stops between the two writes, `check` gives `validating`, and the float samples stay queryable without the histogram samples. Send `finish` again. The retry writes the missing manifest. An upload of the same content under a new ULID also writes it.

The import is idempotent by content. The content hash is a SHA-256 over the `index`, each chunk segment and the `tombstones` file.

| Upload | Result |
| --- | --- |
| The same ULID, after `complete` | `409 block already exists`, as in Mimir |
| Other content under a ULID that has an import | `check` gives `failed` and names both hashes |
| The same content under a new ULID | `check` gives `complete` and `existingBlock`, which names the first ULID. No sample is added. |

Mimir does not return `existingBlock`. Krabka adds it only when an upload adds no samples, so a Mimir client that ignores the field sees the Mimir response.

## Capacity

These figures come from the fixture block in `crates/metrics/tests/testdata/tsdb/`. The block has 7 series and 3,360 samples over two hours.

| Data | Bytes | Bytes per sample |
| --- | --- | --- |
| Prometheus `index`, `chunks/000001` and `tombstones` | 24,143 | 7.2 |
| Krabka float Parquet block, 1,861 rows | 11,126 | 6.0 |
| Krabka native-histogram Parquet block, 1,438 rows | 20,139 | 14.0 |
| Two `.index` manifests | 1,436 | not applicable |
| Upload state, binding and import record | 1,231 | not applicable |

Plan the object storage with these rules:

- Imported float samples need about the same space as in Prometheus.
- Imported native histograms need about two times the Prometheus space. The factor increases with the bucket count.
- The upload keeps its source files under `mimir-block-uploads/<tenant>/<ULID>/files/`. Count the source size once more until you delete them. The procedure below tells you when that is safe.
- Retention applies to imported blocks. A block older than the `compactor_blocks_retention_period` of its tenant expires at the next retention sweep. Set the retention before you import old data.

Plan the querier memory with these rules:

- The `finish` request holds all the uploaded files in memory, up to the 1 GiB limit.
- It also holds each decoded sample. A float sample needs about 40 bytes. A histogram sample needs about 150 bytes, plus 8 bytes for each bucket.
- The Parquet writer holds a second copy of the decoded rows while it writes.
- For a block at the sample limit of 25,000,000 float samples, plan about 3 GiB for one `finish` request.
- Upload one block at a time to each querier. Each concurrent `finish` needs its own memory.

Before you upload, read `stats.numSamples` and `stats.numSeries` in the block's `meta.json`. If a value is over a limit, the upload fails. Prometheus compacts blocks up to 10% of its retention, or 31 days. To keep blocks small, copy the blocks from a Prometheus that runs with `--storage.tsdb.max-block-duration=2h`.

## Procedure

Do these steps for each block directory. `KRABKA` is the query address of a querier or query frontend, by default port 4041. `TENANT` is the tenant that receives the data.

1. Stop the Prometheus server, or copy only blocks that it does not compact again. A block is complete when its directory has `meta.json`.
2. Make the upload `meta.json`. It is the Prometheus `meta.json`, plus a `thanos.files` list with the size of each file:

   ```bash
   BLOCK=01M3MJXM7R4M5X4Q4CKHW5Q8N0
   cd "/prometheus/$BLOCK"
   FILES=$(find index chunks tombstones -type f 2>/dev/null | sort | while read -r f; do
     jq -n --arg p "$f" --argjson s "$(stat -c %s "$f")" '{rel_path: $p, size_bytes: $s}'
   done | jq -s '[{rel_path: "meta.json"}] + .')
   jq --argjson files "$FILES" '.thanos.files = $files' meta.json > /tmp/upload-meta.json
   ```

3. Start the upload:

   ```bash
   curl -fsS -H "X-Scope-OrgID: $TENANT" --data-binary @/tmp/upload-meta.json \
     "$KRABKA/api/v1/upload/block/$BLOCK/start"
   ```

4. Upload each file:

   ```bash
   for f in index chunks/* tombstones; do
     [ -f "$f" ] || continue
     curl -fsS -H "X-Scope-OrgID: $TENANT" --data-binary "@$f" \
       "$KRABKA/api/v1/upload/block/$BLOCK/files?path=$(jq -rn --arg p "$f" '$p|@uri')"
   done
   ```

5. Finish the upload. Krabka validates and imports the block before it responds. Set a client timeout that is long enough for a large block:

   ```bash
   curl -fsS -m 3600 -H "X-Scope-OrgID: $TENANT" -X POST \
     "$KRABKA/api/v1/upload/block/$BLOCK/finish"
   ```

6. Read the result:

   ```bash
   curl -fsS -H "X-Scope-OrgID: $TENANT" "$KRABKA/api/v1/upload/block/$BLOCK/check"
   ```

   | Response | Meaning | Action |
   | --- | --- | --- |
   | `{"result":"complete"}` | The block is imported. | Go to the verification. |
   | `{"result":"complete","existingBlock":"..."}` | The same content is already imported. | Nothing. No sample was added. |
   | `{"result":"failed","error":"..."}` | The block is invalid or over a limit. Nothing is live. | Read the error. Fix the block, then upload it under a new ULID. |
   | `{"result":"validating"}` | An object-store failure stopped `finish`. | Send `finish` again. |

7. When `check` gives `complete`, you can delete `mimir-block-uploads/<tenant>/<ULID>/files/` from the bucket. The import does not read those files again.

`mimirtool backfill` sends the same four requests. The Krabka test suites do not run mimirtool.

## Verification

Compare Krabka with the Prometheus server that wrote the block. Run each query against both, at a time in the block range:

```bash
T=1750000000
for q in 'count({__name__=~".+"})' \
         'count_over_time({__name__=~".+"}[2h])' \
         'sum by (__name__) (count_over_time({__name__=~".+"}[2h]))'; do
  diff <(curl -fsS "$PROMETHEUS/api/v1/query" --data-urlencode "query=$q" --data-urlencode time=$T | jq -S .data) \
       <(curl -fsS -H "X-Scope-OrgID: $TENANT" "$KRABKA/api/v1/query" --data-urlencode "query=$q" --data-urlencode time=$T | jq -S .data)
done
```

The `tsdb_import` suite in `crates/metrics-service` does this comparison against the pinned Prometheus image. It covers instant queries, range queries, `rate`, `increase`, the histogram functions, the tombstoned series, cardinality through `count` and the discovery endpoints, `/api/v1/series`, the label endpoints, `/api/v1/metadata` and `/api/v1/query_exemplars`.

### Metadata and exemplars

A TSDB block holds no metric metadata and no exemplars. Prometheus v3.14.0, which is `github.com/prometheus/prometheus v0.314.0`, shows this in its source:

- `LeveledCompactor.write` in `tsdb/compact.go` writes only `chunks/`, `index`, `meta.json` and `tombstones`.
- The index table of contents, `TOC` in `tsdb/index/index.go`, has only symbols, series, label indices and postings. A series entry holds labels and chunk references.
- `BlockMeta` in `tsdb/block.go` holds the ULID, the time range, the stats, the compaction data and the version.
- Exemplars are in a ring buffer in the head. `DB.ExemplarQuerier` in `tsdb/db.go` reads only `db.head.exemplars`. The WAL has exemplar and metadata records, but compaction does not copy them into a block.
- `/api/v1/metadata` reads the metadata of the active scrape targets, in `API.metricMetadata` of `web/api/v1/api.go`.

`github.com/prometheus/prometheus v0.307.0`, the version of the reference dumper, has the same code. So Prometheus that serves only the block answers `{"status":"success","data":{}}` for `/api/v1/metadata` and `{"status":"success","data":[]}` for `/api/v1/query_exemplars`. Krabka gives the same answers after the import.

### Cardinality

Prometheus gives `/api/v1/status/tsdb` from its head only. For a persisted block, Prometheus gives a head with no series and four empty lists. Krabka counts the series of its blocks. `/api/v1/status/tsdb` and the Grafana Mimir routes `/api/v1/cardinality/label_names`, `/api/v1/cardinality/label_values`, `/api/v1/cardinality/active_series` and `/api/v1/cardinality/active_native_histogram_metrics` take no time range. For such a request, Krabka reads only the blocks inside `--unbounded-compatibility-lookback`, by default one hour. An imported block is usually older than that, so these routes do not count it. This agrees with Prometheus and Mimir, which read these routes from the head. To count the imported series, start the querier with a lookback that reaches the block.

To compare the cardinality of the block with Prometheus, use `count` and the discovery endpoints with a time range:

```bash
curl -fsS -H "X-Scope-OrgID: $TENANT" "$KRABKA/api/v1/query" \
  --data-urlencode 'query=count by (__name__) ({__name__=~".+"})' --data-urlencode time=$T
curl -fsS -H "X-Scope-OrgID: $TENANT" "$KRABKA/api/v1/label/__name__/values" \
  --data-urlencode start=$START --data-urlencode end=$END
```

### Tombstones and Prometheus v2.45.0 and later

Prometheus v2.45.0 and later, up to at least v3.15.0, can fail a query of a series that has a tombstone. The error is `unexpected error: runtime error: index out of range [2] with length 2`. `/api/v1/series` closes the connection. Krabka does not have this problem. If you verify a block with tombstones against Prometheus, expect this error. It is not a sign of a damaged block.

Prometheus fails when four conditions are true for one series and one query window. A chunk that the query reads starts before the window, and a chunk that it reads ends after the window. The tombstone starts after the start of the window, and it ends at or after the end of the window. Prometheus trims the chunks to the window. It puts the front trim interval before the tombstone, then adds the back trim interval. `Intervals.Add` in `tsdb/tombstones/tombstones.go` then reads past the end of the list, at line 379 of v0.314.0. Its open-ended branch sets `maxi := len(in)`, but `maxi` counts from `mini`. Commit `80b7f73d26` added that branch. The `tsdb_import` suite records the windows that fail with the pinned image.

## Rollback

To remove one imported block, delete its objects in this order. The manifests go first, so no query finds a manifest whose block is gone.

1. Read the content hash from `mimir-block-uploads/<tenant>/<ULID>/import.json`. The field is `sha256`.
2. Read the import record `mimir-block-uploads/<tenant>/by-sha256/<sha256>.json`. Its `objects` list names each `index_key` and `block_key`.
3. Delete each `index_key`. After the cold-index cache interval, 30 seconds by default, queries do not return the block.
4. Delete each `block_key`.
5. Delete the import record, then `mimir-block-uploads/<tenant>/<ULID>/`.

Do not delete the import record while the samples are live. Without the record, a new upload of the same block imports the samples again.

Compaction can merge an imported block with other blocks of the tenant. Then the keys in the import record are gone, and the samples are in a merged block. Use one of these methods instead:

- Delete the series with the Prometheus `POST /api/v1/admin/tsdb/delete_series` API, when the admin API is on. Give the block's time range as `start` and `end`.
- Delete the whole tenant with `POST /compactor/delete_tenant`. This also deletes every upload of the tenant.

A rollback of the Krabka binary to v0.4 keeps the imported data queryable. The imported blocks and manifests use the same formats as the blocks that the block builder writes. The v0.4 binary does not read the import record or the binding. It rejects a new Prometheus TSDB upload, and it does not change the imported blocks.
