# Persisted format contract

Krabka is greenfield and has no deployments, so persisted formats have no backward-compatibility requirement. No reader decodes an older format version, no reader has a fallback for a missing version marker, and no qualification runs an upgrade or a rollback between builds.

Each format has one current version. Writers stamp it. Readers reject a missing, malformed, duplicate, or future version before they decode records, update an index, or write a replacement object. A change to a format increments its version, and readers then refuse data of the earlier version.

| Owner | Durable shape | Version marker | Compatibility policy |
| --- | --- | --- | --- |
| metrics | sample, exemplar, metadata, and compaction-clock Kafka WAL records | `krabka-format-version: 1`, required | exact version; validate before replay or commit |
| metrics | HA election and ruler/recording state Kafka records | `krabka-format-version: 1`, required | exact version; validate before replay or commit |
| logs | JSON and native Kafka WAL records | `krabka-format-version: 1` on JSON records; none on native records | exact version for JSON records; a record without the header decodes only as a native record, which a plain Kafka client writes with `krabka-tenant` and `krabka-log-*` headers |
| traces | span Kafka WAL records | `krabka-format-version: 1`, required | exact version; validate before live-store or block-builder mutation |
| profiles | profile Kafka WAL records | `krabka-format-version: 1`, required | exact version; validate before hot-store, index, or block mutation |
| blockstore | log, metric, trace, and profile Parquet blocks | Parquet key `krabka.format.version=1`, required | exact version; a missing or future marker is rejected from the footer |
| blockstore | series index shards | binary shard version 2 | exact version |
| blockstore | trace index shards | binary shard version 1 | exact version; rebuilt from retained blocks when required |
| blockstore | profile index and symbol shards | binary shard version 1 | exact version; symbol data is immutable per block |
| blockstore | index snapshot manifests | JSON version 1 | exact version; snapshots are replaceable from shards |
| blockstore | storage audit report, repair report, and repair audit log (JSONL of `intent` and `outcome` lines) | JSON `schema_version: 1` | exact version; readers reject another version before a repair acts |
| logs | block/index manifests, shard catalogs, and compaction frontier | JSON version 1 | exact version; atomic replacement |
| metrics | compaction index manifests and block-kind keys | JSON/versioned key version 1 | exact version; source blocks and WAL remain authoritative |
| metrics | Prometheus TSDB import records and block-ULID bindings | JSON `version: 1`, required | exact version; another or absent version stops the import before it writes |
| metrics | Prometheus TSDB import publication markers (`uploaded/<ULID>-<hash>/_published`) | empty object; its presence is the state | readers read no `.index` manifest in an import directory without the marker |
| traces | block metadata, compaction keys, and search index manifests | JSON/key version 1 | exact version; replacement only after output is durable |
| profiles | block metadata, SymbolDB objects, and lifecycle manifests | protobuf/JSON version 1 | exact version; immutable object keys |
| ruler | rule groups, evaluations, and active-alert tenant state | `krabka-format-version: 1`, required | exact version; validate the whole poll before state mutation |
| tenant admin | delete requests, deletion markers, overrides, and uploaded debuginfo | JSON/raw version 1 | one current schema; raw uploads are immutable |
| recovery | backup part manifests (`.krabka-recovery/manifest.json`) | JSON `schema_version` 1 | exact version; a manifest names a tenant, a part, or both; another version is rejected before a copy |
| recovery | deployment cut (`krabka-recovery/cut.json`) | JSON `schema_version` 1 | exact version; written last, so a set without it is incomplete; `omitted_parts` is required |
