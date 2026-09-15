# Persisted format contract

Release `v0.4` establishes format version 1. Missing version metadata means the
pre-release version 1, so a supported binary can read data written before this
contract. Writers emit version 1. Readers reject malformed, duplicate, or
future versions before decoding records, updating an index, or writing a
replacement object.

| Owner | Durable shape | Version marker | Compatibility policy |
| --- | --- | --- | --- |
| metrics | sample, exemplar, metadata, and compaction-clock Kafka WAL records | `krabka-format-version: 1` | N-1 read; current write |
| metrics | HA election and ruler/recording state Kafka records | `krabka-format-version: 1` | validate before replay or commit |
| logs | JSON and native Kafka WAL records | `krabka-format-version: 1`; absent for native/legacy | N-1 read; current write |
| traces | span Kafka WAL records | `krabka-format-version: 1` | validate before live-store or block-builder mutation |
| profiles | profile Kafka WAL records | `krabka-format-version: 1` | validate before hot-store, index, or block mutation |
| blockstore | log, metric, trace, and profile Parquet blocks | Parquet key `krabka.format.version=1` | absent is legacy v1; future rejected from footer |
| blockstore | series index shards | binary shard version 2 | readers accept the prior v1 encoding |
| blockstore | trace index shards | binary shard version 1 | exact version; rebuilt from retained blocks when required |
| blockstore | profile index and symbol shards | binary shard version 1 | exact version; symbol data is immutable per block |
| blockstore | index snapshot manifests | JSON version 1 | exact version; snapshots are replaceable from shards |
| logs | block/index manifests, shard catalogs, and compaction frontier | JSON version 1 | exact version; atomic replacement |
| metrics | compaction index manifests and block-kind keys | JSON/versioned key version 1 | exact version; source blocks and WAL remain authoritative |
| traces | block metadata, compaction keys, and search index manifests | JSON/key version 1 | exact version; replacement only after output is durable |
| profiles | block metadata, SymbolDB objects, and lifecycle manifests | protobuf/JSON version 1 | exact version; immutable object keys |
| ruler | rule groups, evaluations, and active-alert tenant state | Kafka version 1 | validate the whole poll before state mutation |
| tenant admin | delete requests, deletion markers, overrides, and uploaded debuginfo | JSON/raw version 1 | JSON schemas are additive; raw uploads are immutable |

The release qualification runs a real old/new rolling replacement across all
role services, queries the same four-signal corpus after every replacement,
then rolls every role back and queries it again. Its artifact records both
image IDs and every response. The initial supported release has no earlier
supported tag; its old side is the immediately preceding qualified `main`
image. Later releases use the previous supported release digest.
