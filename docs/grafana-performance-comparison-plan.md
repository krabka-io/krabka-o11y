# Grafana performance comparison: source and deployment plan

Research checked on 2026-10-04. This document describes the comparison design; it contains no measured Grafana performance result. The initial experiment reruns both implementations with a common workload, including 1,000 metric series and one sample per series per request. It measures API-acknowledged throughput and labels native durability differences explicitly. Prior qualified limits are not used as direct ratio inputs. The Krabka envelope in [operating_envelope.md](operating_envelope.md) is a fixed single-node measurement, so the comparison must use that same host class, data, query mix and resource accounting.

## Versions

Official GitHub `releases/latest` returned these stable releases. The existing differential oracle pins are independent compatibility evidence, not measurements against the newest releases.

| Signal | Current stable release | Existing repository oracle |
| --- | --- | --- |
| Logs | [Loki 3.7.8](https://github.com/grafana/loki/releases/tag/v3.7.8), published September 17 | 3.7.7 |
| Metrics | [Mimir 3.2.1](https://github.com/grafana/mimir/releases/tag/mimir-3.2.1), published September 10 | 3.2.1 |
| Profiles | [Pyroscope 2.3.1](https://github.com/grafana/pyroscope/releases/tag/v2.3.1), published September 8 | 2.3.1 |
| Traces | [Tempo 3.1.0](https://github.com/grafana/tempo/releases/tag/v3.1.0), published September 29 | 3.0.3 |

Latest linux/amd64 manifests resolved from the image registry: Loki `sha256:81a6802ec4bd1b88c564494f06376889ed022998a188826190d26d2754ac2aae`; Tempo `sha256:19dca9c0b1801209424a757cd5970d6ffd7cfc9a7f4966c2a6795fbe29b495a8`.

Resolve release tags to immutable commits and linux/amd64 OCI manifests before measurement. Capture the binary-reported version, complete image manifest and effective `/config`. `MODULE.bazel` and `docs/api/upstream_surfaces.json` already pin the older compatibility oracles.

## Initial experiment and architecture references

The runnable files in `deploy/compare/{loki,mimir,tempo,pyroscope}.yaml` use native single-binary RF1 shapes, matching total service-plus-broker CPU and memory budgets against Krabka. Mimir explicitly uses its classic ingester path; Tempo monolithic mode bypasses Kafka; Pyroscope selects v2. This first experiment is an API performance comparison. Kafka-based production paths below are a separate topology for later qualification. Use three 60-second steady repetitions plus burst/cardinality ramps; mark cold results unqualified unless actual object-read evidence is available.

## Deployment and acknowledgement boundaries

Use one active signal at a time on the same GCP e2-standard-16 host with the existing digest-pinned MinIO. Compare whole deployment CPU and RSS, including brokers and metadata services, and disclose both component count and configured resource budget. RF1 here means a single copy in the measured fault domain; it does not qualify a highly available deployment.

| Product | Recommended measured shape | Acknowledgement and durability boundary |
| --- | --- | --- |
| Loki | Single binary, TSDB v13 index, S3/MinIO, RF1, persistent local WAL | The ingester WAL protects acknowledged data across process crashes. Disk-full and corruption exceptions need separate evidence. |
| Mimir | Kafka ingest storage enabled, one partition and ingester; S3/MinIO blocks; either monolithic modules with ingester shipping or explicit distributed block builders | Kafka decouples distributor writes from ingestion/query availability. Kafka ingest storage is preferred and stable since Mimir 3.0. |
| Tempo | Microservices: one distributor, live-store, block-builder, query-frontend, querier, backend-scheduler and backend-worker; one Kafka partition; S3/MinIO | Distributor waits for Kafka `acks=all`. Object upload and live-store query visibility happen asynchronously. |
| Pyroscope | `-target=all -architecture.storage=v2`, S3/MinIO, persistent single-peer metastore Raft state | V2 writes segments directly to object storage. The normal path acknowledges after object storage and metadata publication; it does not use Kafka. |

These choices follow the [Loki WAL contract](https://grafana.com/docs/loki/latest/operations/storage/wal/), [Mimir Kafka configuration](https://grafana.com/docs/mimir/latest/configure/configure-kafka-backend/), [Tempo Kafka acknowledgement contract](https://grafana.com/docs/tempo/latest/set-up-for-tracing/setup-tempo/configure-kafka/) and [Pyroscope v2 architecture](https://grafana.com/docs/pyroscope/latest/reference-pyroscope-v2-architecture/about-pyroscope-v2-architecture/).

Tempo `-target=all` bypasses Kafka and sends directly to its in-process live-store. It is a valid small-installation comparison, but it must be reported separately from the Kafka-backed production architecture. The required distributed services and an example Compose deployment are documented in [Tempo deployment modes](https://grafana.com/docs/tempo/latest/reference-tempo-architecture/deployment-modes/) and the [3.1.0 example](https://github.com/grafana/tempo/tree/v3.1.0/example/docker-compose/distributed).

Pyroscope v2 single-node mode has no high availability. Production guidance calls for multiple instances and three or five metastore peers. The single-peer run matches the scope of the Krabka RF1 envelope; label it accordingly. See [Pyroscope deployment modes](https://grafana.com/docs/pyroscope/latest/reference-pyroscope-v2-architecture/deployment-modes/).

The existing local differential suites offer tested API/configuration starting points: `crates/observability/tests/loki_differential.rs`, `crates/metrics-service/tests/diff_mimir.rs`, `crates/traces/tests/tempo_differential.rs` and `crates/profiles/tests/pyroscope_differential.rs`. Their filesystem-only storage and long Mimir head retention are designed for correctness corpora; use S3 and realistic flushing for performance.

## Configuration fields and API adapters

### Loki

Set `common.replication_factor: 1`, `schema_config.configs[].store: tsdb`, `schema: v13`, `object_store: s3`, and `index.period: 24h`. Configure S3 endpoint, bucket and path-style requests, persistent `common.path_prefix`, `storage_config.tsdb_shipper.active_index_directory` and `cache_location`. Enable `ingester.wal.enabled` explicitly and persist its directory. Use `ingester.max_chunk_age`, `chunk_idle_period`, `chunk_target_size` and `flush_check_period` to disclose the flushing policy. For capacity tenants, raise `limits_config.ingestion_rate_mb`, `ingestion_burst_size_mb`, `per_stream_rate_limit` and `per_stream_rate_limit_burst`; set `max_global_streams_per_user: 0` when measuring saturation beyond a configured stream cap. See [Loki configuration](https://grafana.com/docs/loki/latest/configure/) and [TSDB configuration](https://grafana.com/docs/loki/latest/setup/migrate/migrate-to-tsdb/).

Reuse `/loki/api/v1/push` and `/loki/api/v1/query_range` verbatim. `POST /flush` triggers ingester chunk uploads; completion must be observed before excluding the ingester from a cold query. See the [Loki HTTP API](https://grafana.com/docs/loki/latest/reference/loki-http-api/).

For retention, enable `compactor.retention_enabled`, `delete_request_store: s3`, a disclosed compaction interval/delete delay, and `limits_config.retention_period`. The 24-hour minimum applies to `retention_stream` rules, not global `retention_period`; this distinction is visible in [3.7.8 limit validation](https://github.com/grafana/loki/blob/v3.7.8/pkg/validation/limits.go). Verify physical objects disappear as well as queries becoming empty. Explicit deletion also has a cancellation period: see [log deletion](https://grafana.com/docs/loki/latest/operations/storage/logs-deletion/).

### Mimir

Enable `ingest_storage.enabled: true`, `ingest_storage.kafka.address`, `topic` and `auto_create_topic_default_partitions: 1` in every participating process. Use an ingester ID ending in `-0`, because the Kafka partition ID is derived from the ingester instance ID. Configure `ingester.partition_ring.min_partition_owners_count: 1`. Set `common.storage.backend: s3`, S3 credentials/endpoint and `blocks_storage.s3.bucket_name`. For a single gateway, configure its sharding ring RF1. Disable series caps with `limits.max_global_series_per_user: 0`; use explicit sufficiently high `ingestion_rate` and `ingestion_burst_size`. See [the 3.2.1 ingest-storage example](https://github.com/grafana/mimir/blob/mimir-3.2.1/development/mimir-ingest-storage/config/mimir.yaml), [partition-ID initialization](https://github.com/grafana/mimir/blob/mimir-3.2.1/pkg/ingester/ingester.go) and [configuration reference](https://grafana.com/docs/mimir/latest/configure/configuration-parameters/).

`target=all` includes ingester, distributor, querier, store-gateway, query frontend/scheduler, ruler and compactor, but excludes the independent block-builder and block-builder-scheduler modules. Keep ingester block shipping enabled for that shape; do not copy `ship_interval: 0s` from the distributed block-builder example without deploying its builders. This is established by [3.2.1 module dependencies](https://github.com/grafana/mimir/blob/mimir-3.2.1/pkg/mimir/modules.go).

The existing Influx corpus can use `POST /api/v1/push/influx/write?precision=ms` with `distributor.influx_endpoint_enabled: true`. Queries need `/prometheus/api/v1/query`. `POST /ingester/flush?wait=true` provides synchronous flushing for the ingester-based block-shipping shape. Store-gateway synchronization and query routing still need verification before calling a query cold. See [Mimir HTTP API](https://grafana.com/docs/mimir/latest/references/http-api/).

### Tempo

Reuse OTLP protobuf `/v1/traces` and TraceQL `/api/search`. Use the distributed example above, remove optional metrics generators, external tracing exporters and caches unless accounted for on both sides. Set `ingest.kafka.address`, `topic` and one auto-created partition. Set `block_builder.instance_id` and `assigned_partitions: {block-builder-0: [0]}`. Disclose `consume_cycle_duration`; the example uses 30 seconds while the default is five minutes. Configure `live_store` partition ownership and startup readiness, and persist its local WAL. Use `overrides.defaults.ingestion.rate_limit_bytes`, `burst_size_bytes`, `max_traces_per_user` and `overrides.defaults.global.max_bytes_per_trace` to separate configured limits from measured saturation. See [3.1.0 example config](https://github.com/grafana/tempo/blob/v3.1.0/example/docker-compose/distributed/tempo.yaml) and [configuration](https://grafana.com/docs/tempo/latest/configuration/).

Cold reads must exclude live-store data and verify object GETs. Block-builder offsets commit after upload; `tempo_ingest_group_partition_lag{group="block-builder"}` is a useful check, accompanied by exact broker end-offset evidence. See [block-builder lifecycle](https://grafana.com/docs/tempo/latest/reference-tempo-architecture/components/block-builder/).

### Pyroscope

Select v2 explicitly with `-architecture.storage=v2`, and disable incidental self-ingestion with `-self-profiling.disable-push=true`. The rendered v2 single-binary chart confirms these flags. Configure `storage.backend: s3`, endpoint, bucket and path style; persist `metastore.raft.dir` and `metastore.data_dir`, with one bootstrap peer. A separate segment writer needs `segment_writer.lifecycler.ring.replication_factor: 1` and a reachable ring backend. Preserve or disclose the default `segment_writer.segment_duration: 500ms`; this directly affects acknowledged write latency. Metadata DLQ settings also affect failure behavior, so retain the effective configuration. See [2.3.1 single-binary chart](https://github.com/grafana/pyroscope/blob/v2.3.1/operations/pyroscope/helm/pyroscope/rendered/single-binary-v2.yaml) and [2.3.1 generated configuration](https://github.com/grafana/pyroscope/blob/v2.3.1/docs/sources/configure-server/reference-configuration-parameters/index.md).

Reuse the legacy groups `/ingest` request and Connect `querier.v1.QuerierService/SelectMergeStacktraces` request already exercised by the local differential suite. The resulting profile type is `process_cpu:cpu:nanoseconds:cpu:nanoseconds`; require a positive flamegraph total and matching stack contents. V2 queries already read object storage; a separate hot/cold label must describe cache state rather than an ingester-head transition.

## Configuration validation

All YAML fields were checked against the corresponding release sources. Exact-image parsing/module resolution passed for Mimir 3.2.1 and Pyroscope 2.3.1; Tempo 3.1.0 `-config.verify=true` passed. Loki 3.7.8 `-verify-config=true` also passed. These checks establish configuration validity; HTTP writes, nonempty queries and object-read proof remain measurement prerequisites. The finite rate ceilings in these files are intentionally far above the measured schedule; preserve them in the effective configuration and report any rate-limit response rather than calling it hardware saturation.

## Evidence and fair interpretation

Run the same seed, label sets, row counts, writer schedule, query windows and positive-result checks. Record acknowledged rows/s separately from query visibility and verified durable rows/s. Repeat at least three times; publish median/min/max and raw operation timestamps. Preserve failures and higher failing ramp points. Never infer a throughput ratio from unrelated vendor scale claims or default cardinality limits.

Collect RSS and CPU for every process plus broker/MinIO. `process_cpu_seconds_total` measures total user and system CPU time in the [Prometheus Go collector](https://github.com/prometheus/client_golang/blob/main/prometheus/process_collector.go); use reset-aware deltas. Host `/proc` or cgroup counters provide a common fallback for Rust and Go, and container CPU throttling should be preserved. Report CPU seconds per million successful durable rows and both simultaneous deployment RSS and the sum of per-role peaks.

Measure S3 requests and transferred bytes at the shared MinIO boundary so client instrumentation differences do not determine the comparison. Record request method/status and read/write bytes, exclude initialization/scraping, and require verified object reads during cold phases. Client cache misses and request retries remain separately useful diagnostics. Maintenance rows need observed uploads, compaction outputs and deletion proofs; elapsed wall time alone is insufficient.

The initial comparison should distinguish this fixed RF1 single-node workload from HA, network-separated S3, long retention, TLS and broad query-mix performance. Public endpoints and schema support are already checked by the differential suites; they do not establish measured speed or cost parity.
