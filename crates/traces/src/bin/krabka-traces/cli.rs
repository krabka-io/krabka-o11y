use super::*;

#[derive(Debug, Parser)]
#[command(name = "krabka-traces")]
#[command(about = "Tempo-compatible traces service for Krabka")]
pub(crate) struct Cli {
    #[command(flatten)]
    pub(crate) profiling: krabka_telemetry::profiling::ProfilingConfig,
    #[arg(long, env = "KRABKA_TRACES_TARGET")]
    pub(crate) target: Target,
    #[arg(long, env = "KRABKA_TRACES_LISTEN", default_value = "127.0.0.1:3200")]
    pub(crate) listen: String,
    #[arg(long, env = "KRABKA_ADMIN_LISTEN_ADDR", default_value = "0.0.0.0:9404")]
    pub(crate) admin_listen_addr: SocketAddr,
    #[arg(
        long,
        env = "KRABKA_TRACES_GRPC_LISTEN",
        default_value = "127.0.0.1:4317"
    )]
    pub(crate) grpc_listen: String,
    #[arg(
        long,
        env = "KRABKA_TRACES_OTLP_HTTP_LISTEN",
        default_value = "127.0.0.1:4318"
    )]
    pub(crate) otlp_http_listen: String,
    #[arg(
        long,
        env = "KRABKA_TRACES_JAEGER_GRPC_LISTEN",
        default_value = "127.0.0.1:14250"
    )]
    pub(crate) jaeger_grpc_listen: String,
    #[arg(
        long,
        env = "KRABKA_TRACES_JAEGER_COMPACT_LISTEN",
        default_value = "127.0.0.1:6831"
    )]
    pub(crate) jaeger_compact_listen: String,
    #[arg(
        long,
        env = "KRABKA_TRACES_JAEGER_HTTP_LISTEN",
        default_value = "127.0.0.1:14268"
    )]
    pub(crate) jaeger_http_listen: String,
    #[arg(
        long,
        env = "KRABKA_TRACES_ZIPKIN_LISTEN",
        default_value = "127.0.0.1:9411"
    )]
    pub(crate) zipkin_listen: String,
    #[arg(
        long,
        env = "KRABKA_TRACES_BOOTSTRAP",
        default_value = "127.0.0.1:9092"
    )]
    pub(crate) bootstrap: String,
    #[arg(
        long,
        env = "KRABKA_TRACES_CLIENT_DISPATCH_QUEUE_CAPACITY",
        default_value_t = DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
        value_parser = parse_client_dispatch_queue_capacity
    )]
    pub(crate) client_dispatch_queue_capacity: usize,
    #[arg(
        long,
        env = "KRABKA_TRACES_CLIENT_FRAME_MAX",
        default_value = "100MiB",
        value_parser = parse_client_frame_max
    )]
    pub(crate) client_frame_max: ByteSize,
    #[arg(
        long,
        env = "KRABKA_TRACES_WAL_FETCH_MAX",
        default_value = "2MiB",
        value_parser = parse_consumer_fetch_size
    )]
    pub(crate) wal_fetch_max: ByteSize,
    #[arg(
        long,
        env = "KRABKA_TRACES_WAL_FETCH_PARTITION_MAX",
        default_value = "256KiB",
        value_parser = parse_consumer_fetch_size
    )]
    pub(crate) wal_fetch_partition_max: ByteSize,
    #[arg(
        long = "retention",
        visible_alias = "retention-ns",
        env = "KRABKA_TRACES_RETENTION",
        default_value = "30m",
        value_parser = parse_positive_time_or_nanos
    )]
    pub(crate) retention: Time,
    #[arg(
        long = "block-builder-window",
        visible_alias = "block-builder-window-secs",
        env = "KRABKA_TRACES_BLOCK_BUILDER_WINDOW",
        default_value = "5s",
        value_parser = parse_positive_time_or_secs
    )]
    pub(crate) block_builder_window: Time,
    #[arg(
        long,
        env = "KRABKA_TRACES_BLOCK_BUILDER_EMPTY_POLL_BACKOFF",
        default_value = "100ms",
        value_parser = parse::positive_time
    )]
    pub(crate) block_builder_empty_poll_backoff: Time,
    #[arg(
        long,
        env = "KRABKA_TRACES_BLOCK_BUILDER_FLUSH_MAX_RECORDS",
        default_value_t = krabka_traces::blockbuilder::DEFAULT_FLUSH_MAX_RECORDS,
        value_parser = parse_positive_usize
    )]
    pub(crate) block_builder_flush_max_records: usize,
    #[arg(
        long = "block-builder-flush-max-age",
        visible_alias = "block-builder-flush-max-age-ms",
        env = "KRABKA_TRACES_BLOCK_BUILDER_FLUSH_MAX_AGE",
        default_value = "10s",
        value_parser = parse_positive_time_or_millis
    )]
    pub(crate) block_builder_flush_max_age: Time,
    #[arg(long, env = "KRABKA_TRACES_QUERIER_LIVE_STORE", action = ArgAction::SetTrue)]
    pub(crate) querier_live_store: bool,
    #[arg(long, env = "KRABKA_TRACES_QUERIER_LIVE_STORE_URL")]
    pub(crate) querier_live_store_url: Option<String>,
    #[arg(
        long,
        env = "KRABKA_TRACES_TRACE_INDEX_KEY",
        default_value = "index/traces.json"
    )]
    pub(crate) trace_index_key: String,
    #[arg(
        long,
        env = "KRABKA_TRACES_INDEX_SNAPSHOT_MAX",
        default_value = "256MiB",
        value_parser = parse_positive_whole_byte_size
    )]
    pub(crate) index_snapshot_max: ByteSize,
    #[arg(
        long,
        env = "KRABKA_TRACES_INDEX_SNAPSHOT_RETAIN",
        default_value_t = IndexSnapshotRetain::default()
    )]
    pub(crate) index_snapshot_retain: IndexSnapshotRetain,
    #[arg(
        long,
        env = "KRABKA_TRACES_BLOCK_READ_MAX",
        default_value = "1GiB",
        value_parser = parse_positive_whole_byte_size
    )]
    pub(crate) block_read_max: ByteSize,
    #[arg(
        long,
        env = "KRABKA_TRACES_SCAN_CONCAT_MAX",
        default_value = "1.5GB",
        value_parser = parse_scan_concat_max
    )]
    pub(crate) scan_concat_max: ByteSize,
    #[arg(
        long,
        env = "KRABKA_TRACES_OBJECT_STORE_URL",
        default_value = "memory:///"
    )]
    pub(crate) object_store_url: String,
    #[arg(long, env = "KRABKA_TRACES_REMOTE_WRITE_URL")]
    pub(crate) remote_write_url: Option<String>,
    #[arg(
        long = "collection-interval",
        visible_alias = "collection-interval-secs",
        env = "KRABKA_TRACES_COLLECTION_INTERVAL",
        value_parser = parse_positive_time_or_secs
    )]
    pub(crate) collection_interval: Option<Time>,
    #[arg(long, env = "KRABKA_TRACES_MAX_EXEMPLARS_PER_SERIES")]
    pub(crate) max_exemplars_per_series: Option<usize>,
    #[arg(
        long = "edge-ttl",
        visible_alias = "edge-ttl-secs",
        env = "KRABKA_TRACES_EDGE_TTL",
        value_parser = parse_non_negative_time_or_secs
    )]
    pub(crate) edge_ttl: Option<Time>,
    #[arg(long, env = "KRABKA_TRACES_EDGE_STORE_MAX_ITEMS")]
    pub(crate) edge_store_max_items: Option<usize>,
    #[arg(
        long = "histogram-buckets",
        visible_alias = "histogram-buckets-ns",
        env = "KRABKA_TRACES_HISTOGRAM_BUCKETS",
        value_delimiter = ',',
        value_parser = parse_positive_time_or_nanos_f64
    )]
    pub(crate) histogram_buckets: Option<Vec<Time>>,
    #[command(flatten)]
    pub(crate) metrics: MetricsFlags,
    #[arg(
        long = "compaction-start",
        visible_alias = "compaction-start-ns",
        env = "KRABKA_TRACES_COMPACTION_START",
        default_value = "0ns",
        value_parser = parse_unix_nano
    )]
    pub(crate) compaction_start: UnixNano,
    #[arg(
        long = "compaction-end",
        visible_alias = "compaction-end-ns",
        env = "KRABKA_TRACES_COMPACTION_END",
        default_value = "max",
        value_parser = parse_unix_nano
    )]
    pub(crate) compaction_end: UnixNano,
    #[arg(
        long,
        env = "KRABKA_TRACES_QUERIER_URL",
        default_value = "http://127.0.0.1:3200"
    )]
    pub(crate) querier_url: String,
    #[arg(
        long = "live-frontier",
        visible_alias = "live-frontier-ns",
        env = "KRABKA_TRACES_LIVE_FRONTIER",
        value_parser = parse_unix_nano
    )]
    pub(crate) live_frontier: Option<UnixNano>,
    #[arg(long, env = "KRABKA_TRACES_QUERY_QUEUE_DEPTH", default_value_t = 128)]
    pub(crate) query_queue_depth: usize,
    #[arg(
        long,
        env = "KRABKA_TRACES_TARGET_BYTES_PER_JOB",
        default_value = "0B",
        value_parser = parse_non_negative_whole_byte_size_or_bytes
    )]
    pub(crate) target_bytes_per_job: ByteSize,
    #[arg(long, env = "KRABKA_TRACES_MAX_TRACE_SPANS", default_value_t = usize::MAX)]
    pub(crate) max_trace_spans: usize,
    #[arg(
        long,
        env = "KRABKA_TRACES_TAG_QUERY_FILTER_AUTOCOMPLETE_LIMIT",
        default_value_t = 25,
        value_parser = parse_positive_usize
    )]
    pub(crate) tag_query_filter_autocomplete_limit: usize,
    #[arg(
        long = "traceql-default-limit",
        env = "KRABKA_TRACES_TRACEQL_DEFAULT_LIMIT",
        default_value_t = 20,
        value_parser = parse_positive_usize
    )]
    pub(crate) traceql_default_limit: usize,
    #[arg(
        long = "traceql-default-spans-per-span-set",
        env = "KRABKA_TRACES_TRACEQL_DEFAULT_SPANS_PER_SPAN_SET",
        default_value_t = 3,
        value_parser = parse_positive_usize
    )]
    pub(crate) traceql_default_spss: usize,
    #[arg(
        long,
        env = "KRABKA_TRACES_TRACEQL_MAX_TRACES",
        default_value_t = 1000,
        value_parser = parse_positive_usize
    )]
    pub(crate) max_search_traces: usize,
    #[arg(long, env = "KRABKA_TRACES_TRACEQL_MAX_EXEMPLARS", default_value_t = 0)]
    pub(crate) max_metric_exemplars: usize,
    #[arg(
        long = "traceql-compare-max-values-per-attr",
        env = "KRABKA_TRACES_TRACEQL_COMPARE_MAX_VALUES_PER_ATTR",
        default_value_t = 256,
        value_parser = parse_positive_usize
    )]
    pub(crate) traceql_compare_max_values_per_attr: usize,
    #[arg(
        long = "traceql-histogram-buckets",
        env = "KRABKA_TRACES_TRACEQL_HISTOGRAM_BUCKETS",
        default_value = "2ms,4ms,8ms,16ms,32ms,64ms,128ms,256ms,512ms,1024ms,2048ms,4096ms,8192ms,16384ms",
        value_delimiter = ',',
        value_parser = parse::positive_time
    )]
    pub(crate) traceql_histogram_buckets: Vec<Time>,
    #[arg(
        long,
        env = "KRABKA_TRACES_MAX_SPANS_PER_REQUEST",
        default_value_t = 10_000
    )]
    pub(crate) max_spans_per_request: usize,
    #[arg(long, env = "KRABKA_TRACES_MAX_SPANS_PER_TRACE", default_value_t = usize::MAX)]
    pub(crate) max_spans_per_trace: usize,
    #[arg(long, env = "KRABKA_TRACES_MAX_INGEST_SPANS_PER_SECOND", default_value_t = usize::MAX)]
    pub(crate) max_ingest_spans_per_second: usize,
    #[arg(long, env = "KRABKA_TRACES_INGEST_RATE_BURST", default_value_t = usize::MAX)]
    pub(crate) ingest_rate_burst: usize,
    #[arg(
        long = "promote-span-attr",
        env = "KRABKA_TRACES_PROMOTE_SPAN_ATTR",
        value_delimiter = ','
    )]
    pub(crate) promote_span_attrs: Vec<String>,
    #[arg(
        long = "promote-resource-attr",
        env = "KRABKA_TRACES_PROMOTE_RESOURCE_ATTR",
        value_delimiter = ','
    )]
    pub(crate) promote_resource_attrs: Vec<String>,
    #[arg(
        long,
        env = "KRABKA_TRACES_MAX_ATTR_VALUE_LEN",
        default_value = "64KiB",
        value_parser = parse_non_negative_whole_byte_size_or_bytes
    )]
    pub(crate) max_attr_value_len: ByteSize,
    #[arg(
        long,
        env = "KRABKA_TRACES_MAX_DECOMPRESSED_BYTES",
        default_value = "10MiB",
        value_parser = parse_non_negative_whole_byte_size_or_bytes
    )]
    pub(crate) max_decompressed_bytes: ByteSize,
    #[arg(
        long,
        env = "KRABKA_TRACES_METRICS_GENERATOR_POLL_BATCH_SIZE",
        default_value_t = 1_000,
        value_parser = parse_positive_usize
    )]
    pub(crate) metrics_generator_poll_batch_size: usize,
    #[arg(
        long,
        env = "KRABKA_TRACES_METRICS_GENERATOR_POLL_ERROR_BACKOFF",
        default_value = "200ms",
        value_parser = parse::positive_time
    )]
    pub(crate) metrics_generator_poll_error_backoff: Time,
    #[arg(long, env = "KRABKA_TRACES_CONFIG")]
    pub(crate) config: Option<String>,
}
