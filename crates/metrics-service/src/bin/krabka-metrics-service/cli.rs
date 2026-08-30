use super::{
    ByteSize, DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY, Parser, PathBuf, RULER_STATE_TOPIC,
    SocketAddr, Target, Time, WAL_TOPIC, parse, parse_client_dispatch_queue_capacity,
    parse_client_frame_max, parse_positive_usize, parse_remote_read_max_body,
};

#[derive(Debug, Parser)]
pub(crate) struct Cli {
    #[command(flatten)]
    pub(crate) profiling: krabka_telemetry::profiling::ProfilingConfig,
    #[arg(long, env = "KRABKA_METRICS_SERVICE_TARGET")]
    pub(crate) target: Target,
    #[arg(
        long,
        env = "KRABKA_METRICS_SERVICE_LISTEN",
        default_value = "127.0.0.1:4041"
    )]
    pub(crate) listen: SocketAddr,
    #[arg(
        long,
        env = "KRABKA_METRICS_SERVICE_CLIENT_DISPATCH_QUEUE_CAPACITY",
        default_value_t = DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
        value_parser = parse_client_dispatch_queue_capacity
    )]
    pub(crate) client_dispatch_queue_capacity: usize,
    #[arg(
        long,
        env = "KRABKA_METRICS_SERVICE_CLIENT_FRAME_MAX",
        default_value = "100MiB",
        value_parser = parse_client_frame_max
    )]
    pub(crate) client_frame_max: ByteSize,
    #[arg(
        long,
        env = "KRABKA_METRICS_OBJECT_STORE_URL",
        default_value = "file://./.krabka-metrics-blocks"
    )]
    pub(crate) object_store_url: String,
    #[arg(
        long,
        env = "KRABKA_METRICS_MANIFEST_PREFIX",
        default_value = "metrics"
    )]
    pub(crate) manifest_prefix: String,
    #[arg(
        long,
        env = "KRABKA_METRICS_COLD_CACHE_TTL",
        default_value = "30s",
        value_parser = parse::positive_time
    )]
    pub(crate) cold_cache_ttl: Time,
    #[arg(
        long,
        env = "KRABKA_METRICS_UNBOUNDED_COMPATIBILITY_LOOKBACK",
        default_value = "1h",
        value_parser = parse::positive_time
    )]
    pub(crate) unbounded_compatibility_lookback: Time,
    #[arg(long, env = "KRABKA_METRICS_RUNTIME_OVERRIDES")]
    pub(crate) runtime_overrides: Option<PathBuf>,
    #[arg(
        long,
        env = "KRABKA_METRICS_QUERY_FRONTEND_SPLIT",
        default_value = "60s",
        value_parser = parse::positive_time
    )]
    pub(crate) query_frontend_split: Time,
    #[arg(
        long,
        env = "KRABKA_METRICS_QUERY_FRONTEND_SHARDS",
        default_value_t = 1
    )]
    pub(crate) query_frontend_shards: usize,
    #[arg(
        long,
        env = "KRABKA_METRICS_MAX_CONCURRENT_QUERIES",
        default_value_t = 2
    )]
    pub(crate) max_concurrent_queries: usize,
    #[arg(
        long = "query-lookback-delta",
        env = "KRABKA_METRICS_QUERY_LOOKBACK_DELTA",
        default_value = "5m",
        value_parser = parse::positive_time
    )]
    pub(crate) query_lookback_delta: Time,
    #[arg(
        long = "query-eval-interval",
        env = "KRABKA_METRICS_QUERY_EVAL_INTERVAL",
        default_value = "1m",
        value_parser = parse::positive_time
    )]
    pub(crate) query_eval_interval: Time,
    #[arg(
        long = "query-max-samples",
        env = "KRABKA_METRICS_QUERY_MAX_SAMPLES",
        default_value_t = 50_000_000,
        value_parser = parse_positive_usize
    )]
    pub(crate) query_max_samples: usize,
    #[arg(
        long = "remote-read-max-body",
        env = "KRABKA_METRICS_REMOTE_READ_MAX_BODY",
        default_value = "64MiB",
        value_parser = parse_remote_read_max_body
    )]
    pub(crate) remote_read_max_body: ByteSize,
    #[arg(
        long,
        env = "KRABKA_METRICS_QUERY_FRONTEND_CACHE_PREFIX",
        default_value = "metrics-query-cache"
    )]
    pub(crate) query_frontend_cache_prefix: String,
    #[arg(long, env = "KRABKA_METRICS_RULER_TENANT", default_value = "anonymous")]
    pub(crate) ruler_tenant: String,
    #[arg(
        long,
        env = "KRABKA_METRICS_RULER_EVAL_INTERVAL",
        default_value = "60s",
        value_parser = parse::positive_time
    )]
    pub(crate) ruler_eval_interval: Time,
    #[arg(long, env = "KRABKA_METRICS_RULER_SHARD_INDEX", default_value_t = 1)]
    pub(crate) ruler_shard_index: usize,
    #[arg(long, env = "KRABKA_METRICS_RULER_SHARD_TOTAL", default_value_t = 1)]
    pub(crate) ruler_shard_total: usize,
    #[arg(long, env = "KRABKA_METRICS_RULER_ALERTMANAGER_URL")]
    pub(crate) ruler_alertmanager_url: Option<String>,
    /// A Prometheus rule file the ruler installs at startup.
    ///
    /// The ruler posts each group of the file to its own ruler-config API, so a
    /// bundled group and a group an operator posts behave the same way. The
    /// start stops when the ruler cannot read, parse, or install the file.
    #[arg(long, env = "KRABKA_METRICS_RULER_BUNDLED_RULES")]
    pub(crate) ruler_bundled_rules: Option<PathBuf>,
    #[arg(
        long,
        env = "KRABKA_METRICS_RULER_STATE_TOPIC",
        default_value = RULER_STATE_TOPIC
    )]
    pub(crate) ruler_state_topic: String,
    #[arg(long, env = "KRABKA_METRICS_WAL_BOOTSTRAP")]
    pub(crate) wal_bootstrap: Option<String>,
    #[arg(
        long,
        env = "KRABKA_METRICS_WAL_GROUP_ID",
        default_value = "krabka-metrics-querier"
    )]
    pub(crate) wal_group_id: String,
    #[arg(
        long,
        env = "KRABKA_METRICS_WAL_CLIENT_ID",
        default_value = "krabka-metrics-querier"
    )]
    pub(crate) wal_client_id: String,
    #[arg(
        long,
        env = "KRABKA_METRICS_WAL_TOPIC",
        default_value = WAL_TOPIC
    )]
    pub(crate) wal_topic: String,
    #[arg(
        long,
        env = "KRABKA_METRICS_WAL_POLL_TIMEOUT",
        default_value = "500ms",
        value_parser = parse::positive_time
    )]
    pub(crate) wal_poll_timeout: Time,
    /// How far back the in-memory WAL head keeps samples.
    #[arg(
        long,
        env = "KRABKA_METRICS_QUERIER_WAL_HEAD_RETENTION",
        default_value = "5m",
        value_parser = parse::positive_time
    )]
    pub(crate) wal_head_retention: Time,
}
