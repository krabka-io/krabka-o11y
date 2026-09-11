use super::{
    AuditArgs, ByteSize, ConfigFileArgs, DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
    ExternalLabels, Parser, PathBuf, RULER_STATE_TOPIC, ServerSecurityArgs, SocketAddr, Target,
    TenantId, Time, WAL_TOPIC, WalClientSecurityArgs, parse, parse_client_dispatch_queue_capacity,
    parse_client_frame_max, parse_external_label, parse_external_labels_env, parse_positive_usize,
    parse_remote_read_max_body,
};

#[derive(Debug, Parser)]
pub(crate) struct Cli {
    #[command(flatten)]
    pub(crate) config_file: ConfigFileArgs,
    #[command(flatten)]
    pub(crate) profiling: krabka_telemetry::profiling::ProfilingConfig,
    #[arg(long, env = "KRABKA_METRICS_SERVICE_TARGET")]
    pub(crate) target: Target,
    /// Address for the admin port: pprof, Prometheus metrics and `/ready`. Default: `0.0.0.0:9404`.
    ///
    /// The admin port always serves plain HTTP with no authentication. The
    /// TLS and credential flags apply only to the data port. Bind the admin
    /// port to an address that only the cluster can reach.
    #[arg(long, env = "KRABKA_ADMIN_LISTEN_ADDR", default_value = "0.0.0.0:9404")]
    pub(crate) admin_listen_addr: SocketAddr,
    /// HTTP query listen address. Default: `0.0.0.0:4041`.
    ///
    /// Every interface, as Prometheus and Mimir default to. A container that
    /// binds loopback is unreachable from outside its pod, and the only
    /// symptom is a health check timing out with nothing in the logs.
    #[arg(
        long,
        env = "KRABKA_METRICS_SERVICE_LISTEN",
        default_value = "0.0.0.0:4041"
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
    pub(crate) ruler_tenant: TenantId,
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
    #[arg(
        long,
        env = "KRABKA_METRICS_RULER_ALERTMANAGER_URL",
        value_delimiter = ','
    )]
    pub(crate) ruler_alertmanager_url: Vec<String>,
    /// Maximum alert batches buffered while Alertmanager is unavailable.
    #[arg(
        long,
        env = "KRABKA_METRICS_RULER_ALERTMANAGER_QUEUE_CAPACITY",
        default_value_t = 64,
        value_parser = parse_positive_usize
    )]
    pub(crate) ruler_alertmanager_queue_capacity: usize,
    /// Label in `name=value` form added when an alert does not define it.
    #[arg(long, value_parser = parse_external_label)]
    pub(crate) ruler_external_label: Vec<(String, String)>,
    /// External labels from a JSON array in the environment.
    #[arg(
        long = "ruler-external-label-env",
        env = "KRABKA_METRICS_RULER_EXTERNAL_LABEL",
        hide = true,
        value_parser = parse_external_labels_env
    )]
    pub(crate) ruler_external_label_env: Option<ExternalLabels>,
    /// Generator URL template. `{alertname}` expands to the outgoing alert name.
    #[arg(long, env = "KRABKA_METRICS_RULER_GENERATOR_URL_TEMPLATE")]
    pub(crate) ruler_generator_url_template: Option<String>,
    /// A Prometheus rule file the ruler installs at startup.
    ///
    /// The ruler posts each group of the file over HTTP to its own
    /// ruler-config API at `localhost`, so a bundled group and a group an
    /// operator posts behave the same way. The request carries the
    /// `--internal-client-*` credentials. With authentication on, they should
    /// name a principal that may use `--ruler-tenant`. With TLS on, the server
    /// certificate should be valid for `localhost`. The start stops when the
    /// ruler cannot read, parse, or install the file.
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
    /// The Kafka consumer group the metrics querier's WAL head reader joins.
    ///
    /// This names the group. It does not scale the write path. See
    /// `krabka_observability::wal_group_assignment` for what a change of a
    /// group's membership costs.
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
    // The shared security flags come after this binary's own flags, so
    // `--help` lists the role and its listener first.
    /// TLS, authentication and outbound-credential flags for the data port.
    #[command(flatten)]
    pub(crate) server_security: ServerSecurityArgs,
    /// Audit trail flags. The audit layer is off until `--audit-topic` is set.
    #[command(flatten)]
    pub(crate) audit: AuditArgs,
    /// TLS and SASL flags for every broker connection, the audit producer included.
    #[command(flatten)]
    pub(crate) wal_security: WalClientSecurityArgs,
}
