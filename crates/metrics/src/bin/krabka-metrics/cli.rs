use super::{ByteSize, DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY, DEFAULT_MAX_RATE_BUCKETS, HA_TRACKER_TOPIC, Parser, SocketAddr, Target, Time, parse, parse_client_dispatch_queue_capacity, parse_client_frame_max, parse_distributor_max_decompressed, parse_ingest_rate_bucket_cap};

#[derive(Debug, Parser)]
pub(crate) struct Cli {
    #[command(flatten)]
    pub(crate) profiling: krabka_telemetry::profiling::ProfilingConfig,
    #[arg(long, env = "KRABKA_METRICS_TARGET")]
    pub(crate) target: Target,
    #[arg(long, env = "KRABKA_METRICS_LISTEN", default_value = "127.0.0.1:4041")]
    pub(crate) listen: SocketAddr,
    #[arg(long, env = "KRABKA_ADMIN_LISTEN_ADDR", default_value = "0.0.0.0:9404")]
    pub(crate) admin_listen_addr: SocketAddr,
    #[arg(
        long,
        env = "KRABKA_METRICS_BOOTSTRAP",
        default_value = "127.0.0.1:9092"
    )]
    pub(crate) bootstrap: String,
    #[arg(
        long,
        env = "KRABKA_METRICS_CLIENT_DISPATCH_QUEUE_CAPACITY",
        default_value_t = DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
        value_parser = parse_client_dispatch_queue_capacity
    )]
    pub(crate) client_dispatch_queue_capacity: usize,
    #[arg(
        long,
        env = "KRABKA_METRICS_CLIENT_FRAME_MAX",
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
        env = "KRABKA_METRICS_COMPACTOR_GROUP_ID",
        default_value = "krabka-metrics-compactor"
    )]
    pub(crate) compactor_group_id: String,
    #[arg(
        long,
        env = "KRABKA_METRICS_COMPACTOR_CLIENT_ID",
        default_value = "krabka-metrics-compactor"
    )]
    pub(crate) compactor_client_id: String,
    #[arg(
        long,
        env = "KRABKA_METRICS_COMPACTOR_POLL_TIMEOUT",
        default_value = "1s",
        value_parser = parse::positive_time
    )]
    pub(crate) compactor_poll_timeout: Time,
    /// Flush the accumulated compaction buffer once this many WAL records are
    /// buffered.
    #[arg(
        long,
        env = "KRABKA_METRICS_COMPACTOR_FLUSH_MAX_ROWS",
        default_value_t = krabka_metrics::DEFAULT_FLUSH_MAX_ROWS
    )]
    pub(crate) compactor_flush_max_rows: usize,
    /// Flush the accumulated compaction buffer once its oldest record reaches
    /// this age.
    #[arg(
        long,
        env = "KRABKA_METRICS_COMPACTOR_FLUSH_MAX_AGE",
        default_value = "1m",
        value_parser = parse::positive_time
    )]
    pub(crate) compactor_flush_max_age: Time,
    /// Delete compacted metric blocks older than this window. Zero turns
    /// retention off.
    #[arg(
        long,
        env = "KRABKA_METRICS_COMPACTOR_RETENTION",
        default_value = "0s",
        value_parser = parse::non_negative_time
    )]
    pub(crate) compactor_retention: Time,
    /// How often the compactor sweeps object-store blocks and indexes for
    /// retention.
    #[arg(
        long,
        env = "KRABKA_METRICS_COMPACTOR_RETENTION_SWEEP_INTERVAL",
        default_value = "1m",
        value_parser = parse::positive_time
    )]
    pub(crate) compactor_retention_sweep_interval: Time,
    #[arg(
        long,
        env = "KRABKA_METRICS_HA_TRACKER_TOPIC",
        default_value = HA_TRACKER_TOPIC
    )]
    pub(crate) ha_tracker_topic: String,
    #[arg(
        long,
        env = "KRABKA_METRICS_HA_TRACKER_GROUP_ID",
        default_value = "krabka-metrics-ha-tracker"
    )]
    pub(crate) ha_tracker_group_id: String,
    #[arg(
        long,
        env = "KRABKA_METRICS_HA_TRACKER_CLIENT_ID",
        default_value = "krabka-metrics-ha-tracker"
    )]
    pub(crate) ha_tracker_client_id: String,
    #[arg(
        long,
        env = "KRABKA_METRICS_HA_TRACKER_POLL_TIMEOUT",
        default_value = "500ms",
        value_parser = parse::positive_time
    )]
    pub(crate) ha_tracker_poll_timeout: Time,
    #[arg(
        long,
        env = "KRABKA_METRICS_HA_FAILOVER_TIMEOUT",
        default_value = "30s",
        value_parser = parse::time,
        allow_hyphen_values = true
    )]
    pub(crate) ha_failover_timeout: Time,
    #[arg(
        long,
        env = "KRABKA_METRICS_INGEST_RATE_BUCKET_CAP",
        default_value_t = DEFAULT_MAX_RATE_BUCKETS,
        value_parser = parse_ingest_rate_bucket_cap
    )]
    pub(crate) ingest_rate_bucket_cap: usize,
    #[arg(
        long,
        env = "KRABKA_METRICS_DISTRIBUTOR_MAX_DECOMPRESSED",
        default_value = "32MiB",
        value_parser = parse_distributor_max_decompressed
    )]
    pub(crate) distributor_max_decompressed: ByteSize,
}
