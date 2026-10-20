use krabka_observability::cli_value_parsers::{
    parse_client_dispatch_queue_capacity, parse_client_frame_max, parse_min_two_usize,
    parse_positive_u32,
};

use super::{
    ByteSize, ConfigFileArgs, DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
    DEFAULT_MAX_BLOCKS_PER_JOB, DEFAULT_MAX_LEVEL, DEFAULT_MAX_RATE_BUCKETS,
    DEFAULT_TARGET_ROWS_PER_BLOCK, HA_TRACKER_TOPIC, Parser, PathBuf, SocketAddr, Time, parse,
    parse_compactor_target_rows, parse_distributor_max_decompressed, parse_ingest_rate_bucket_cap,
};

/// Writer options shared by standalone and all-in-one metrics roles.
#[derive(Clone, Debug, Parser)]
pub struct WriterConfig {
    #[command(flatten)]
    pub(crate) config_file: ConfigFileArgs,
    /// HTTP ingest listen address. Default: `0.0.0.0:4041`.
    ///
    /// Every interface, as Prometheus and Mimir default to. A container that
    /// binds loopback is unreachable from outside its pod, and the only
    /// symptom is a health check timing out with nothing in the logs.
    #[arg(long, env = "KRABKA_METRICS_LISTEN", default_value = "0.0.0.0:4041")]
    pub(crate) listen: SocketAddr,
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
    /// The Kafka consumer group the metrics block builder joins.
    ///
    /// Members divide the WAL topic's partitions. Revoked, uncommitted buffers
    /// are replayed by their new owner.
    #[arg(
        long,
        env = "KRABKA_METRICS_BLOCK_BUILDER_GROUP_ID",
        default_value = "krabka-metrics-block-builder"
    )]
    pub(crate) block_builder_group_id: String,
    #[arg(
        long,
        env = "KRABKA_METRICS_BLOCK_BUILDER_CLIENT_ID",
        default_value = "krabka-metrics-block-builder"
    )]
    pub(crate) block_builder_client_id: String,
    #[arg(
        long,
        env = "KRABKA_METRICS_BLOCK_BUILDER_POLL_TIMEOUT",
        default_value = "1s",
        value_parser = parse::positive_time
    )]
    pub(crate) block_builder_poll_timeout: Time,
    /// Flush the accumulated block buffer once this many WAL records are
    /// buffered.
    #[arg(
        long,
        env = "KRABKA_METRICS_BLOCK_BUILDER_FLUSH_MAX_ROWS",
        default_value_t = crate::DEFAULT_FLUSH_MAX_ROWS
    )]
    pub(crate) block_builder_flush_max_rows: usize,
    /// Flush the accumulated block buffer once its oldest record reaches this
    /// age.
    #[arg(
        long,
        env = "KRABKA_METRICS_BLOCK_BUILDER_FLUSH_MAX_AGE",
        default_value = "1m",
        value_parser = parse::positive_time
    )]
    pub(crate) block_builder_flush_max_age: Time,
    /// How often the block builder sweeps object-store blocks and indexes for
    /// retention.
    ///
    /// The window itself is per tenant, and it is the
    /// `compactor_blocks_retention_period` limit in the file
    /// `--runtime-overrides` names. With no window set anywhere the sweep still
    /// runs and expires nothing.
    ///
    /// The same pass also deletes the objects that no index manifest names. A
    /// write that put its block and never published the manifest leaves one,
    /// and so does a merge whose inputs the `compactor` role retired.
    #[arg(
        long,
        env = "KRABKA_METRICS_BLOCK_BUILDER_RETENTION_SWEEP_INTERVAL",
        default_value = "1m",
        value_parser = parse::positive_time
    )]
    pub(crate) block_builder_retention_sweep_interval: Time,
    /// Blocks one compaction job merges at most.
    ///
    /// A job needs at least two, and a larger cap means fewer, larger output
    /// blocks per pass at the cost of holding more inputs open at once. A job
    /// of one block rewrites that block and leaves the same rows behind, so
    /// the flag refuses one rather than letting
    /// [`CompactionPolicy`](krabka_blockstore::CompactionPolicy) clamp it.
    #[arg(
        long,
        env = "KRABKA_METRICS_COMPACTOR_MAX_BLOCKS_PER_JOB",
        default_value_t = DEFAULT_MAX_BLOCKS_PER_JOB,
        value_parser = parse_min_two_usize
    )]
    pub(crate) compactor_max_blocks_per_job: usize,
    /// Rows at which a block is large enough to be left alone.
    ///
    /// A block this large is never a compaction input again, whatever its
    /// level, so the bytes that cost the most to move are moved once.
    #[arg(
        long,
        env = "KRABKA_METRICS_COMPACTOR_TARGET_ROWS",
        default_value_t = DEFAULT_TARGET_ROWS_PER_BLOCK,
        value_parser = parse_compactor_target_rows
    )]
    pub(crate) compactor_target_rows: usize,
    /// How many times the same rows may be rewritten.
    ///
    /// A block at this level is never a compaction input again, which is what
    /// makes the ladder terminate. The flag refuses zero: a ladder of zero
    /// levels leaves every block at its cap, so the compactor merges nothing.
    #[arg(
        long,
        env = "KRABKA_METRICS_COMPACTOR_MAX_LEVEL",
        default_value_t = DEFAULT_MAX_LEVEL.get(),
        value_parser = parse_positive_u32
    )]
    pub(crate) compactor_max_level: u32,
    /// The level-zero grouping window. Blocks meet only inside one window.
    ///
    /// The window doubles with each level, so the ladder widens as it climbs.
    #[arg(
        long,
        env = "KRABKA_METRICS_COMPACTOR_LEVEL_WINDOW",
        default_value = "2h",
        value_parser = parse::positive_time
    )]
    pub(crate) compactor_level_window: Time,
    /// How often the compactor plans and applies one pass.
    ///
    /// A pass that plans nothing costs one listing of the manifests.
    #[arg(
        long,
        env = "KRABKA_METRICS_COMPACTOR_INTERVAL",
        default_value = "5m",
        value_parser = parse::positive_time
    )]
    pub(crate) compactor_interval: Time,
    #[arg(
        long,
        env = "KRABKA_METRICS_HA_TRACKER_TOPIC",
        default_value = HA_TRACKER_TOPIC
    )]
    pub(crate) ha_tracker_topic: String,
    /// The Kafka consumer group the HA tracker joins.
    ///
    /// This names the group. It does not scale the write path. See
    /// `krabka_observability::wal_group_assignment` for what a change of a
    /// group's membership costs.
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
    /// Additional OTLP resource attributes to copy onto every translated
    /// metric series. Service identity is promoted to job and instance
    /// without being listed here; all resource attributes remain on
    /// `target_info`.
    #[arg(
        long = "distributor.otel-promote-resource-attributes",
        env = "KRABKA_METRICS_DISTRIBUTOR_OTEL_PROMOTE_RESOURCE_ATTRIBUTES",
        value_delimiter = ','
    )]
    pub(crate) distributor_otel_promote_resource_attributes: Vec<String>,
    /// Mimir-style runtime overrides file, which sets the per-tenant limits.
    ///
    /// Without one, every tenant gets the built-in defaults, and the built-in
    /// block retention window is zero: the block builder then keeps every
    /// block forever. The file names the same keys `krabka-metrics-service`
    /// reads, so one file serves the write path, the read path and the
    /// retention sweep.
    #[arg(long, env = "KRABKA_METRICS_RUNTIME_OVERRIDES")]
    pub(crate) runtime_overrides: Option<PathBuf>,
}

impl WriterConfig {
    /// Loads the writer option file through the actual writer CLI parser.
    ///
    /// Process security, audit, profiling and admin options belong to the caller.
    /// # Errors
    /// Returns an error for an unreadable file or an invalid writer option.
    pub fn from_file(
        path: &std::path::Path,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let argv = krabka_observability::argv_with_config_file::<Self>([
            std::ffi::OsString::from("krabka-metrics"),
            std::ffi::OsString::from("--config.file"),
            path.as_os_str().to_owned(),
        ])?;
        Ok(Self::try_parse_from(argv)?)
    }

    /// The broker this writer reaches.
    #[must_use]
    pub fn bootstrap(&self) -> &str {
        &self.bootstrap
    }

    /// The durable block builder's consumer group.
    #[must_use]
    pub fn block_builder_group_id(&self) -> &str {
        &self.block_builder_group_id
    }

    /// The object storage shared by the writer and query roles.
    #[must_use]
    pub fn object_store_url(&self) -> &str {
        &self.object_store_url
    }

    /// The distributor's data listener.
    #[must_use]
    pub fn listen(&self) -> SocketAddr {
        self.listen
    }

    /// The tenant policy file shared by all metrics roles.
    #[must_use]
    pub fn runtime_overrides(&self) -> Option<&std::path::Path> {
        self.runtime_overrides.as_deref()
    }
}
