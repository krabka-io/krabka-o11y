use super::{
    ByteSize, DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY, IndexSnapshotRetain, Parser, SocketAddr,
    Target, Time, parse, parse_client_dispatch_queue_capacity, parse_client_frame_max,
    parse_consumer_fetch_size, parse_min_two_usize, parse_non_empty_string,
    parse_positive_time_or_legacy_millis, parse_positive_time_or_legacy_nanos,
    parse_positive_usize, parse_positive_whole_byte_size,
};

#[derive(Debug, Parser)]
pub(crate) struct Cli {
    #[command(flatten)]
    pub(crate) profiling: krabka_telemetry::profiling::ProfilingConfig,
    #[arg(long, env = "KRABKA_PROFILES_TARGET")]
    pub(crate) target: Target,
    #[arg(
        long,
        env = "KRABKA_PROFILES_LISTEN_ADDR",
        default_value = "127.0.0.1:4040"
    )]
    pub(crate) listen: SocketAddr,
    #[arg(long, env = "KRABKA_ADMIN_LISTEN_ADDR", default_value = "0.0.0.0:9404")]
    pub(crate) admin_listen_addr: SocketAddr,
    #[arg(
        long,
        env = "KRABKA_PROFILES_BOOTSTRAP",
        default_value = "127.0.0.1:9092"
    )]
    pub(crate) bootstrap: String,
    #[arg(
        long,
        env = "KRABKA_PROFILES_WAL_TOPIC",
        default_value = krabka_profiles::PROFILES_WAL_TOPIC,
        value_parser = parse_non_empty_string
    )]
    pub(crate) wal_topic: String,
    #[arg(
        long,
        env = "KRABKA_PROFILES_BLOCK_BUILDER_GROUP_ID",
        default_value = "krabka-profiles-block-builder",
        value_parser = parse_non_empty_string
    )]
    pub(crate) block_builder_group_id: String,
    #[arg(
        long,
        env = "KRABKA_PROFILES_CLIENT_DISPATCH_QUEUE_CAPACITY",
        default_value_t = DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
        value_parser = parse_client_dispatch_queue_capacity
    )]
    pub(crate) client_dispatch_queue_capacity: usize,
    #[arg(
        long,
        env = "KRABKA_PROFILES_CLIENT_FRAME_MAX",
        default_value = "100MiB",
        value_parser = parse_client_frame_max
    )]
    pub(crate) client_frame_max: ByteSize,
    #[arg(
        long,
        env = "KRABKA_PROFILES_DISTRIBUTOR_REQUEST_MAX",
        default_value = "16MiB",
        value_parser = parse_positive_whole_byte_size
    )]
    pub(crate) distributor_request_max: ByteSize,
    #[arg(
        long,
        env = "KRABKA_PROFILES_DISTRIBUTOR_MAX_TRACKED_TENANTS",
        default_value_t = 4096,
        value_parser = parse_positive_usize
    )]
    pub(crate) distributor_max_tracked_tenants: usize,
    #[arg(
        long,
        env = "KRABKA_PROFILES_LEGACY_MAX_NODES",
        default_value_t = 500_000,
        value_parser = parse_positive_usize
    )]
    pub(crate) legacy_max_nodes: usize,
    #[arg(
        long,
        env = "KRABKA_PROFILES_LEGACY_MAX_PATH_BYTES",
        default_value = "64MiB",
        value_parser = parse_positive_whole_byte_size
    )]
    pub(crate) legacy_max_path_bytes: ByteSize,
    #[arg(
        long,
        env = "KRABKA_PROFILES_LEGACY_MAX_TRIE_DEPTH",
        default_value_t = 4096,
        value_parser = parse_positive_usize
    )]
    pub(crate) legacy_max_trie_depth: usize,
    #[arg(
        long,
        env = "KRABKA_PROFILES_WAL_FETCH_MAX",
        default_value = "2MiB",
        value_parser = parse_consumer_fetch_size
    )]
    pub(crate) wal_fetch_max: ByteSize,
    #[arg(
        long,
        env = "KRABKA_PROFILES_WAL_FETCH_PARTITION_MAX",
        default_value = "256KiB",
        value_parser = parse_consumer_fetch_size
    )]
    pub(crate) wal_fetch_partition_max: ByteSize,
    #[arg(
        long,
        env = "KRABKA_PROFILES_OBJECT_STORE_URL",
        default_value = "file://./.krabka-profiles-blocks"
    )]
    pub(crate) object_store_url: String,
    #[arg(
        long,
        env = "KRABKA_PROFILES_INDEX_OBJECT_KEY",
        default_value = "index/profiles.json",
        value_parser = parse_non_empty_string
    )]
    pub(crate) index_object_key: String,
    #[arg(
        long,
        env = "KRABKA_PROFILES_INDEX_SNAPSHOT_MAX",
        default_value = "256MiB",
        value_parser = parse_positive_whole_byte_size
    )]
    pub(crate) index_snapshot_max: ByteSize,
    #[arg(
        long,
        env = "KRABKA_PROFILES_INDEX_SNAPSHOT_RETAIN",
        default_value_t = IndexSnapshotRetain::default()
    )]
    pub(crate) index_snapshot_retain: IndexSnapshotRetain,
    #[arg(
        long,
        env = "KRABKA_PROFILES_INDEX_REFRESH_INTERVAL",
        default_value = "15s",
        value_parser = parse::positive_time
    )]
    pub(crate) index_refresh_interval: Time,
    #[arg(
        long,
        env = "KRABKA_PROFILES_WAL_POLL_TIMEOUT",
        default_value = "500ms",
        value_parser = parse::positive_time
    )]
    pub(crate) wal_poll_timeout: Time,
    #[arg(
        long,
        env = "KRABKA_PROFILES_HOT_STORE_MAX_AGE",
        default_value = "6h",
        value_parser = parse::positive_time
    )]
    pub(crate) hot_store_max_age: Time,
    #[arg(
        long,
        env = "KRABKA_PROFILES_HOT_STORE_MAX_RECORDS",
        default_value_t = 1_000_000,
        value_parser = parse_positive_usize
    )]
    pub(crate) hot_store_max_records: usize,
    #[arg(
        long,
        env = "KRABKA_PROFILES_HEATMAP_VALUE_BUCKETS",
        default_value_t = 32,
        value_parser = parse_positive_usize
    )]
    pub(crate) heatmap_value_buckets: usize,
    #[arg(
        long,
        env = "KRABKA_PROFILES_HEATMAP_TIME_BUCKETS_MAX",
        default_value_t = 4096,
        value_parser = parse_positive_usize
    )]
    pub(crate) heatmap_time_buckets_max: usize,
    #[arg(
        long = "query-frontend-shard-width",
        visible_alias = "query-frontend-shard-ms",
        env = "KRABKA_PROFILES_QUERY_FRONTEND_SHARD_WIDTH",
        default_value = "15m",
        value_parser = parse_positive_time_or_legacy_millis
    )]
    pub(crate) query_frontend_shard_width: Time,
    #[arg(long, env = "KRABKA_PROFILES_TENANT_LIMITS_CONFIG")]
    pub(crate) tenant_limits_config: Option<std::path::PathBuf>,
    #[arg(long, env = "KRABKA_PROFILES_LIMITS_OVERRIDES_CONFIG")]
    pub(crate) profiles_limits_overrides_config: Option<std::path::PathBuf>,
    #[arg(
        long,
        env = "KRABKA_PROFILES_QUERY_WAL_TAIL_GROUP_ID",
        default_value = "krabka-profiles-query-wal-tail",
        value_parser = parse_non_empty_string
    )]
    pub(crate) query_wal_tail_group_id: String,
    #[arg(long, env = "KRABKA_PROFILES_COMPACTOR_MAX_BLOCKS_PER_JOB", default_value_t = 8, value_parser = parse_min_two_usize)]
    pub(crate) compactor_max_blocks_per_job: usize,
    #[arg(
        long = "compactor-downsample-resolution",
        visible_alias = "compactor-downsample-resolution-ns",
        env = "KRABKA_PROFILES_COMPACTOR_DOWNSAMPLE_RESOLUTION",
        value_parser = parse_positive_time_or_legacy_nanos
    )]
    pub(crate) compactor_downsample_resolution: Option<Time>,
    #[arg(long, env = "KRABKA_PROFILES_BLOCK_BUILDER_FLUSH_RECORDS", default_value_t = krabka_profiles::blockbuilder::DEFAULT_FLUSH_RECORDS, value_parser = parse_positive_usize)]
    pub(crate) block_builder_flush_records: usize,
    #[arg(
        long = "block-builder-flush-max-age",
        visible_alias = "block-builder-flush-max-age-ms",
        env = "KRABKA_PROFILES_BLOCK_BUILDER_FLUSH_MAX_AGE",
        default_value = "10s",
        value_parser = parse_positive_time_or_legacy_millis
    )]
    pub(crate) block_builder_flush_max_age: Time,
    /// debuginfod base URLs, comma-separated, to fetch DWARF for unsymbolized
    /// native frames. Empty by default: the symbolizer makes NO outbound
    /// requests until an operator supplies URLs.
    #[arg(
        long = "debuginfod-url",
        env = "KRABKA_PROFILES_DEBUGINFOD_URLS",
        value_delimiter = ','
    )]
    pub(crate) debuginfod_urls: Vec<String>,
    #[arg(
        long,
        env = "KRABKA_PROFILES_DEBUGINFOD_MAX_ARTIFACT_SIZE",
        value_parser = parse_positive_whole_byte_size
    )]
    pub(crate) debuginfod_max_artifact_size: Option<ByteSize>,
    #[arg(
        long,
        env = "KRABKA_PROFILES_DEBUGINFOD_CONNECT_TIMEOUT",
        value_parser = parse::positive_time
    )]
    pub(crate) debuginfod_connect_timeout: Option<Time>,
    #[arg(
        long,
        env = "KRABKA_PROFILES_DEBUGINFOD_REQUEST_TIMEOUT",
        value_parser = parse::positive_time
    )]
    pub(crate) debuginfod_request_timeout: Option<Time>,
}
