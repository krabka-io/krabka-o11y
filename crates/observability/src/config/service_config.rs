use super::{
    AuditArgs, ByteSize, NonZeroUsize, Parser, PathBuf, QuerierIndexSource, Role,
    ServerSecurityArgs, SocketAddr, Time, WalClientSecurityArgs, days, millis, minutes, secs,
};

/// Operator-facing service configuration.
///
/// It is not `Eq`. The quantity-typed limits store `f64`, and nothing in the
/// workspace compares two configs for total equality.
#[derive(Clone, Debug, Parser, PartialEq)]
#[command(name = "krabka-observability")]
pub struct ServiceConfig {
    #[arg(long, env = "KRABKA_OBSERVABILITY_TARGET", value_enum)]
    pub target: Role,

    /// HTTP query and ingest listen address. Default: `0.0.0.0:3100`.
    ///
    /// Every interface, as `Loki`'s own default is. A container that binds
    /// loopback is unreachable from outside its pod, and the only symptom is
    /// a health check timing out with nothing in the logs.
    #[arg(
        long,
        env = "KRABKA_OBSERVABILITY_LISTEN_ADDR",
        default_value = "0.0.0.0:3100"
    )]
    pub listen_addr: SocketAddr,

    #[arg(long, env = "KRABKA_OBSERVABILITY_OBJECT_STORE_URL")]
    pub object_store_url: Option<String>,

    #[arg(long, env = "KRABKA_OBSERVABILITY_WAL_BOOTSTRAP_SERVER")]
    pub wal_bootstrap_server: Option<String>,

    #[arg(
        long,
        env = "KRABKA_OBSERVABILITY_WAL_TOPIC",
        default_value = "__krabka_observability_logs_wal"
    )]
    pub wal_topic: String,

    /// The Kafka consumer group the logs compactor joins.
    ///
    /// This names the group. It does not scale the write path. The compactor
    /// buffers WAL records across polls, and the group abandons that buffer for
    /// every partition it moves, so the group's membership should not change
    /// while it runs. Set --wal-topic's partition count to shard the write
    /// path. See `krabka_observability::wal_group_assignment`.
    #[arg(
        long,
        env = "KRABKA_OBSERVABILITY_WAL_GROUP_ID",
        default_value = "krabka-observability-block-builder"
    )]
    pub wal_group_id: String,

    #[arg(long, env = "KRABKA_OBSERVABILITY_DATA_ROOT", default_value = ".")]
    pub data_root: PathBuf,

    #[arg(
        long,
        env = "KRABKA_OBSERVABILITY_QUERIER_INDEX_SOURCE",
        value_enum,
        default_value = "local-manifest"
    )]
    pub querier_index_source: QuerierIndexSource,

    #[arg(long, env = "KRABKA_OBSERVABILITY_TENANT")]
    pub tenant: Option<String>,

    #[arg(long, env = "KRABKA_OBSERVABILITY_INDEX_PREFIX")]
    pub index_prefix: Option<String>,

    #[arg(long, env = "KRABKA_OBSERVABILITY_QUERY_START_NS")]
    pub query_start_ns: Option<i64>,

    #[arg(long, env = "KRABKA_OBSERVABILITY_QUERY_END_NS")]
    pub query_end_ns: Option<i64>,

    /// Widest `[start, end]` window a query may span, as `1h` / `30s`.
    #[arg(
        long,
        env = "KRABKA_OBSERVABILITY_MAX_QUERY_RANGE",
        value_parser = krabka_units::parse::non_negative_time
    )]
    pub max_query_range: Option<Time>,

    /// Ceiling on the number of series a query may match. A count, not a volume.
    #[arg(long, env = "KRABKA_OBSERVABILITY_MAX_QUERY_SERIES")]
    pub max_query_series: Option<usize>,

    /// Ceiling on the summed size of the blocks a query plans to read, as
    /// `512MiB`.
    #[arg(
        long,
        env = "KRABKA_OBSERVABILITY_MAX_QUERY_READ",
        value_parser = krabka_units::parse::non_negative_byte_size
    )]
    pub max_query_read: Option<ByteSize>,

    /// Ceiling on the length of the `LogQL` query string, as `4KiB`.
    ///
    /// Not `--max-query-length`: `Loki` gives that key to the `[start, end]`
    /// window a query may cover, and this one counts bytes of query text.
    #[arg(
        long,
        env = "KRABKA_OBSERVABILITY_MAX_QUERY_STRING_BYTES",
        value_parser = krabka_units::parse::non_negative_byte_size
    )]
    pub max_query_string_bytes: Option<ByteSize>,

    /// Runtime per-tenant limits overrides, as a path to a YAML file.
    ///
    /// The file holds a `defaults` block and an `overrides` map keyed by
    /// tenant. Both merge over the values the scalar limit flags set. Default:
    /// unset, so every tenant gets the same limits.
    #[arg(long, env = "KRABKA_OBSERVABILITY_LOGS_LIMITS_OVERRIDES_CONFIG")]
    pub logs_limits_overrides_config: Option<PathBuf>,

    /// Largest accepted ingest request body, as `4MiB`.
    #[arg(
        long,
        env = "KRABKA_OBSERVABILITY_MAX_INGEST_BODY",
        value_parser = krabka_units::parse::non_negative_byte_size
    )]
    pub max_ingest_body: Option<ByteSize>,

    /// How long a WAL append may take before the push is failed, as `250ms`.
    #[arg(
        long,
        env = "KRABKA_OBSERVABILITY_WAL_APPEND_TIMEOUT",
        value_parser = krabka_units::parse::non_negative_time
    )]
    pub wal_append_timeout: Option<Time>,

    #[arg(long, env = "KRABKA_OBSERVABILITY_REJECT_OLD_SAMPLES_MAX_AGE", default_value = "7d", value_parser = krabka_units::parse::positive_time)]
    pub reject_old_samples_max_age: Time,

    #[arg(long, env = "KRABKA_OBSERVABILITY_CREATION_GRACE_PERIOD", default_value = "10m", value_parser = krabka_units::parse::positive_time)]
    pub creation_grace_period: Time,

    #[arg(long, env = "KRABKA_OBSERVABILITY_INGEST_QUOTA_BURST_WINDOW", default_value = "1s", value_parser = krabka_units::parse::positive_time)]
    pub ingest_quota_burst_window: Time,

    /// How long a broker answer about ACLs or a tenant's quota is used before
    /// it is asked again, as `10s`. Default: `10s`.
    ///
    /// The query authorizer and the ingest limiter read these answers from
    /// memory. A background task refreshes the WAL topic ACLs once per TTL,
    /// and a push refreshes its tenant's quota after one TTL. An ACL change
    /// therefore takes effect within about one TTL. The default is the reload
    /// period of Loki's and Mimir's runtime configuration.
    #[arg(long, env = "KRABKA_OBSERVABILITY_BROKER_ACCESS_CACHE_TTL", default_value = "10s", value_parser = krabka_units::parse::positive_time)]
    pub broker_access_cache_ttl: Time,

    /// The oldest broker answer that a check still uses while the broker does
    /// not answer, as `1m`. Default: `1m`.
    ///
    /// While refreshes fail, the last good answer is used until it is this
    /// old, and the `query-authorization` gate of a querier or a block builder
    /// reads unmet on `/ready`. After that, every
    /// authorization and every ingest-limit check fails closed until the
    /// broker answers again. A revoked ACL is therefore in force no later than
    /// this bound, even with the broker unreachable. The default is six TTLs,
    /// so a broker restart or a controller failover does not fail every check.
    /// The service refuses a value shorter than `--broker-access-cache-ttl`.
    #[arg(long, env = "KRABKA_OBSERVABILITY_BROKER_ACCESS_MAX_STALENESS", default_value = "1m", value_parser = krabka_units::parse::positive_time)]
    pub broker_access_max_staleness: Time,

    /// The most tenants whose broker quota and ingest rate bucket are held in
    /// memory. Default: `10000`.
    ///
    /// When a new tenant arrives at the limit, the tenant that was used least
    /// recently is removed. Its next push asks the broker again and starts
    /// with a full rate bucket.
    #[arg(
        long,
        env = "KRABKA_OBSERVABILITY_BROKER_ACCESS_TENANT_CAPACITY",
        default_value = "10000"
    )]
    pub broker_access_tenant_capacity: NonZeroUsize,

    #[arg(long, env = "KRABKA_OBSERVABILITY_WAL_CONNECT_STARTUP_DEADLINE", default_value = "2m", value_parser = krabka_units::parse::positive_time)]
    pub wal_connect_startup_deadline: Time,

    #[arg(long, env = "KRABKA_OBSERVABILITY_WAL_CONNECT_ATTEMPT_TIMEOUT", default_value = "15s", value_parser = krabka_units::parse::positive_time)]
    pub wal_connect_attempt_timeout: Time,

    #[arg(long, env = "KRABKA_OBSERVABILITY_WAL_CONNECT_INITIAL_BACKOFF", default_value = "200ms", value_parser = krabka_units::parse::positive_time)]
    pub wal_connect_initial_backoff: Time,

    #[arg(long, env = "KRABKA_OBSERVABILITY_WAL_CONNECT_MAX_BACKOFF", default_value = "2s", value_parser = krabka_units::parse::positive_time)]
    pub wal_connect_max_backoff: Time,

    #[arg(long, env = "KRABKA_OBSERVABILITY_COMPACTOR_WAL_POLL_TIMEOUT", default_value = "500ms", value_parser = krabka_units::parse::positive_time)]
    pub compactor_wal_poll_timeout: Time,

    #[arg(long, env = "KRABKA_OBSERVABILITY_COMPACTOR_ACCUMULATION_WINDOW", default_value = "2s", value_parser = krabka_units::parse::positive_time)]
    pub compactor_accumulation_window: Time,

    #[arg(long, env = "KRABKA_OBSERVABILITY_COMPACTOR_ACCUMULATION_POLL_TIMEOUT", default_value = "250ms", value_parser = krabka_units::parse::positive_time)]
    pub compactor_accumulation_poll_timeout: Time,

    #[arg(
        long,
        env = "KRABKA_OBSERVABILITY_COMPACTOR_MAX_RECORDS_PER_BATCH",
        default_value = "4096"
    )]
    pub compactor_max_records_per_batch: NonZeroUsize,

    #[arg(long, env = "KRABKA_OBSERVABILITY_COMPACTOR_IDLE_INTERVAL", default_value = "10ms", value_parser = krabka_units::parse::positive_time)]
    pub compactor_idle_interval: Time,

    /// How long one stage of a `--target all` stop may take before the next
    /// stage is asked to stop anyway.
    ///
    /// The stop is staged so that the data port drains before the block
    /// builder empties the WAL behind it. A stage that hangs would otherwise
    /// hold the whole process past an orchestrator's grace period and be
    /// killed mid-write, which is the failure the staging exists to avoid.
    #[arg(long, env = "KRABKA_OBSERVABILITY_ALL_DRAIN_STAGE_TIMEOUT", default_value = "30s", value_parser = krabka_units::parse::positive_time)]
    pub all_drain_stage_timeout: Time,

    #[arg(long, env = "KRABKA_OBSERVABILITY_COMPACTOR_OBJECT_STORE_INITIAL_BACKOFF", default_value = "10ms", value_parser = krabka_units::parse::positive_time)]
    pub compactor_object_store_initial_backoff: Time,

    #[arg(long, env = "KRABKA_OBSERVABILITY_COMPACTOR_OBJECT_STORE_MAX_BACKOFF", default_value = "500ms", value_parser = krabka_units::parse::positive_time)]
    pub compactor_object_store_max_backoff: Time,

    #[arg(long, env = "KRABKA_OBSERVABILITY_QUERIER_FRONTIER_REFRESH_INTERVAL", default_value = "5s", value_parser = krabka_units::parse::positive_time)]
    pub querier_frontier_refresh_interval: Time,

    #[arg(long, env = "KRABKA_OBSERVABILITY_QUERIER_DYNAMIC_INDEX_CACHE_TTL", default_value = "5s", value_parser = krabka_units::parse::positive_time)]
    pub querier_dynamic_index_cache_ttl: Time,

    #[arg(long, env = "KRABKA_OBSERVABILITY_QUERIER_SHARD_INDEX_CACHE_TTL", default_value = "5m", value_parser = krabka_units::parse::positive_time)]
    pub querier_shard_index_cache_ttl: Time,

    #[arg(
        long,
        env = "KRABKA_OBSERVABILITY_QUERIER_SHARD_FETCH_CONCURRENCY",
        default_value = "32"
    )]
    pub querier_shard_fetch_concurrency: NonZeroUsize,

    #[arg(
        long,
        env = "KRABKA_OBSERVABILITY_QUERIER_COLD_BLOCK_FETCH_CONCURRENCY",
        default_value = "8"
    )]
    pub querier_cold_block_fetch_concurrency: NonZeroUsize,

    #[arg(long, env = "KRABKA_OBSERVABILITY_QUERIER_HOT_TAIL_BUCKET_WIDTH", default_value = "1m", value_parser = krabka_units::parse::positive_time)]
    pub querier_hot_tail_bucket_width: Time,

    #[arg(long, env = "KRABKA_OBSERVABILITY_QUERIER_HOT_TAIL_INTERVAL", default_value = "50ms", value_parser = krabka_units::parse::positive_time)]
    pub querier_hot_tail_interval: Time,

    #[arg(long, env = "KRABKA_OBSERVABILITY_QUERIER_DEPENDENCY_RECONNECT_INTERVAL", default_value = "500ms", value_parser = krabka_units::parse::positive_time)]
    pub querier_dependency_reconnect_interval: Time,

    /// TLS for the data port, authentication of its requests, and the
    /// credentials for calls to other Krabka services. Default: every flag
    /// unset, so the data port serves plain HTTP with no authentication, as
    /// Loki does.
    ///
    /// The admin port of the `krabka-observability` binary does not use these
    /// flags.
    #[command(flatten)]
    pub server_security: ServerSecurityArgs,

    /// The audit trail of tenant-affecting and admin operations. Default:
    /// `--audit-topic` unset, so audit is off.
    #[command(flatten)]
    pub audit: AuditArgs,

    /// TLS and SASL for every connection to the WAL broker, and for the audit
    /// producer. Default: `PLAINTEXT`.
    #[command(flatten)]
    pub wal_client_security: WalClientSecurityArgs,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            target: Role::Distributor,
            listen_addr: "0.0.0.0:3100"
                .parse()
                .expect("default observability listen address is valid"),
            object_store_url: None,
            wal_bootstrap_server: None,
            wal_topic: "__krabka_observability_logs_wal".to_string(),
            wal_group_id: "krabka-observability-block-builder".to_string(),
            data_root: PathBuf::from("."),
            querier_index_source: QuerierIndexSource::LocalManifest,
            tenant: None,
            index_prefix: None,
            query_start_ns: None,
            query_end_ns: None,
            max_query_range: None,
            max_query_series: None,
            max_query_read: None,
            max_query_string_bytes: None,
            logs_limits_overrides_config: None,
            max_ingest_body: None,
            wal_append_timeout: None,
            reject_old_samples_max_age: days(7),
            creation_grace_period: minutes(10),
            ingest_quota_burst_window: secs(1),
            broker_access_cache_ttl: secs(10),
            broker_access_max_staleness: minutes(1),
            broker_access_tenant_capacity: NonZeroUsize::new(10_000)
                .expect("default broker access tenant capacity is nonzero"),
            wal_connect_startup_deadline: minutes(2),
            wal_connect_attempt_timeout: secs(15),
            wal_connect_initial_backoff: millis(200),
            wal_connect_max_backoff: secs(2),
            compactor_wal_poll_timeout: millis(500),
            compactor_accumulation_window: secs(2),
            compactor_accumulation_poll_timeout: millis(250),
            compactor_max_records_per_batch: NonZeroUsize::new(4096)
                .expect("default compactor batch size is nonzero"),
            compactor_idle_interval: millis(10),
            all_drain_stage_timeout: secs(30),
            compactor_object_store_initial_backoff: millis(10),
            compactor_object_store_max_backoff: millis(500),
            querier_frontier_refresh_interval: secs(5),
            querier_dynamic_index_cache_ttl: secs(5),
            querier_shard_index_cache_ttl: minutes(5),
            querier_shard_fetch_concurrency: NonZeroUsize::new(32)
                .expect("default querier shard fetch concurrency is nonzero"),
            querier_cold_block_fetch_concurrency: NonZeroUsize::new(8)
                .expect("default querier cold-block fetch concurrency is nonzero"),
            querier_hot_tail_bucket_width: minutes(1),
            querier_hot_tail_interval: millis(50),
            querier_dependency_reconnect_interval: millis(500),
            server_security: ServerSecurityArgs::default(),
            audit: AuditArgs::default(),
            wal_client_security: WalClientSecurityArgs::default(),
        }
    }
}
