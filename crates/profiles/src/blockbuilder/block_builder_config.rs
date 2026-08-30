use super::{
    Arc, ByteSize, DEFAULT_FLUSH_MAX_AGE, DEFAULT_FLUSH_RECORDS, DEFAULT_INDEX_SNAPSHOT_MAX,
    DEFAULT_WAL_FETCH_MAX, DEFAULT_WAL_FETCH_PARTITION_MAX, IndexSnapshotRetain, ObjectStore,
    PROFILES_WAL_TOPIC, ServiceMetrics, Time, millis,
};

#[derive(Clone)]
pub struct BlockBuilderConfig {
    pub client_dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity,
    pub client_frame_max: krabka_client_core::ClientFrameMax,
    pub bootstrap: String,
    pub wal_topic: String,
    pub group_id: String,
    pub store: Arc<dyn ObjectStore>,
    pub index_key: String,
    pub wal_fetch_max: ByteSize,
    pub wal_fetch_partition_max: ByteSize,
    pub flush_records: usize,
    /// Flush the accumulated buffer once the oldest buffered record reaches this age.
    pub flush_max_age: Time,
    /// How long each WAL poll waits for records.
    pub poll_timeout: Time,
    pub index_snapshot_max: ByteSize,
    pub index_snapshot_retain: IndexSnapshotRetain,
    /// Optional self-instrumentation metrics. When set, the block-builder adds
    /// to `krabka_profiles_blocks_built_total` the number of blocks that each
    /// flush wrote. `None`, the default, turns metric emission off. The
    /// block-builder then still works without a metrics registry, as in tests
    /// and in `run()`.
    pub metrics: Option<ServiceMetrics>,
}

impl BlockBuilderConfig {
    #[must_use]
    pub fn new(bootstrap: String, store: Arc<dyn ObjectStore>) -> Self {
        Self {
            client_dispatch_queue_capacity:
                krabka_client_core::ConnectionDispatchQueueCapacity::default(),
            client_frame_max: krabka_client_core::ClientFrameMax::default(),
            bootstrap,
            wal_topic: PROFILES_WAL_TOPIC.to_owned(),
            group_id: "krabka-profiles-block-builder".to_string(),
            store,
            index_key: "index/profiles.json".to_string(),
            wal_fetch_max: DEFAULT_WAL_FETCH_MAX,
            wal_fetch_partition_max: DEFAULT_WAL_FETCH_PARTITION_MAX,
            flush_records: DEFAULT_FLUSH_RECORDS,
            flush_max_age: DEFAULT_FLUSH_MAX_AGE,
            poll_timeout: millis(500),
            index_snapshot_max: DEFAULT_INDEX_SNAPSHOT_MAX,
            index_snapshot_retain: IndexSnapshotRetain::default(),
            metrics: None,
        }
    }

    /// Attach a [`ServiceMetrics`] bundle so the block-builder emits
    /// `krabka_profiles_blocks_built_total`.
    #[must_use]
    pub fn with_metrics(mut self, metrics: ServiceMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }
}
