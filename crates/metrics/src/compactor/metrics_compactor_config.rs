use super::{
    Arc, AutoOffsetReset, BlockWriter, CompactionLoopConfig, Consumer,
    DEFAULT_FLUSH_MAX_AGE, DEFAULT_FLUSH_MAX_ROWS, MetricsCompactorBuildError,
    MetricsCompactorConfigError, MetricsCompactorRuntime, ObjectStore,
    ObjectStoreCompactionIndexSink, Time, TimeExt,
    consumer_build_error, secs, validate_non_empty};

/// Configuration for the metrics compactor role.
#[derive(Clone, Debug)]
pub struct MetricsCompactorConfig {
    pub client_dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity,
    pub client_frame_max: krabka_client_core::ClientFrameMax,
    pub bootstrap: String,
    pub group_id: String,
    pub client_id: String,
    pub wal_topic: String,
    pub poll_timeout: Time,
    pub auto_offset_reset: AutoOffsetReset,
    /// Flush the accumulated buffer once this many WAL records are buffered.
    pub flush_max_rows: usize,
    /// Flush the accumulated buffer once its oldest record reaches this age.
    pub flush_max_age: Time,
}

impl MetricsCompactorConfig {
    /// Configuration defaults for the metrics compactor role.
    #[must_use]
    pub fn new(bootstrap: impl Into<String>) -> Self {
        Self {
            client_dispatch_queue_capacity:
                krabka_client_core::ConnectionDispatchQueueCapacity::default(),
            client_frame_max: krabka_client_core::ClientFrameMax::default(),
            bootstrap: bootstrap.into(),
            group_id: "krabka-metrics-compactor".to_string(),
            client_id: "krabka-metrics-compactor".to_string(),
            wal_topic: crate::WAL_TOPIC.to_string(),
            poll_timeout: secs(1),
            auto_offset_reset: AutoOffsetReset::Earliest,
            flush_max_rows: DEFAULT_FLUSH_MAX_ROWS,
            flush_max_age: DEFAULT_FLUSH_MAX_AGE,
        }
    }

    /// # Errors
    /// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
    pub fn validate(&self) -> Result<(), MetricsCompactorConfigError> {
        validate_non_empty("bootstrap", &self.bootstrap)?;
        validate_non_empty("group_id", &self.group_id)?;
        validate_non_empty("client_id", &self.client_id)?;
        validate_non_empty("wal_topic", &self.wal_topic)?;
        if self.poll_timeout <= Time::ZERO {
            return Err(MetricsCompactorConfigError::ZeroPollTimeout);
        }
        if self.flush_max_rows == 0 {
            return Err(MetricsCompactorConfigError::ZeroFlushMaxRows);
        }
        Ok(())
    }

    /// # Errors
    /// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
    pub fn build_runtime(
        &self,
        store: Arc<dyn ObjectStore>,
    ) -> Result<MetricsCompactorRuntime, MetricsCompactorConfigError> {
        self.validate()?;
        Ok(MetricsCompactorRuntime {
            block_writer: BlockWriter::new(store.clone()),
            index_sink: ObjectStoreCompactionIndexSink::new(store),
            loop_config: CompactionLoopConfig {
                wal_topic: self.wal_topic.clone(),
                poll_timeout: self.poll_timeout,
                flush_max_rows: self.flush_max_rows,
                flush_max_age: self.flush_max_age,
            },
        })
    }

    /// # Errors
    /// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
    pub async fn build_consumer(&self) -> Result<Consumer, MetricsCompactorBuildError> {
        self.validate()?;
        Consumer::builder()
            .bootstrap(self.bootstrap.clone())
            .dispatch_queue_capacity(self.client_dispatch_queue_capacity.get())
            .frame_max(self.client_frame_max.size())
            .group_id(self.group_id.clone())
            .client_id(self.client_id.clone())
            .auto_offset_reset(self.auto_offset_reset)
            .subscribe([self.wal_topic.clone()])
            .build()
            .await
            .map_err(|error| consumer_build_error(&error))
    }
}
