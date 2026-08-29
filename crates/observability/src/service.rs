#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServiceStatus {
    pub role: Role,
}

#[derive(Clone, Default)]
pub struct ServiceDependencies {
    wal_sink: Option<Arc<dyn LogWalSink>>,
    wal_consumer: Option<Arc<tokio::sync::Mutex<Box<dyn LogWalConsumer>>>>,
    ingest_limiter: Option<Arc<dyn LogIngestLimiter>>,
    query_authorizer: Option<Arc<dyn LogQueryAuthorizer>>,
    hot_tail: Option<HotTailDependency>,
    compaction_frontier: Option<SharedCompactionFrontier>,
    delete_requests: Option<SharedLogDeleteRequests>,
    /// Querier-only: the params for the hot-tail WAL consumer. The connection
    /// happens asynchronously after HTTP serving begins (FIX B2).
    deferred_wal_consumer_connect: Option<DeferredWalConsumerConnect>,
    /// Shared RED-metrics bundle threaded into the per-role state. It is
    /// `None` in tests that do not care about metrics, and each state then
    /// gets a fresh bundle. The binary constructs one bundle and threads it
    /// through here, so the `:9404` exporter and the handlers share the same
    /// registry.
    metrics: Option<ServiceMetrics>,
}

#[derive(Clone)]
struct HotTailDependency {
    source: Arc<dyn LogHotTail>,
    frontier: CompactionFrontierSource,
}

/// Parameters needed to connect a [`KafkaLogWalConsumer`] in the background.
#[derive(Clone)]
struct DeferredWalConsumerConnect {
    bootstrap: String,
    group_id: String,
    topic: String,
    client_resource_policy: ClientResourcePolicy,
}

/// Validated Kafka connection resource limits shared by this process.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ClientResourcePolicy {
    pub dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity,
    pub frame_max: krabka_client_core::ClientFrameMax,
}

#[derive(Clone, Default)]
pub struct SharedLogDeleteRequests {
    inner: Arc<Mutex<CompactorDeleteRequests>>,
    storage_path: Option<Arc<PathBuf>>,
}

#[derive(Debug, Error)]
pub enum LogDeleteRequestStoreError {
    #[error("delete request store I/O error for {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("delete request store JSON error for {path:?}: {source}")]
    Json {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
}

#[derive(Debug, Error)]
pub enum LokiRuleStoreError {
    #[error("Loki rule store I/O error for {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("Loki rule store JSON error for {path:?}: {source}")]
    Json {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
}

#[derive(Debug, Error)]
pub enum ActiveLogDeleteFilterError {
    #[error(transparent)]
    Store(#[from] LogDeleteRequestStoreError),
    #[error(transparent)]
    BlockStore(#[from] BlockStoreError),
    #[error("stored delete request query {query:?} failed to parse: {source}")]
    Parse { query: String, source: ParseError },
}

impl ServiceDependencies {
    /// Threads a shared RED-metrics bundle into the service, so the per-role
    /// state, that is the distributor or the querier, increments the same
    /// registry the `:9404` exporter serves.
    #[must_use]
    pub fn with_metrics(mut self, metrics: ServiceMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    #[must_use]
    pub fn with_wal_sink(mut self, sink: impl LogWalSink) -> Self {
        self.wal_sink = Some(Arc::new(sink));
        self
    }

    #[must_use]
    pub fn with_wal_consumer(mut self, consumer: impl LogWalConsumer) -> Self {
        self.wal_consumer = Some(Arc::new(tokio::sync::Mutex::new(Box::new(consumer))));
        self
    }

    #[must_use]
    pub fn with_ingest_limiter(mut self, limiter: impl LogIngestLimiter) -> Self {
        self.ingest_limiter = Some(Arc::new(limiter));
        self
    }

    #[must_use]
    pub fn with_query_authorizer(mut self, authorizer: impl LogQueryAuthorizer) -> Self {
        self.query_authorizer = Some(Arc::new(authorizer));
        self
    }

    #[must_use]
    pub fn with_delete_requests(mut self, requests: SharedLogDeleteRequests) -> Self {
        self.delete_requests = Some(requests);
        self
    }

    #[must_use]
    pub fn with_compaction_frontier(mut self, frontier: SharedCompactionFrontier) -> Self {
        self.compaction_frontier = Some(frontier);
        self
    }

    #[must_use]
    pub fn with_hot_tail(self, source: impl LogHotTail, compacted_through_ns: i64) -> Self {
        self.with_hot_tail_frontier(source, CompactionFrontier::new(compacted_through_ns))
    }

    #[must_use]
    pub fn with_hot_tail_frontier(
        mut self,
        source: impl LogHotTail,
        frontier: CompactionFrontier,
    ) -> Self {
        self.hot_tail = Some(HotTailDependency {
            source: Arc::new(source),
            frontier: CompactionFrontierSource::Snapshot(frontier),
        });
        self
    }

    #[must_use]
    pub fn with_hot_tail_shared_frontier(
        mut self,
        source: impl LogHotTail,
        frontier: SharedCompactionFrontier,
    ) -> Self {
        self.hot_tail = Some(HotTailDependency {
            source: Arc::new(source),
            frontier: CompactionFrontierSource::Shared(frontier),
        });
        self
    }

    #[must_use]
    fn with_deferred_wal_consumer_connect(
        mut self,
        bootstrap: String,
        group_id: String,
        topic: String,
        client_resource_policy: ClientResourcePolicy,
    ) -> Self {
        self.deferred_wal_consumer_connect = Some(DeferredWalConsumerConnect {
            bootstrap,
            group_id,
            topic,
            client_resource_policy,
        });
        self
    }
}

#[must_use]
/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub fn run(config: ServiceConfig) -> Result<ServiceStatus, Infallible> {
    let ServiceConfig {
        target,
        listen_addr: _listen_addr,
        object_store_url: _object_store_url,
        wal_bootstrap_server: _wal_bootstrap_server,
        wal_topic: _wal_topic,
        wal_group_id: _wal_group_id,
        data_root: _data_root,
        querier_index_source: _querier_index_source,
        tenant: _tenant,
        index_prefix: _index_prefix,
        query_start_ns: _query_start_ns,
        query_end_ns: _query_end_ns,
        max_query_range: _max_query_range,
        max_query_series: _max_query_series,
        max_query_read: _max_query_read,
        max_query_length: _max_query_length,
        max_ingest_body: _max_ingest_body,
        wal_append_timeout: _wal_append_timeout,
        reject_old_samples_max_age: _reject_old_samples_max_age,
        creation_grace_period: _creation_grace_period,
        ingest_quota_burst_window: _ingest_quota_burst_window,
        wal_connect_startup_deadline: _wal_connect_startup_deadline,
        wal_connect_attempt_timeout: _wal_connect_attempt_timeout,
        wal_connect_initial_backoff: _wal_connect_initial_backoff,
        wal_connect_max_backoff: _wal_connect_max_backoff,
        compactor_wal_poll_timeout: _compactor_wal_poll_timeout,
        compactor_accumulation_window: _compactor_accumulation_window,
        compactor_accumulation_poll_timeout: _compactor_accumulation_poll_timeout,
        compactor_max_records_per_batch: _compactor_max_records_per_batch,
        compactor_idle_interval: _compactor_idle_interval,
        compactor_object_store_initial_backoff: _compactor_object_store_initial_backoff,
        compactor_object_store_max_backoff: _compactor_object_store_max_backoff,
        querier_frontier_refresh_interval: _querier_frontier_refresh_interval,
        querier_dynamic_index_cache_ttl: _querier_dynamic_index_cache_ttl,
        querier_shard_index_cache_ttl: _querier_shard_index_cache_ttl,
        querier_shard_fetch_concurrency: _querier_shard_fetch_concurrency,
        querier_cold_block_fetch_concurrency: _querier_cold_block_fetch_concurrency,
        querier_hot_tail_bucket_width: _querier_hot_tail_bucket_width,
        querier_hot_tail_interval: _querier_hot_tail_interval,
        querier_dependency_reconnect_interval: _querier_dependency_reconnect_interval,
    } = config;

    Ok(ServiceStatus { role: target })
}

