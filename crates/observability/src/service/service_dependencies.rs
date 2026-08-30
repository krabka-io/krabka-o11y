use super::*;

#[derive(Clone, Default)]
pub struct ServiceDependencies {
    pub(crate) wal_sink: Option<Arc<dyn LogWalSink>>,
    pub(crate) wal_consumer: Option<Arc<tokio::sync::Mutex<Box<dyn LogWalConsumer>>>>,
    pub(crate) ingest_limiter: Option<Arc<dyn LogIngestLimiter>>,
    pub(crate) query_authorizer: Option<Arc<dyn LogQueryAuthorizer>>,
    pub(crate) hot_tail: Option<HotTailDependency>,
    pub(crate) compaction_frontier: Option<SharedCompactionFrontier>,
    pub(crate) delete_requests: Option<SharedLogDeleteRequests>,
    /// Querier-only: the params for the hot-tail WAL consumer. The connection
    /// happens asynchronously after HTTP serving begins (FIX B2).
    pub(crate) deferred_wal_consumer_connect: Option<DeferredWalConsumerConnect>,
    /// Shared RED-metrics bundle threaded into the per-role state. It is
    /// `None` in tests that do not care about metrics, and each state then
    /// gets a fresh bundle. The binary constructs one bundle and threads it
    /// through here, so the `:9404` exporter and the handlers share the same
    /// registry.
    pub(crate) metrics: Option<ServiceMetrics>,
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
    pub(crate) fn with_deferred_wal_consumer_connect(
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
