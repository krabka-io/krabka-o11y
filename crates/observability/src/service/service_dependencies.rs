use super::{
    Arc, AuditHandle, ClientSecurity, CompactionFrontier, CompactionFrontierSource,
    DeferredQueryAuthorizerConnect, DeferredWalConsumerConnect, HotTailDependency, LogHotTail,
    LogIngestLimiter, LogQueryAuthorizer, LogWalConsumer, LogWalSink, OverridesProvider,
    RoleReadiness, ServerSecurity, ServiceMetrics, SharedCompactionFrontier,
    SharedLogDeleteRequests,
};

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
    /// Querier and block builder: the params for the broker-backed query
    /// authorizer. It connects in the background, and the role fails every
    /// authorization closed until it has.
    pub(crate) deferred_query_authorizer_connect: Option<DeferredQueryAuthorizerConnect>,
    /// Shared RED-metrics bundle threaded into the per-role state. It is
    /// `None` in tests that do not care about metrics, and each state then
    /// gets a fresh bundle. The binary constructs one bundle and threads it
    /// through here, so the `:9404` exporter and the handlers share the same
    /// registry.
    pub(crate) metrics: Option<ServiceMetrics>,
    /// Readiness shared with the binary, so the `:9404` admin port and the
    /// data port report the same startup state. `None` leaves the role with a
    /// readiness of its own.
    pub(crate) readiness: Option<RoleReadiness>,
    /// The listener security that the binary loaded once at start. `None`
    /// makes the service runtime load it from the `ServiceConfig`.
    pub(crate) server_security: Option<ServerSecurity>,
    /// The WAL client security that every broker connection of the role
    /// uses, and the audit producer too. `None` connects in plain text.
    pub(crate) wal_security: Option<ClientSecurity>,
    /// The audit handle the handlers record through. `None` makes the service
    /// runtime start the audit layer that the `ServiceConfig` names.
    pub(crate) audit: Option<AuditHandle>,
    /// The per-tenant limits the role resolves a tenant against. `None` makes
    /// the role load them from the `ServiceConfig`.
    pub(crate) limits: Option<Arc<OverridesProvider>>,
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

    /// The object-store instruments from the shared bundle, when one was
    /// threaded in.
    ///
    /// A role with no bundle has nothing to record into, so its object store
    /// is wrapped in an unregistered one and exports no series.
    #[must_use]
    pub(crate) fn object_store_metrics(&self) -> Option<krabka_blockstore::ObjectStoreMetrics> {
        self.metrics
            .as_ref()
            .map(|metrics| metrics.object_store.clone())
    }

    /// The compaction instruments from the shared bundle, when one was
    /// threaded in.
    #[must_use]
    pub(crate) fn compaction_metrics(
        &self,
    ) -> Option<crate::compaction_metrics::CompactionMetrics> {
        self.metrics
            .as_ref()
            .map(|metrics| metrics.compaction.clone())
    }

    /// Shares one readiness between the role's router and whatever else the
    /// binary exposes it on, above all the admin listener.
    #[must_use]
    pub fn with_readiness(mut self, readiness: RoleReadiness) -> Self {
        self.readiness = Some(readiness);
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

    /// The same dependencies with the WAL consumer taken out.
    ///
    /// The querier takes whichever consumer it is handed as its hot tail. In a
    /// process that runs only the querier that is the right reading, and it is
    /// how a test injects a pre-connected consumer. In an all-in-one it is a
    /// trap: the consumer in there is the block builder's, in the block
    /// builder's group, and a querier polling it would take records off the
    /// topic that the block builder then never sees and never writes to a
    /// block. The records are acknowledged, queryable for as long as they sit
    /// in the querier's hot tail, and gone. This is how the all-in-one hands
    /// the querier its own deferred connect instead.
    #[must_use]
    pub(crate) fn without_wal_consumer(mut self) -> Self {
        self.wal_consumer = None;
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
    pub(crate) fn with_deferred_query_authorizer_connect(
        mut self,
        connect: DeferredQueryAuthorizerConnect,
    ) -> Self {
        self.deferred_query_authorizer_connect = Some(connect);
        self
    }

    #[must_use]
    pub(crate) fn with_deferred_wal_consumer_connect(
        mut self,
        connect: DeferredWalConsumerConnect,
    ) -> Self {
        self.deferred_wal_consumer_connect = Some(connect);
        self
    }

    /// Serves every listener of the role with `security`, in place of the
    /// security that the `ServiceConfig` names.
    ///
    /// A binary loads the security once at start, so a bad flag stops it
    /// before it connects to a broker, and hands the result in here.
    #[must_use]
    pub fn with_server_security(mut self, security: ServerSecurity) -> Self {
        self.server_security = Some(security);
        self
    }

    /// Records the audit events of the role through `audit`, in place of the
    /// audit layer that the `ServiceConfig` names.
    ///
    /// The caller owns the writer task behind `audit`, and stops it after the
    /// role stops. A test gives the handle of an audit layer over a
    /// `krabka_audit::MemorySink`.
    #[must_use]
    pub fn with_audit(mut self, audit: AuditHandle) -> Self {
        self.audit = Some(audit);
        self
    }

    /// Resolves every tenant of the role against `limits`, in place of the
    /// overrides file that the `ServiceConfig` names.
    ///
    /// One process holds one provider, so the ingest gate, the read gate and
    /// the compactor's retention sweep answer a tenant with the same numbers.
    #[must_use]
    pub fn with_limits(mut self, limits: Arc<OverridesProvider>) -> Self {
        self.limits = Some(limits);
        self
    }
}
