use super::{
    CompactionMetrics, Counter, Family, IngestHelpText, IngestInstruments, IngestRequest,
    ObjectStoreMetrics, PipelineInstruments, QueryHelpText, QueryInstruments, QueryRequest,
    Registry, SharedRegistry, TenantLabel, WalConsumerMetrics, WalProduceMetrics,
    register_in_new_registry,
};

const INGEST_HELP: IngestHelpText = IngestHelpText {
    requests: "Log-ingest (push) requests by outcome (status=ok|error)",
    bytes: "Cumulative request-body bytes accepted on the log-ingest path",
    items: "Cumulative log lines/records accepted on the log-ingest path",
    duration: "Log-ingest push-handler latency in seconds",
    wal_append_failures: "Cumulative log-WAL (produce) append failures",
};

const QUERY_HELP: QueryHelpText = QueryHelpText {
    requests: "Querier requests by route and outcome (route, status=ok|error)",
    duration: "Querier handler latency in seconds, by route",
};

/// Cheaply-clonable bundle of metric handles plus the shared registry.
///
/// Construct it once in the binary's `run()` before role dispatch, then clone
/// it into the distributor and querier state structs. Each clone is a handful
/// of `Arc::clone`s.
#[derive(Clone)]
pub struct ServiceMetrics {
    pub registry: SharedRegistry,
    // INGEST (distributor role).
    /// Log-ingest requests, bytes, lines/records, latency and WAL append
    /// failures.
    pub ingest: IngestInstruments,
    /// Per-tenant accepted log lines on the ingest path. Complements the
    /// tenant-agnostic `ingest_items` counter with per-tenant attribution.
    pub ingest_lines: Family<TenantLabel, Counter>,
    // COMPACT (compactor role).
    /// Log blocks that the compactor durably wrote to object storage. There
    /// is one increment per persisted
    /// [`krabka_blockstore::BlockDescriptor`].
    pub blocks_written: Counter,
    // QUERY (querier role).
    /// Querier requests and per-route latency.
    pub query: QueryInstruments,
    // WAL-CONSUMER, COMPACTOR and OBJECT-STORE roles.
    /// WAL consumer progress and receive delay. See
    /// [`WalConsumerMetrics`] for what lag this measures and what it leaves
    /// to the broker.
    pub wal_consumer: WalConsumerMetrics,
    /// Partial WAL batches and the records they left unacked. See
    /// [`WalProduceMetrics`] for why a partial batch is counted apart from
    /// `wal_append_failures`, and why it matters most on this signal.
    pub wal_produce: WalProduceMetrics,
    /// Compaction passes, their outcome and their output.
    pub compaction: CompactionMetrics,
    /// Object-store requests, latencies, failures and retries. Give this to
    /// [`MeteredObjectStore::wrap`](krabka_blockstore::MeteredObjectStore::wrap)
    /// where the service builds its store, so every read and write this
    /// process makes is counted once.
    pub object_store: ObjectStoreMetrics,
}

impl ServiceMetrics {
    /// Builds a fresh registry, registers every metric, and returns the
    /// bundle.
    #[must_use]
    pub fn new() -> Self {
        register_in_new_registry("krabka_logs", Self::register)
    }

    fn register(registry: &mut Registry, shared: SharedRegistry) -> Self {
        let ingest = IngestInstruments::register(registry, &INGEST_HELP);
        let ingest_lines = Family::<TenantLabel, Counter>::default();
        registry.register(
            "ingest_lines",
            "Accepted log lines on the ingest path, by tenant",
            ingest_lines.clone(),
        );
        let blocks_written = Counter::default();
        registry.register(
            "blocks_written",
            "Log blocks durably written to object storage by the compactor",
            blocks_written.clone(),
        );
        let query = QueryInstruments::register(registry, &QUERY_HELP);
        let PipelineInstruments {
            wal_consumer,
            wal_produce,
            compaction,
            object_store,
        } = PipelineInstruments::register(registry);

        Self {
            registry: shared,
            ingest,
            ingest_lines,
            blocks_written,
            query,
            wal_consumer,
            wal_produce,
            compaction,
            object_store,
        }
    }

    /// Records one log-ingest request outcome. It bumps the per-status
    /// request counter, accumulates bytes and lines, and observes the handler
    /// latency.
    ///
    /// [`RequestOutcome::Error`](crate::service_metrics::RequestOutcome::Error)
    /// covers any 4xx or 5xx, that is a validation, rate-limit,
    /// decode, or produce failure. [`Self::record_wal_append_failure`] bumps
    /// the WAL/produce-specific failure counter separately, and only at the
    /// actual produce error site, so a 4xx client error does not inflate
    /// it.
    pub fn record_ingest(&self, request: IngestRequest) {
        self.ingest.record(request);
    }

    /// Bumps the WAL/produce append-failure counter. Callers call it only
    /// when the failure was an actual WAL (Kafka produce) error, and not a
    /// client or validation 4xx.
    pub fn record_wal_append_failure(&self) {
        self.ingest.record_wal_append_failure();
    }

    /// Adds `lines` accepted log lines to the per-tenant ingest-lines counter.
    ///
    /// The push handlers call it once per accepted ingest request, where both
    /// the tenant (`X-Scope-OrgID`) and the normalized record count are
    /// known.
    pub fn record_ingest_lines(&self, tenant: &str, lines: u64) {
        if lines == 0 {
            return;
        }
        self.ingest_lines
            .get_or_create(&TenantLabel {
                tenant: tenant.into(),
            })
            .inc_by(lines);
    }

    /// Bumps the compactor blocks-written counter once per log block that is
    /// durably persisted to object storage.
    pub fn record_block_written(&self) {
        self.blocks_written.inc();
    }

    /// Records one querier request. It bumps the per-(route, status) request
    /// counter and observes the per-route handler latency.
    pub fn record_query(&self, request: QueryRequest<'_>) {
        self.query.record(request);
    }
}

impl Default for ServiceMetrics {
    fn default() -> Self {
        Self::new()
    }
}
