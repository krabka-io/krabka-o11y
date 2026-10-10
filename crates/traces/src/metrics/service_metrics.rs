use super::{
    ByteSize, CompactionMetrics, Counter, Family, IngestHelpText, IngestInstruments, IngestRequest,
    ObjectStoreMetrics, PipelineInstruments, QueryHelpText, QueryInstruments, QueryRequest,
    Registry, RequestOutcome, SharedRegistry, TenantId, TenantLabel, Time, WalConsumerMetrics,
    WalProduceMetrics, register_in_new_registry,
};

const INGEST_HELP: IngestHelpText = IngestHelpText {
    requests: "Trace-ingest (push) requests by outcome (status=ok|error)",
    bytes: "Cumulative request-body bytes accepted on the trace-ingest path",
    items: "Cumulative spans accepted on the trace-ingest path",
    duration: "Trace-ingest push-handler latency in seconds",
    wal_append_failures: "Cumulative trace-WAL (produce) append failures",
};

const QUERY_HELP: QueryHelpText = QueryHelpText {
    requests: "Querier requests by route and outcome (route, status=ok|error)",
    duration: "Querier handler latency in seconds, by route",
};

/// Cheaply-clonable bundle of metric handles plus the shared registry.
///
/// Construct it once in the binary's `run()`, before role dispatch. Then clone
/// it into the distributor and querier state structs. Each clone is a handful
/// of `Arc::clone`s.
#[derive(Clone)]
pub struct ServiceMetrics {
    pub registry: SharedRegistry,
    // INGEST (distributor role).
    /// Trace-ingest requests, bytes, spans, latency and WAL append failures.
    pub ingest: IngestInstruments,
    /// Spans accepted on the ingest path, attributed per tenant.
    pub ingest_spans: Family<TenantLabel, Counter>,
    // BLOCK-BUILDER (WAL-consumer role).
    /// WAL span blocks durably written by the block-builder.
    pub blocks_flushed: Counter,
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
    /// `wal_append_failures`.
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
    /// Build a fresh registry, register every metric, and return the bundle.
    #[must_use]
    pub fn new() -> Self {
        register_in_new_registry("krabka_traces", Self::register)
    }

    fn register(registry: &mut Registry, shared: SharedRegistry) -> Self {
        let ingest = IngestInstruments::register(registry, &INGEST_HELP);
        let ingest_spans = Family::<TenantLabel, Counter>::default();
        registry.register(
            "ingest_spans",
            "Cumulative spans accepted on the trace-ingest path, by tenant",
            ingest_spans.clone(),
        );
        let blocks_flushed = Counter::default();
        registry.register(
            "blocks_flushed",
            "Cumulative trace-WAL span blocks durably written by the block-builder",
            blocks_flushed.clone(),
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
            ingest_spans,
            blocks_flushed,
            query,
            wal_consumer,
            wal_produce,
            compaction,
            object_store,
        }
    }

    /// Record one trace-ingest request outcome.
    ///
    /// This bumps the per-status request counter, accumulates bytes and spans,
    /// and observes the handler latency. `ok=false` covers any 4xx or 5xx: a
    /// validation, rate-limit, decode, or produce failure.
    /// [`Self::record_wal_append_failure`] bumps the WAL/produce-specific
    /// failure counter separately, only at the actual produce error site, so a
    /// 4xx client error does not inflate it.
    ///
    /// `body` is the request-body size and `elapsed` is the handler latency.
    /// `items` is a plain span count. It is dimensionless, so it stays an
    /// integer.
    pub fn record_ingest(&self, ok: bool, body: ByteSize, items: u64, elapsed: Time) {
        self.ingest.record(IngestRequest {
            outcome: if ok {
                RequestOutcome::Ok
            } else {
                RequestOutcome::Error
            },
            body,
            items,
            elapsed,
        });
    }

    /// Bump the WAL/produce append-failure counter.
    ///
    /// Call this only when the failure was an actual WAL error, that is a Kafka
    /// produce error. Do not call it for a client or validation 4xx.
    pub fn record_wal_append_failure(&self) {
        self.ingest.record_wal_append_failure();
    }

    /// Attribute `count` accepted spans to `tenant` on the ingest path.
    ///
    /// Call this once per successful push request with the batch size, not once
    /// per span record. Per-tenant span volume is then visible without a
    /// high-cardinality per-record hop.
    pub fn record_ingest_spans(&self, tenant: &TenantId, count: u64) {
        if count == 0 {
            return;
        }
        self.ingest_spans
            .get_or_create(&TenantLabel {
                tenant: tenant.as_str().to_owned(),
            })
            .inc_by(count);
    }

    /// Bump the block-builder flushed-block counter once per span block durably
    /// written to object storage.
    pub fn record_block_flushed(&self) {
        self.blocks_flushed.inc();
    }

    /// Record one querier request. This bumps the per-(route, status) request
    /// counter and observes the per-route handler latency.
    pub fn record_query(&self, route: &str, ok: bool, secs: f64) {
        self.query.record(QueryRequest {
            route,
            outcome: if ok {
                RequestOutcome::Ok
            } else {
                RequestOutcome::Error
            },
            elapsed_secs: secs,
        });
    }
}

impl Default for ServiceMetrics {
    fn default() -> Self {
        Self::new()
    }
}
