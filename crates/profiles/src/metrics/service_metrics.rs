use super::{
    ByteSize, ByteSizeExt as _, CompactionMetrics, Counter, Family, IngestBytes, IngestHelpText,
    IngestInstruments, IngestItems, IngestRequest, ObjectStoreMetrics, PipelineInstruments,
    QueryHelpText, QueryInstruments, QueryRequest, Registry, RequestOutcome, SharedRegistry,
    StatusLabel, TenantLabel, Time, TimeExt as _, WalConsumerMetrics, WalProduceMetrics,
    register_in_new_registry,
};

const INGEST_HELP: IngestHelpText = IngestHelpText {
    requests: "Ingest requests handled, labelled by outcome (ok/error).",
    bytes: "Cumulative ingest request body bytes accepted.",
    items: "Cumulative profiles/samples ingested.",
    duration: "Ingest handler latency in seconds.",
    wal_append_failures: "Cumulative WAL/produce append failures on the ingest path.",
};

const QUERY_HELP: QueryHelpText = QueryHelpText {
    requests: "Query requests handled, labelled by route and outcome (ok/error).",
    duration: "Per-route query handler latency in seconds.",
};

/// Cheaply-clonable bundle of metric handles plus the shared registry.
///
/// Construct the bundle once with [`ServiceMetrics::new`], then clone it
/// freely. Each clone is a small number of `Arc::clone` calls.
#[derive(Clone)]
pub struct ServiceMetrics {
    pub registry: SharedRegistry,
    /// Ingest requests by outcome, accepted body bytes, accepted
    /// profile/sample items, handler latency, and WAL/produce append
    /// failures. They render as `krabka_profiles_ingest_requests_total{status}`,
    /// `krabka_profiles_ingest_bytes_total`, `krabka_profiles_ingest_items_total`,
    /// `krabka_profiles_ingest_duration_seconds` and
    /// `krabka_profiles_wal_append_failures_total`.
    pub ingest: IngestInstruments,
    /// Cumulative profile samples accepted, labelled by tenant. Renders as
    /// `krabka_profiles_ingest_samples_total{tenant}`. The service adds to it
    /// once per ingest request, by the number of WAL samples that the request
    /// produced.
    pub ingest_samples: Family<TenantLabel, Counter>,
    /// Cumulative profile sample blocks flushed to object storage by the
    /// block-builder. Renders as `krabka_profiles_blocks_built_total`.
    pub blocks_built: Counter,
    /// Query requests by route and outcome, and per-route handler latency.
    /// They render as `krabka_profiles_query_requests_total{route,status}`
    /// and `krabka_profiles_query_duration_seconds{route}`.
    pub query: QueryInstruments,
    /// Debug-info uploads retried after pending state became stale.
    pub debuginfo_upload_retries: Counter,
    /// Debug-info HTTP uploads that exceeded their deadline.
    pub debuginfo_upload_timeouts: Counter,
    /// Uploaded-symbol cache lookups, labelled `hit` or `miss`.
    pub symbolizer_cache_requests: Family<StatusLabel, Counter>,
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
    /// Build a fresh registry, register every metric, and return the bundle.
    #[must_use]
    pub fn new() -> Self {
        register_in_new_registry("krabka_profiles", Self::register)
    }

    fn register(registry: &mut Registry, shared: SharedRegistry) -> Self {
        let ingest = IngestInstruments::register(registry, &INGEST_HELP);
        let ingest_samples = Family::<TenantLabel, Counter>::default();
        registry.register(
            "ingest_samples",
            "Cumulative profile samples accepted, labelled by tenant.",
            ingest_samples.clone(),
        );
        let blocks_built = Counter::default();
        registry.register(
            "blocks_built",
            "Cumulative profile sample blocks flushed to object storage by the block-builder.",
            blocks_built.clone(),
        );
        let query = QueryInstruments::register(registry, &QUERY_HELP);
        let debuginfo_upload_retries = Counter::default();
        registry.register(
            "debuginfo_upload_retries",
            "Debug-info uploads retried after stale pending state.",
            debuginfo_upload_retries.clone(),
        );
        let debuginfo_upload_timeouts = Counter::default();
        registry.register(
            "debuginfo_upload_timeouts",
            "Debug-info HTTP uploads that exceeded their deadline.",
            debuginfo_upload_timeouts.clone(),
        );
        let symbolizer_cache_requests = Family::<StatusLabel, Counter>::default();
        registry.register(
            "symbolizer_cache_requests",
            "Uploaded-symbol cache lookups labelled hit or miss.",
            symbolizer_cache_requests.clone(),
        );
        let PipelineInstruments {
            wal_consumer,
            wal_produce,
            compaction,
            object_store,
        } = PipelineInstruments::register(registry);

        Self {
            registry: shared,
            ingest,
            ingest_samples,
            blocks_built,
            query,
            debuginfo_upload_retries,
            debuginfo_upload_timeouts,
            symbolizer_cache_requests,
            wal_consumer,
            wal_produce,
            compaction,
            object_store,
        }
    }

    /// Record one ingest request outcome: bump the per-status request counter,
    /// add to the cumulative bytes and items counters, and observe the latency.
    ///
    /// This method does NOT touch `wal_append_failures`. Increment that counter
    /// separately at the WAL or produce error site. A 4xx client or validation
    /// error is an `ok=false` request, but it is not a WAL failure.
    pub fn record_ingest(&self, ok: bool, bytes: IngestBytes, items: IngestItems, elapsed: Time) {
        self.ingest.record(IngestRequest {
            outcome: if ok {
                RequestOutcome::Ok
            } else {
                RequestOutcome::Error
            },
            body: ByteSize::from_bytes(bytes.0),
            items: items.0,
            elapsed,
        });
    }

    /// Record one WAL or produce append failure, that is, a failed durable write
    /// to the profiles WAL topic. This is distinct from a 4xx client or
    /// validation rejection.
    pub fn record_wal_append_failure(&self) {
        self.ingest.record_wal_append_failure();
    }

    /// Add `samples` to the per-tenant cumulative ingested-samples counter.
    ///
    /// Each ingest request calls this method once with the number of WAL samples
    /// that the request produced. The method does nothing when `samples == 0`.
    pub fn record_ingest_samples(&self, tenant: &str, samples: u64) {
        if samples == 0 {
            return;
        }
        self.ingest_samples
            .get_or_create(&TenantLabel {
                tenant: tenant.into(),
            })
            .inc_by(samples);
    }

    /// Add `blocks` to the cumulative block-builder blocks-flushed counter.
    ///
    /// Each block-build poll batch calls this method once with the number of
    /// blocks that the flush wrote to object storage. The method does nothing
    /// when `blocks == 0`.
    pub fn record_blocks_built(&self, blocks: u64) {
        if blocks == 0 {
            return;
        }
        self.blocks_built.inc_by(blocks);
    }

    /// Record one query request outcome on `route`: bump the per-route+status
    /// request counter and observe the per-route latency.
    pub fn record_query(&self, route: &str, ok: bool, elapsed: Time) {
        self.query.record(QueryRequest {
            route,
            outcome: if ok {
                RequestOutcome::Ok
            } else {
                RequestOutcome::Error
            },
            elapsed_secs: elapsed.secs_f64(),
        });
    }

    /// Record one lookup in the per-pass uploaded-symbol cache.
    pub fn record_symbolizer_cache(&self, hit: bool) {
        self.symbolizer_cache_requests
            .get_or_create(&StatusLabel {
                status: if hit { "hit" } else { "miss" }.into(),
            })
            .inc();
    }
}

impl Default for ServiceMetrics {
    fn default() -> Self {
        Self::new()
    }
}
