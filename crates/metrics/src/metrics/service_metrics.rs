use super::{
    ByteSize, CompactionMetrics, Counter, Family, IngestHelpText, IngestInstruments, IngestRequest,
    ObjectStoreMetrics, PipelineInstruments, Registry, RequestOutcome, RoleKind, RoleRegistry,
    SharedRegistry, TenantLabel, Time, WalConsumerMetrics, WalProduceMetrics, register_for_role,
    register_in_new_registry,
};

/// Prefix of every metric the metrics subsystem exports, from either the
/// ingest or the query binary.
pub const METRICS_PREFIX: &str = "krabka_metrics";

/// The sub-registry of `shared` that the metrics subsystem's `role`
/// registers its instruments in, prefixed `krabka_metrics_<role>`.
#[must_use]
pub fn metrics_role_registry(shared: SharedRegistry, role: RoleKind) -> RoleRegistry {
    RoleRegistry {
        shared,
        signal_prefix: METRICS_PREFIX,
        role,
    }
}

/// `# HELP` text of the metrics subsystem's ingest instruments. The query
/// binary registers the same instruments with the same text.
pub const INGEST_HELP: IngestHelpText = IngestHelpText {
    requests: "Ingest (push) requests handled, labelled by outcome status.",
    bytes: "Cumulative request-body bytes accepted on the ingest path.",
    items: "Cumulative items (series/samples) accepted on the ingest path.",
    duration: "Ingest handler latency in seconds.",
    wal_append_failures: "Cumulative WAL/produce append failures on the ingest path.",
};

/// Cheaply-clonable bundle of metric handles. Construct it once with
/// [`ServiceMetrics::new`], then hand out clones to the handlers that emit.
/// Each clone is a single `Arc::clone`.
#[derive(Clone)]
pub struct ServiceMetrics {
    pub registry: SharedRegistry,
    // INGEST (distributor) role.
    /// Ingest requests, bytes, items, latency and WAL append failures.
    pub ingest: IngestInstruments,
    /// Accepted series counted per tenant on the ingest path.
    pub ingest_series: Family<TenantLabel, Counter>,
    // COMPACTOR role.
    /// Metric blocks written to object storage by the compactor.
    pub blocks_compacted: Counter,
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
    /// Builds a fresh registry, registers every metric, and returns the
    /// bundle.
    #[must_use]
    pub fn new() -> Self {
        register_in_new_registry(METRICS_PREFIX, Self::register)
    }

    /// Registers this role's instruments in the process registry.
    ///
    /// Each role has a separate prefix. Its gauges do not overwrite another role's gauges.
    pub async fn for_role(shared: SharedRegistry, role: RoleKind) -> Self {
        register_for_role(metrics_role_registry(shared, role), Self::register).await
    }

    fn register(registry: &mut Registry, shared: SharedRegistry) -> Self {
        let ingest = IngestInstruments::register(registry, &INGEST_HELP);
        let ingest_series: Family<TenantLabel, Counter> = Family::default();
        registry.register(
            "ingest_series",
            "Accepted series on the ingest path, labelled by tenant.",
            ingest_series.clone(),
        );
        let blocks_compacted = Counter::default();
        registry.register(
            "blocks_compacted",
            "Metric blocks written to object storage by the compactor.",
            blocks_compacted.clone(),
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
            ingest_series,
            blocks_compacted,
            wal_consumer,
            wal_produce,
            compaction,
            object_store,
        }
    }

    /// Records one ingest request outcome. This method does NOT touch
    /// `wal_append_failures`. Increment that counter separately at the real WAL
    /// or produce error site, so a 4xx client or validation error does not
    /// inflate the WAL-failure counter.
    ///
    /// `body` is the request-body size and `elapsed` is the handler latency.
    /// This method converts both to the raw units the Prometheus instruments
    /// hold, so a caller never spells out `_bytes` or `_secs`.
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

    /// Records `series` accepted series for `tenant` on the ingest path. The
    /// handler calls this once per accepted push request, after the body
    /// decodes to a series count.
    pub fn record_ingest_series(&self, tenant: &str, series: u64) {
        if series == 0 {
            return;
        }
        self.ingest_series
            .get_or_create(&TenantLabel {
                tenant: tenant.into(),
            })
            .inc_by(series);
    }

    /// Records the `blocks` metric blocks that the compactor wrote in one
    /// flush.
    pub fn record_blocks_compacted(&self, blocks: u64) {
        if blocks == 0 {
            return;
        }
        self.blocks_compacted.inc_by(blocks);
    }
}

impl Default for ServiceMetrics {
    fn default() -> Self {
        Self::new()
    }
}
