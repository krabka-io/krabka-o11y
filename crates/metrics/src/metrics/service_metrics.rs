use super::{ByteSizeExt, TimeExt, SharedRegistry, Family, StatusLabel, Counter, Histogram, TenantLabel, Registry, Arc, Mutex, ByteSize, Time};

/// Cheaply-clonable bundle of metric handles. Construct it once with
/// [`ServiceMetrics::new`], then hand out clones to the handlers that emit.
/// Each clone is a single `Arc::clone`.
#[derive(Clone)]
pub struct ServiceMetrics {
    pub registry: SharedRegistry,
    // INGEST (distributor) role.
    pub ingest_requests: Family<StatusLabel, Counter>,
    pub ingest_bytes: Counter,
    pub ingest_items: Counter,
    pub ingest_duration: Histogram,
    pub wal_append_failures: Counter,
    /// Accepted series counted per tenant on the ingest path.
    pub ingest_series: Family<TenantLabel, Counter>,
    // COMPACTOR role.
    /// Metric blocks written to object storage by the compactor.
    pub blocks_compacted: Counter,
}

impl ServiceMetrics {
    /// Builds a fresh registry, registers every metric, and returns the
    /// bundle.
    #[must_use]
    pub fn new() -> Self {
        let mut registry = Registry::with_prefix("krabka_metrics");

        let ingest_requests: Family<StatusLabel, Counter> = Family::default();
        let ingest_bytes = Counter::default();
        let ingest_items = Counter::default();
        let ingest_duration = Histogram::new([
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
        ]);
        let wal_append_failures = Counter::default();
        let ingest_series: Family<TenantLabel, Counter> = Family::default();

        let blocks_compacted = Counter::default();

        registry.register(
            "ingest_requests",
            "Ingest (push) requests handled, labelled by outcome status.",
            ingest_requests.clone(),
        );
        registry.register(
            "ingest_bytes",
            "Cumulative request-body bytes accepted on the ingest path.",
            ingest_bytes.clone(),
        );
        registry.register(
            "ingest_items",
            "Cumulative items (series/samples) accepted on the ingest path.",
            ingest_items.clone(),
        );
        registry.register(
            "ingest_duration_seconds",
            "Ingest handler latency in seconds.",
            ingest_duration.clone(),
        );
        registry.register(
            "wal_append_failures",
            "Cumulative WAL/produce append failures on the ingest path.",
            wal_append_failures.clone(),
        );
        registry.register(
            "ingest_series",
            "Accepted series on the ingest path, labelled by tenant.",
            ingest_series.clone(),
        );
        registry.register(
            "blocks_compacted",
            "Metric blocks written to object storage by the compactor.",
            blocks_compacted.clone(),
        );
        Self {
            registry: Arc::new(Mutex::new(registry)),
            ingest_requests,
            ingest_bytes,
            ingest_items,
            ingest_duration,
            wal_append_failures,
            ingest_series,
            blocks_compacted,
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
        let status = if ok { "ok" } else { "error" };
        self.ingest_requests
            .get_or_create(&StatusLabel {
                status: status.into(),
            })
            .inc();
        self.ingest_bytes.inc_by(body.bytes_u64());
        self.ingest_items.inc_by(items);
        self.ingest_duration.observe(elapsed.secs_f64());
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
