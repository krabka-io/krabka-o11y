use super::*;

/// Cheaply-clonable bundle of metric handles plus the shared registry.
///
/// Construct the bundle once with [`ServiceMetrics::new`], then clone it
/// freely. Each clone is a small number of `Arc::clone` calls.
#[derive(Clone)]
pub struct ServiceMetrics {
    pub registry: SharedRegistry,
    /// Ingest requests, labelled by outcome. Renders as
    /// `krabka_profiles_ingest_requests_total{status}`.
    pub ingest_requests: Family<StatusLabel, Counter>,
    /// Cumulative ingest body bytes accepted. Renders as
    /// `krabka_profiles_ingest_bytes_total`.
    pub ingest_bytes: Counter,
    /// Cumulative profile/sample items ingested. Renders as
    /// `krabka_profiles_ingest_items_total`.
    pub ingest_items: Counter,
    /// Ingest handler latency in seconds.
    pub ingest_duration: Histogram,
    /// Cumulative WAL/produce append failures. Renders as
    /// `krabka_profiles_wal_append_failures_total`.
    pub wal_append_failures: Counter,
    /// Cumulative profile samples accepted, labelled by tenant. Renders as
    /// `krabka_profiles_ingest_samples_total{tenant}`. The service adds to it
    /// once per ingest request, by the number of WAL samples that the request
    /// produced.
    pub ingest_samples: Family<TenantLabel, Counter>,
    /// Cumulative profile sample blocks flushed to object storage by the
    /// block-builder. Renders as `krabka_profiles_blocks_built_total`.
    pub blocks_built: Counter,
    /// Query requests, labelled by route + outcome. Renders as
    /// `krabka_profiles_query_requests_total{route,status}`.
    pub query_requests: Family<RouteStatusLabel, Counter>,
    /// Per-route query handler latency in seconds.
    pub query_duration: Family<RouteLabel, Histogram>,
}

impl ServiceMetrics {
    /// Build a fresh registry, register every metric, and return the bundle.
    #[must_use]
    pub fn new() -> Self {
        let mut registry = Registry::with_prefix("krabka_profiles");

        let ingest_requests = Family::<StatusLabel, Counter>::default();
        let ingest_bytes = Counter::default();
        let ingest_items = Counter::default();
        let ingest_duration = Histogram::new([
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
        ]);
        let wal_append_failures = Counter::default();
        let ingest_samples = Family::<TenantLabel, Counter>::default();
        let blocks_built = Counter::default();
        let query_requests = Family::<RouteStatusLabel, Counter>::default();
        let query_duration = Family::<RouteLabel, Histogram>::new_with_constructor(|| {
            Histogram::new([0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0])
        });

        registry.register(
            "ingest_requests",
            "Ingest requests handled, labelled by outcome (ok/error).",
            ingest_requests.clone(),
        );
        registry.register(
            "ingest_bytes",
            "Cumulative ingest request body bytes accepted.",
            ingest_bytes.clone(),
        );
        registry.register(
            "ingest_items",
            "Cumulative profiles/samples ingested.",
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
            "ingest_samples",
            "Cumulative profile samples accepted, labelled by tenant.",
            ingest_samples.clone(),
        );
        registry.register(
            "blocks_built",
            "Cumulative profile sample blocks flushed to object storage by the block-builder.",
            blocks_built.clone(),
        );
        registry.register(
            "query_requests",
            "Query requests handled, labelled by route and outcome (ok/error).",
            query_requests.clone(),
        );
        registry.register(
            "query_duration_seconds",
            "Per-route query handler latency in seconds.",
            query_duration.clone(),
        );

        Self {
            registry: Arc::new(Mutex::new(registry)),
            ingest_requests,
            ingest_bytes,
            ingest_items,
            ingest_duration,
            wal_append_failures,
            ingest_samples,
            blocks_built,
            query_requests,
            query_duration,
        }
    }

    /// Record one ingest request outcome: bump the per-status request counter,
    /// add to the cumulative bytes and items counters, and observe the latency.
    ///
    /// This method does NOT touch `wal_append_failures`. Increment that counter
    /// separately at the WAL or produce error site. A 4xx client or validation
    /// error is an `ok=false` request, but it is not a WAL failure.
    pub fn record_ingest(&self, ok: bool, bytes: IngestBytes, items: IngestItems, elapsed: Time) {
        let status = if ok { "ok" } else { "error" };
        self.ingest_requests
            .get_or_create(&StatusLabel {
                status: status.into(),
            })
            .inc();
        if bytes.0 > 0 {
            self.ingest_bytes.inc_by(bytes.0);
        }
        if items.0 > 0 {
            self.ingest_items.inc_by(items.0);
        }
        // `prometheus-client` histograms take fractional seconds, so the extent
        // is extracted here, at the exposition seam.
        self.ingest_duration.observe(elapsed.secs_f64());
    }

    /// Record one WAL or produce append failure, that is, a failed durable write
    /// to the profiles WAL topic. This is distinct from a 4xx client or
    /// validation rejection.
    pub fn record_wal_append_failure(&self) {
        self.wal_append_failures.inc();
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
        let status = if ok { "ok" } else { "error" };
        self.query_requests
            .get_or_create(&RouteStatusLabel {
                route: route.into(),
                status: status.into(),
            })
            .inc();
        self.query_duration
            .get_or_create(&RouteLabel {
                route: route.into(),
            })
            .observe(elapsed.secs_f64());
    }
}

impl Default for ServiceMetrics {
    fn default() -> Self {
        Self::new()
    }
}
