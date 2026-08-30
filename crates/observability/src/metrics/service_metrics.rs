use super::*;

/// Cheaply-clonable bundle of metric handles plus the shared registry.
///
/// Construct it once in the binary's `run()` before role dispatch, then clone
/// it into the distributor and querier state structs. Each clone is a handful
/// of `Arc::clone`s.
#[derive(Clone)]
pub struct ServiceMetrics {
    pub registry: SharedRegistry,
    // INGEST (distributor role).
    pub ingest_requests: Family<StatusLabel, Counter>,
    pub ingest_bytes: Counter,
    pub ingest_items: Counter,
    pub ingest_duration: Histogram,
    pub wal_append_failures: Counter,
    /// Per-tenant accepted log lines on the ingest path. Complements the
    /// tenant-agnostic `ingest_items` counter with per-tenant attribution.
    pub ingest_lines: Family<TenantLabel, Counter>,
    // COMPACT (compactor role).
    /// Log blocks that the compactor durably wrote to object storage. There
    /// is one increment per persisted
    /// [`krabka_blockstore::BlockDescriptor`].
    pub blocks_written: Counter,
    // QUERY (querier role).
    pub query_requests: Family<RouteStatusLabel, Counter>,
    pub query_duration: Family<RouteLabel, Histogram>,
}

impl ServiceMetrics {
    /// Builds a fresh registry, registers every metric, and returns the
    /// bundle.
    #[must_use]
    pub fn new() -> Self {
        let mut registry = Registry::with_prefix("krabka_logs");

        let ingest_requests = Family::<StatusLabel, Counter>::default();
        let ingest_bytes = Counter::default();
        let ingest_items = Counter::default();
        let ingest_duration = Histogram::new([
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
        ]);
        let wal_append_failures = Counter::default();
        let ingest_lines = Family::<TenantLabel, Counter>::default();
        let blocks_written = Counter::default();
        let query_requests = Family::<RouteStatusLabel, Counter>::default();
        let query_duration = Family::<RouteLabel, Histogram>::new_with_constructor(|| {
            Histogram::new([0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0])
        });

        registry.register(
            "ingest_requests",
            "Log-ingest (push) requests by outcome (status=ok|error)",
            ingest_requests.clone(),
        );
        registry.register(
            "ingest_bytes",
            "Cumulative request-body bytes accepted on the log-ingest path",
            ingest_bytes.clone(),
        );
        registry.register(
            "ingest_items",
            "Cumulative log lines/records accepted on the log-ingest path",
            ingest_items.clone(),
        );
        registry.register(
            "ingest_duration_seconds",
            "Log-ingest push-handler latency in seconds",
            ingest_duration.clone(),
        );
        registry.register(
            "wal_append_failures",
            "Cumulative log-WAL (produce) append failures",
            wal_append_failures.clone(),
        );
        registry.register(
            "ingest_lines",
            "Accepted log lines on the ingest path, by tenant",
            ingest_lines.clone(),
        );
        registry.register(
            "blocks_written",
            "Log blocks durably written to object storage by the compactor",
            blocks_written.clone(),
        );
        registry.register(
            "query_requests",
            "Querier requests by route and outcome (route, status=ok|error)",
            query_requests.clone(),
        );
        registry.register(
            "query_duration_seconds",
            "Querier handler latency in seconds, by route",
            query_duration.clone(),
        );

        Self {
            registry: Arc::new(Mutex::new(registry)),
            ingest_requests,
            ingest_bytes,
            ingest_items,
            ingest_duration,
            wal_append_failures,
            ingest_lines,
            blocks_written,
            query_requests,
            query_duration,
        }
    }

    /// Records one log-ingest request outcome. It bumps the per-status
    /// request counter, accumulates bytes and lines, and observes the handler
    /// latency.
    ///
    /// `ok=false` covers any 4xx or 5xx, that is a validation, rate-limit,
    /// decode, or produce failure. [`Self::record_wal_append_failure`] bumps
    /// the WAL/produce-specific failure counter separately, and only at the
    /// actual produce error site, so a 4xx client error does not inflate
    /// it.
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

    /// Bumps the WAL/produce append-failure counter. Callers call it only
    /// when the failure was an actual WAL (Kafka produce) error, and not a
    /// client or validation 4xx.
    pub fn record_wal_append_failure(&self) {
        self.wal_append_failures.inc();
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
