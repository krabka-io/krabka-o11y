use super::{SharedRegistry, Family, StatusLabel, Counter, Histogram, TenantLabel, RouteStatusLabel, RouteLabel, Registry, Arc, Mutex, EncodeLabelSet, ByteRateExt, ByteSizeExt, FrequencyExt, RatioExt, StdDurationExt, TimeExt, ByteSize, Time};

/// Cheaply-clonable bundle of metric handles plus the shared registry.
///
/// Construct it once in the binary's `run()`, before role dispatch. Then clone
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
    /// Spans accepted on the ingest path, attributed per tenant.
    pub ingest_spans: Family<TenantLabel, Counter>,
    // BLOCK-BUILDER (WAL-consumer role).
    /// WAL span blocks durably written by the block-builder.
    pub blocks_flushed: Counter,
    // QUERY (querier role).
    pub query_requests: Family<RouteStatusLabel, Counter>,
    pub query_duration: Family<RouteLabel, Histogram>,
}

impl ServiceMetrics {
    /// Build a fresh registry, register every metric, and return the bundle.
    #[must_use]
    pub fn new() -> Self {
        let mut registry = Registry::with_prefix("krabka_traces");

        let ingest_requests = Family::<StatusLabel, Counter>::default();
        let ingest_bytes = Counter::default();
        let ingest_items = Counter::default();
        let ingest_duration = Histogram::new([
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
        ]);
        let wal_append_failures = Counter::default();
        let ingest_spans = Family::<TenantLabel, Counter>::default();
        let blocks_flushed = Counter::default();
        let query_requests = Family::<RouteStatusLabel, Counter>::default();
        let query_duration = Family::<RouteLabel, Histogram>::new_with_constructor(|| {
            Histogram::new([0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0])
        });

        registry.register(
            "ingest_requests",
            "Trace-ingest (push) requests by outcome (status=ok|error)",
            ingest_requests.clone(),
        );
        registry.register(
            "ingest_bytes",
            "Cumulative request-body bytes accepted on the trace-ingest path",
            ingest_bytes.clone(),
        );
        registry.register(
            "ingest_items",
            "Cumulative spans accepted on the trace-ingest path",
            ingest_items.clone(),
        );
        registry.register(
            "ingest_duration_seconds",
            "Trace-ingest push-handler latency in seconds",
            ingest_duration.clone(),
        );
        registry.register(
            "wal_append_failures",
            "Cumulative trace-WAL (produce) append failures",
            wal_append_failures.clone(),
        );
        registry.register(
            "ingest_spans",
            "Cumulative spans accepted on the trace-ingest path, by tenant",
            ingest_spans.clone(),
        );
        registry.register(
            "blocks_flushed",
            "Cumulative trace-WAL span blocks durably written by the block-builder",
            blocks_flushed.clone(),
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
            ingest_spans,
            blocks_flushed,
            query_requests,
            query_duration,
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
    /// This method converts both to the raw units the Prometheus instruments
    /// hold, so callers never spell out `_bytes` or `_secs` themselves. `items`
    /// is a plain span count. It is dimensionless, so it stays an integer.
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

    /// Bump the WAL/produce append-failure counter.
    ///
    /// Call this only when the failure was an actual WAL error, that is a Kafka
    /// produce error. Do not call it for a client or validation 4xx.
    pub fn record_wal_append_failure(&self) {
        self.wal_append_failures.inc();
    }

    /// Attribute `count` accepted spans to `tenant` on the ingest path.
    ///
    /// Call this once per successful push request with the batch size, not once
    /// per span record. Per-tenant span volume is then visible without a
    /// high-cardinality per-record hop.
    pub fn record_ingest_spans(&self, tenant: &str, count: u64) {
        if count == 0 {
            return;
        }
        self.ingest_spans
            .get_or_create(&TenantLabel {
                tenant: tenant.into(),
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
            .observe(secs);
    }
}

impl Default for ServiceMetrics {
    fn default() -> Self {
        Self::new()
    }
}
