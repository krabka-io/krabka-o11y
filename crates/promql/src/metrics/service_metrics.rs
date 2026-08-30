use super::{
    Arc, ByteSize, ByteSizeExt, Counter, Family, Gauge, Histogram, Mutex, QueryTypeLabel, Registry,
    RouteLabel, RouteStatusLabel, SharedRegistry, StatusLabel, Time, TimeExt,
};

/// Bundle of metric handles that is cheap to clone. Build it one time with
/// [`ServiceMetrics::new`]. Give a clone, one `Arc::clone` each, to every
/// handler that emits a metric.
#[derive(Clone)]
pub struct ServiceMetrics {
    pub registry: SharedRegistry,
    // INGEST (distributor) role.
    pub ingest_requests: Family<StatusLabel, Counter>,
    pub ingest_bytes: Counter,
    pub ingest_items: Counter,
    pub ingest_duration: Histogram,
    pub wal_append_failures: Counter,
    // QUERY (querier) role.
    pub query_requests: Family<RouteStatusLabel, Counter>,
    pub query_duration: Family<RouteLabel, Histogram>,
    /// PromQL-engine evaluation latency (parse + plan + execute), labelled by
    /// query `type` (`instant`|`range`). This scope is narrower than
    /// `query_duration`, which covers the whole HTTP handler: param decode,
    /// permit wait, and encode.
    pub query_eval_duration: Family<QueryTypeLabel, Histogram>,
    /// Cumulative engine-eval failures, labelled by query `type`.
    pub query_errors: Family<QueryTypeLabel, Counter>,
    /// In-flight `PromQL` queries currently executing in the engine.
    pub active_queries: Gauge,
}

impl ServiceMetrics {
    /// Builds a new registry, registers every metric, and returns the bundle.
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

        let query_requests: Family<RouteStatusLabel, Counter> = Family::default();
        let query_duration: Family<RouteLabel, Histogram> = Family::new_with_constructor(|| {
            Histogram::new([0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0])
        });
        let query_eval_duration: Family<QueryTypeLabel, Histogram> =
            Family::new_with_constructor(|| {
                Histogram::new([0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0])
            });
        let query_errors: Family<QueryTypeLabel, Counter> = Family::default();
        let active_queries = Gauge::default();

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
            "query_requests",
            "Query requests handled, labelled by route and outcome status.",
            query_requests.clone(),
        );
        registry.register(
            "query_duration_seconds",
            "Query handler latency in seconds, labelled by route.",
            query_duration.clone(),
        );
        registry.register(
            "query_eval_duration_seconds",
            "PromQL engine evaluation latency in seconds (parse+plan+execute), labelled by query type.",
            query_eval_duration.clone(),
        );
        registry.register(
            "query_errors",
            "Cumulative PromQL engine evaluation failures, labelled by query type.",
            query_errors.clone(),
        );
        registry.register(
            "active_queries",
            "PromQL queries currently executing in the engine.",
            active_queries.clone(),
        );

        Self {
            registry: Arc::new(Mutex::new(registry)),
            ingest_requests,
            ingest_bytes,
            ingest_items,
            ingest_duration,
            wal_append_failures,
            query_requests,
            query_duration,
            query_eval_duration,
            query_errors,
            active_queries,
        }
    }

    /// Records one ingest request outcome.
    ///
    /// This method does NOT touch `wal_append_failures`. Increment that counter
    /// at the WAL or produce error site, so that a 4xx client or validation
    /// error does not inflate the WAL-failure counter.
    pub fn record_ingest(&self, ok: bool, size: ByteSize, items: u64, latency: Time) {
        let status = if ok { "ok" } else { "error" };
        self.ingest_requests
            .get_or_create(&StatusLabel {
                status: status.into(),
            })
            .inc();
        self.ingest_bytes.inc_by(size.bytes_u64());
        self.ingest_items.inc_by(items);
        // Prometheus histograms are in base units, so the latency lands in
        // seconds no matter what unit the caller measured it in.
        self.ingest_duration.observe(latency.secs_f64());
    }

    /// Records one query request outcome on `route` with its latency.
    pub fn record_query(&self, route: &str, ok: bool, latency: Time) {
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
            .observe(latency.secs_f64());
    }

    /// Records one `PromQL` engine evaluation.
    ///
    /// This method observes `latency` under `query_eval_duration{type}`. When
    /// `ok` is false, it also increments `query_errors{type}`. `query_type` is
    /// `"instant"` or `"range"`.
    pub fn record_eval(&self, query_type: &str, ok: bool, latency: Time) {
        self.query_eval_duration
            .get_or_create(&QueryTypeLabel {
                r#type: query_type.into(),
            })
            .observe(latency.secs_f64());
        if !ok {
            self.query_errors
                .get_or_create(&QueryTypeLabel {
                    r#type: query_type.into(),
                })
                .inc();
        }
    }

    /// Increments `active_queries` at query entry, without an RAII guard.
    ///
    /// Pair every call with [`Self::query_finished`].
    pub fn query_started(&self) {
        self.active_queries.inc();
    }

    /// Decrements `active_queries` at query exit. Pairs with
    /// [`Self::query_started`].
    pub fn query_finished(&self) {
        self.active_queries.dec();
    }
}

impl Default for ServiceMetrics {
    fn default() -> Self {
        Self::new()
    }
}
