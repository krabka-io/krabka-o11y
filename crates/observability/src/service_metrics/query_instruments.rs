use super::{
    Counter, Family, Histogram, Registry, RequestOutcome, RouteLabel, RouteStatusLabel, Time,
    TimeExt as _,
};

/// Latency buckets, in seconds, of the `query_duration_seconds` histogram.
const QUERY_DURATION_BUCKETS: [f64; 10] = [0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0];

/// The query (querier) instruments every queryable signal exports: requests
/// by route and outcome, and per-route handler latency.
#[derive(Clone)]
pub struct QueryInstruments {
    /// Query requests: `query_requests_total{route,status}`.
    pub requests: Family<RouteStatusLabel, Counter>,
    /// Per-route handler latency: `query_duration_seconds{route}`.
    pub duration: Family<RouteLabel, Histogram>,
}

/// The `# HELP` text of each query instrument. Each signal words its own.
#[derive(Debug, Clone, Copy)]
pub struct QueryHelpText {
    pub requests: &'static str,
    pub duration: &'static str,
}

/// One query request outcome, as [`QueryInstruments::record`] takes it.
#[derive(Debug, Clone, Copy)]
pub struct QueryRequest<'a> {
    pub route: &'a str,
    pub outcome: RequestOutcome,
    /// Handler latency.
    pub elapsed: Time,
}

impl QueryInstruments {
    /// Registers the query instruments in `registry`, in the order
    /// `query_requests`, `query_duration_seconds`.
    pub fn register(registry: &mut Registry, help: &QueryHelpText) -> Self {
        let instruments = Self {
            requests: Family::default(),
            duration: Family::new_with_constructor(|| Histogram::new(QUERY_DURATION_BUCKETS)),
        };
        registry.register(
            "query_requests",
            help.requests,
            instruments.requests.clone(),
        );
        registry.register(
            "query_duration_seconds",
            help.duration,
            instruments.duration.clone(),
        );
        instruments
    }

    /// Records one query request. It bumps the per-(route, status) request
    /// counter and observes the per-route handler latency.
    ///
    /// It converts the latency to the raw seconds the Prometheus histogram
    /// holds, so a caller never spells out `_secs`.
    pub fn record(&self, request: QueryRequest<'_>) {
        self.requests
            .get_or_create(&RouteStatusLabel {
                route: request.route.into(),
                status: request.outcome.status().into(),
            })
            .inc();
        self.duration
            .get_or_create(&RouteLabel {
                route: request.route.into(),
            })
            .observe(request.elapsed.secs_f64());
    }
}
