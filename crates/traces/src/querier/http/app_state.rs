use axum::http::StatusCode;
use krabka_observability::service_metrics::{QueryRequest, RequestOutcome};
use krabka_units::convert::StdDurationExt as _;

use super::{Arc, HttpConfig, ServiceMetrics, SpanStore, TraceqlEngine};

pub(crate) struct AppState<S: SpanStore> {
    pub(crate) engine: Arc<TraceqlEngine<S>>,
    pub(crate) cfg: HttpConfig,
    pub(crate) metrics: Option<ServiceMetrics>,
}

impl<S: SpanStore> Clone for AppState<S> {
    fn clone(&self) -> Self {
        Self {
            engine: Arc::clone(&self.engine),
            cfg: self.cfg.clone(),
            metrics: self.metrics.clone(),
        }
    }
}

impl<S: SpanStore> AppState<S> {
    /// Record one querier request on `route` with the outcome its response
    /// `status` gives and its elapsed time. A 2xx counts as `status="ok"`,
    /// anything else as `status="error"`. This does nothing when metrics are
    /// not wired, as in test routers.
    pub(crate) fn record_query(&self, route: &str, status: StatusCode, start: std::time::Instant) {
        if let Some(metrics) = &self.metrics {
            metrics.record_query(QueryRequest {
                route,
                outcome: RequestOutcome::from_success_status(status),
                elapsed: start.elapsed().as_time(),
            });
        }
    }
}
