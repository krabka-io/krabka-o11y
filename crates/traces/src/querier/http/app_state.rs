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
    /// Record one querier request on `route` with the given outcome and
    /// elapsed time. This does nothing when metrics are not wired, as in test
    /// routers.
    pub(crate) fn record_query(&self, route: &str, ok: bool, start: std::time::Instant) {
        if let Some(metrics) = &self.metrics {
            metrics.record_query(route, ok, start.elapsed().as_secs_f64());
        }
    }
}
