use super::*;

/// RAII guard that holds the `active_queries` increment while a query runs.
///
/// The guard decrements `active_queries` on drop, which covers early returns
/// and panics. The guard does nothing when no metrics bundle is configured.
pub(crate) struct ActiveQueryGuard {
    pub(crate) metrics: Option<ServiceMetrics>,
}

impl Drop for ActiveQueryGuard {
    fn drop(&mut self) {
        if let Some(metrics) = &self.metrics {
            metrics.query_finished();
        }
    }
}
