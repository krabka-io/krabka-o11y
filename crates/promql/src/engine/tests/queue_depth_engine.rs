use super::*;

/// An engine over `queue_depth{job="api"}`: 1, 2 and 3 at 0s, 60s and 120s.
pub(crate) fn queue_depth_engine() -> PromqlEngine<InMemoryMetricStore> {
    let store = queue_depth_store(&[(0, 1.0), (60_000, 2.0), (120_000, 3.0)]);
    PromqlEngine::new(Arc::new(store), EngineOpts::default())
}
