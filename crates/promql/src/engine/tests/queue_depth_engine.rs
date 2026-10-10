use super::*;

/// An engine over `queue_depth{job="api"}`: 1, 2 and 3 at 0s, 60s and 120s.
pub(crate) fn queue_depth_engine() -> PromqlEngine<InMemoryMetricStore> {
    let store = SeriesFixture::new(labels(&[("__name__", "queue_depth"), ("job", "api")]))
        .at(0_i64, 1.0)
        .at(60_000, 2.0)
        .at(120_000, 3.0)
        .store();
    PromqlEngine::new(Arc::new(store), EngineOpts::default())
}
