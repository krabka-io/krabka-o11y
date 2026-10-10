use super::*;

/// One `queue_depth{instance, zone}` series and its value at 10s.
#[derive(Clone, Copy)]
pub(crate) struct ZonedQueueDepth<'a> {
    pub(crate) instance: &'a str,
    pub(crate) zone: &'a str,
    pub(crate) depth: f64,
}

/// An engine over one `queue_depth` series per entry of `series`, at 10s.
pub(crate) fn zoned_queue_depth_engine(
    series: &[ZonedQueueDepth<'_>],
) -> PromqlEngine<InMemoryMetricStore> {
    let mut store = InMemoryMetricStore::new();
    for entry in series {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "queue_depth"),
                ("instance", entry.instance),
                ("zone", entry.zone),
            ]),
            10_000,
            entry.depth,
        );
    }
    PromqlEngine::new(Arc::new(store), EngineOpts::default())
}
