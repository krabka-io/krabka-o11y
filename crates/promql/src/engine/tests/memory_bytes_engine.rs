use super::*;

/// An engine over `memory_bytes` for instances `a` = 1, `b` = 3 and `c` = 2 at 10s.
pub(crate) fn memory_bytes_engine() -> PromqlEngine<InMemoryMetricStore> {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [("a", 1.0), ("b", 3.0), ("c", 2.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "memory_bytes"), ("instance", instance)]),
            10_000,
            value,
        );
    }
    PromqlEngine::new(Arc::new(store), EngineOpts::default())
}
