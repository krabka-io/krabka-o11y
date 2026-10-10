#[cfg(feature = "experimental-functions")]
use super::*;

#[cfg(feature = "experimental-functions")]
pub(crate) fn sample_instances(samples: &[crate::InstantSample]) -> Vec<&str> {
    let mut instances = samples
        .iter()
        .map(|sample| sample.labels.get("instance").expect("instance label"))
        .collect::<Vec<_>>();
    instances.sort_unstable();
    instances
}

/// The instances `instant_vector` selects with `query` at 10s, out of
/// `memory_bytes{job="api"}` instances `a`..`e` valued 1..5.
#[cfg(feature = "experimental-functions")]
pub(crate) async fn selected_memory_instances(query: &str) -> Vec<String> {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 4.0), ("e", 5.0)] {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "memory_bytes"),
                ("job", "api"),
                ("instance", instance),
            ]),
            10_000,
            value,
        );
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, query, 10_000).await;
    sample_instances(&samples)
        .into_iter()
        .map(str::to_owned)
        .collect()
}
