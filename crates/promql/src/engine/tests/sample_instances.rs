#[cfg(feature = "experimental-functions")]
use super::*;

#[cfg(feature = "experimental-functions")]
struct MemoryInstance {
    instance: &'static str,
    value: f64,
}

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
    for sample in [
        MemoryInstance {
            instance: "a",
            value: 1.0,
        },
        MemoryInstance {
            instance: "b",
            value: 2.0,
        },
        MemoryInstance {
            instance: "c",
            value: 3.0,
        },
        MemoryInstance {
            instance: "d",
            value: 4.0,
        },
        MemoryInstance {
            instance: "e",
            value: 5.0,
        },
    ] {
        push_named_job_instance_sample(
            &mut store,
            "memory_bytes",
            "api",
            sample.instance,
            sample.value,
        );
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, query, 10_000).await;
    sample_instances(&samples)
        .into_iter()
        .map(str::to_owned)
        .collect()
}
