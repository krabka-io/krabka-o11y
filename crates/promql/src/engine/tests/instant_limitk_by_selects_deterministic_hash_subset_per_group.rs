#[cfg(feature = "experimental-functions")]
use super::*;

#[cfg(feature = "experimental-functions")]
struct SeriesInput {
    job: &'static str,
    instance: &'static str,
    value: f64,
}

#[cfg(feature = "experimental-functions")]
#[tokio::test]
pub(crate) async fn instant_limitk_by_selects_deterministic_hash_subset_per_group() {
    let mut store = InMemoryMetricStore::new();
    for series in [
        SeriesInput {
            job: "api",
            instance: "a",
            value: 1.0,
        },
        SeriesInput {
            job: "api",
            instance: "b",
            value: 2.0,
        },
        SeriesInput {
            job: "api",
            instance: "c",
            value: 3.0,
        },
        SeriesInput {
            job: "worker",
            instance: "d",
            value: 4.0,
        },
        SeriesInput {
            job: "worker",
            instance: "e",
            value: 5.0,
        },
        SeriesInput {
            job: "worker",
            instance: "f",
            value: 6.0,
        },
    ] {
        store.push_float(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "memory_bytes"),
                crate::test_support::TestLabelPair::new("job", series.job),
                crate::test_support::TestLabelPair::new("instance", series.instance),
            ]),
            10_000,
            series.value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "limitk by (job) (1, memory_bytes)", 10_000).await;
    check!(samples.len() == 2);
    check!(samples.iter().any(|sample| {
        sample.labels.get("__name__") == Some("memory_bytes")
            && sample.labels.get("job") == Some("api")
            && sample.labels.get("instance") == Some("c")
            && approx_eq(float_value(&sample.value), 3.0)
    }));
    let selected = sample_instances(&samples);
    check!(selected == vec!["c", "e"]);
}
