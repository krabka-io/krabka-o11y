use super::*;

#[derive(bon::Builder)]
struct InstanceSample {
    instance: &'static str,
    value: f64,
}

#[tokio::test]
pub(crate) async fn vector_vector_group_right_carries_labels_from_one_side() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "target_limit"),
            crate::test_support::TestLabelPair::new("job", "api"),
            crate::test_support::TestLabelPair::new("region", "east"),
        ]),
        10_000,
        100.0,
    );
    for sample in [
        InstanceSample::builder().instance("a").value(10.0).build(),
        InstanceSample::builder().instance("b").value(25.0).build(),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "http_requests_total"),
                crate::test_support::TestLabelPair::new("job", "api"),
                crate::test_support::TestLabelPair::new("instance", sample.instance),
            ]),
            10_000,
            sample.value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(
        &engine,
        "target_limit / on (job) group_right(region) http_requests_total",
        10_000,
    )
    .await;
    check!(samples.len() == 2);
    let has_east_api_sample = |instance: &str, want: f64| {
        samples.iter().any(|sample| {
            sample.labels.get("__name__").is_none()
                && sample.labels.get("job") == Some("api")
                && sample.labels.get("region") == Some("east")
                && sample.labels.get("instance") == Some(instance)
                && approx_eq(float_value(&sample.value), want)
        })
    };
    check!(has_east_api_sample("a", 10.0));
    check!(has_east_api_sample("b", 4.0));
}
