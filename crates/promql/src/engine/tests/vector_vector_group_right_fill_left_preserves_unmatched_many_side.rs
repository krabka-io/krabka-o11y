use super::*;

#[derive(bon::Builder)]
struct RequestSeries {
    job: &'static str,
    instance: &'static str,
    value: f64,
}

#[tokio::test]
pub(crate) async fn vector_vector_group_right_fill_left_preserves_unmatched_many_side() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "job_quota"),
            crate::test_support::TestLabelPair::new("job", "api"),
            crate::test_support::TestLabelPair::new("region", "east"),
        ]),
        10_000,
        10.0,
    );
    for series in [
        RequestSeries::builder()
            .job("api")
            .instance("a")
            .value(100.0)
            .build(),
        RequestSeries::builder()
            .job("api")
            .instance("b")
            .value(50.0)
            .build(),
        RequestSeries::builder()
            .job("worker")
            .instance("c")
            .value(7.0)
            .build(),
    ] {
        push_named_job_instance_sample(
            &mut store,
            "http_requests_total",
            series.job,
            series.instance,
            series.value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    // Prometheus swaps the operand sides for group_right before applying fill flags.
    let samples = instant_vector(
        &engine,
        "job_quota + on (job) group_right(region) fill_right(0) http_requests_total",
        10_000,
    )
    .await;
    assert_filled_many_side(&samples);
}
