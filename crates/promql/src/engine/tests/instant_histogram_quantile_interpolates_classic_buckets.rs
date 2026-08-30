use super::*;

#[tokio::test]
pub(crate) async fn instant_histogram_quantile_interpolates_classic_buckets() {
    let mut store = InMemoryMetricStore::new();
    for (le, value) in [("0.1", 0.0), ("0.2", 1.0), ("0.4", 3.0), ("+Inf", 3.0)] {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "http_request_duration_seconds_bucket"),
                ("job", "api"),
                ("le", le),
            ]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            "histogram_quantile(0.5, http_request_duration_seconds_bucket)",
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("le").is_none());
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(float_value(&samples[0].value), 0.25));
}
