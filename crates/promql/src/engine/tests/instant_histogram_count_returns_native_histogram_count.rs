use super::*;

#[tokio::test]
pub(crate) async fn instant_histogram_count_returns_native_histogram_count() {
    let engine = PromqlEngine::new(Arc::new(native_histogram_store()), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            "histogram_count(request_duration_seconds)",
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(float_value(&samples[0].value), 4.0));
}
