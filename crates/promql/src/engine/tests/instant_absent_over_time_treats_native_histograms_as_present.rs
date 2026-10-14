use super::*;

#[tokio::test]
pub(crate) async fn instant_absent_over_time_treats_native_histograms_as_present() {
    let store = store_with_request_duration_histogram_at(native_histogram(4.0, 10.0), 90_000);

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            &tenant_id("tenant-a"),
            r#"absent_over_time(request_duration_seconds{job="api"}[1m])"#,
            120_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert2::assert!(samples.is_empty());
}
