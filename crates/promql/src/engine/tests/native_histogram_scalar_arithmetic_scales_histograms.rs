use super::*;

#[tokio::test]
pub(crate) async fn native_histogram_scalar_arithmetic_scales_histograms() {
    let engine = PromqlEngine::new(Arc::new(native_histogram_store()), EngineOpts::default());
    for (query, expected) in [
        ("histogram_count(request_duration_seconds * 2)", 8.0),
        ("histogram_sum(2 * request_duration_seconds)", 20.0),
        ("histogram_count(request_duration_seconds / 2)", 2.0),
        ("histogram_sum(request_duration_seconds / 2)", 5.0),
    ] {
        let result = engine
            .query_instant("tenant-a", query, 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert2::assert!(samples.len() == 1);
        assert2::assert!(samples[0].labels.get("__name__") == None);
        assert2::assert!(samples[0].labels.get("job") == Some("api"));
        assert2::assert!(approx_eq(float_value(&samples[0].value), expected));
    }
}
