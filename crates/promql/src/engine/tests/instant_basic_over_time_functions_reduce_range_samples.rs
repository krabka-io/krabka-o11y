use super::*;

#[tokio::test]
pub(crate) async fn instant_basic_over_time_functions_reduce_range_samples() {
    let store = SeriesFixture::new(labels(&[("__name__", "queue_depth"), ("job", "api")]))
        .at(0_i64, 1.0)
        .at(60_000, 3.0)
        .at(120_000, 5.0)
        .store();

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected, preserves_name) in [
        ("sum_over_time(queue_depth[2m])", 8.0, false),
        ("avg_over_time(queue_depth[2m])", 4.0, false),
        ("count_over_time(queue_depth[2m])", 2.0, false),
        ("min_over_time(queue_depth[2m])", 3.0, false),
        ("max_over_time(queue_depth[2m])", 5.0, false),
        ("first_over_time(queue_depth[2m])", 3.0, true),
        ("last_over_time(queue_depth[2m])", 5.0, true),
        ("present_over_time(queue_depth[2m])", 1.0, false),
    ] {
        let samples = instant_vector(&engine, query, 120_000).await;
        assert2::assert!(samples.len() == 1);
        if preserves_name {
            assert2::assert!(samples[0].labels.get("__name__") == Some("queue_depth"));
        } else {
            assert2::assert!(samples[0].labels.get("__name__").is_none());
        }
        assert2::assert!(samples[0].labels.get("job") == Some("api"));
        assert2::assert!(approx_eq(float_value(&samples[0].value), expected));
    }
}
