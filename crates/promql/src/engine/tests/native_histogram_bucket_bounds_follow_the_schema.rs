use super::*;

/// Bucket bounds at a schema other than zero. At schema 0 the exponent factor
/// is one, so multiplying the index by it, dividing by it, and negating the
/// schema all give the same bound -- every histogram test above uses that
/// schema. At schema 1 the buckets are a square root apart.
#[tokio::test]
pub(crate) async fn native_histogram_bucket_bounds_follow_the_schema() {
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "h")]),
        10_000,
        NativeHistogram {
            schema: 1,
            is_float: true,
            reset_hint: ResetHint::No,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count: 2.0,
            sum: 3.0,
            // Buckets 1 and 2: (1, sqrt 2] and (sqrt 2, 2].
            positive_spans: vec![BucketSpan {
                offset: 1,
                length: 2,
            }],
            positive_counts: vec![1.0, 1.0],
            negative_spans: vec![],
            negative_counts: vec![],
            custom_values: None,
            start_timestamp_ms: None,
        },
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let QueryResult::InstantVector(samples) = engine
        .query_instant("tenant-a", "histogram_quantile(0.5, h)", 10_000)
        .await
        .expect("a quantile")
    else {
        panic!("expected a vector");
    };
    check!(
        approx_eq(float_value(&samples[0].value), std::f64::consts::SQRT_2),
        "the median sits on the boundary between the two buckets: {}",
        float_value(&samples[0].value)
    );
}
