use super::*;

/// `histogram_stdvar` weights each bucket by the square of the distance from
/// its representative value to the mean, and that value is the geometric
/// midpoint -- negated for a bucket below zero, arithmetic for one spanning
/// it. The `sum` here is deliberately not zero: at a mean of zero the squaring
/// hides the sign, so a negative bucket's midpoint could come back positive
/// and the variance would not move.
#[tokio::test]
pub(crate) async fn histogram_stdvar_places_each_bucket_at_its_own_midpoint() {
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "h")]),
        10_000,
        NativeHistogram {
            schema: 0,
            is_float: true,
            reset_hint: ResetHint::No,
            zero_threshold: 0.5,
            zero_count: 2.0,
            count: 12.0,
            sum: 12.0,
            positive_spans: vec![BucketSpan {
                offset: 1,
                length: 2,
            }],
            positive_counts: vec![4.0, 2.0],
            negative_spans: vec![BucketSpan {
                offset: 1,
                length: 2,
            }],
            negative_counts: vec![3.0, 1.0],
            custom_values: None,
            start_timestamp_ms: None,
        },
    );
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    for (query, want) in [
        ("histogram_stdvar(h)", 3.459_559_885_480_119_5),
        ("histogram_stddev(h)", 1.859_989_216_495_654_6),
    ] {
        let result = engine
            .query_instant("tenant-a", query, 10_000)
            .await
            .unwrap_or_else(|error| panic!("{query}: {error}"));
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected a vector for {query}");
        };
        assert2::assert!(samples.len() == 1, "{query}");
        assert2::assert!(approx_eq(float_value(&samples[0].value), want), "{query}");
    }
}
