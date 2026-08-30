use super::*;

/// `histogram_quantile` interpolates inside the bucket the rank lands in, and
/// which way depends on where that bucket sits. One spanning zero interpolates
/// linearly; one entirely positive interpolates geometrically; one entirely
/// negative does the same on magnitudes and flips the sign back. Only the
/// linear arm had ever run -- the negative arm's sign, its multiply and its
/// ratio were all free, and in a bucket starting at 1.0 dividing by the lower
/// bound is the same as multiplying by it, so the positive arm needs a bucket
/// that starts somewhere else.
#[tokio::test]
pub(crate) async fn histogram_quantile_interpolates_each_kind_of_native_bucket() {
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
            sum: 0.0,
            // Buckets -4..-2 and -2..-1, then the zero bucket, then 1..2 and
            // 2..4. The last is the one that separates dividing by the lower
            // bound from multiplying by it.
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

    for (quantile, want) in [
        // Lands in -2..-1: geometric on magnitudes, negated.
        (0.1, -1.909_683_207_820_833),
        // Lands inside the zero bucket, not on an edge -- at the edge the
        // interpolation returns the upper bound whatever the lower one is.
        (0.35, -0.400_000_000_000_000_36),
        // And exactly at its top edge.
        (0.5, 0.5),
        // Lands in 1..2 and then in 2..4: geometric.
        (0.75, 1.681_792_830_507_429),
        (0.9, 2.639_015_821_545_789_3),
    ] {
        let query = format!("histogram_quantile({quantile}, h)");
        let result = engine
            .query_instant("tenant-a", &query, 10_000)
            .await
            .unwrap_or_else(|error| panic!("{query}: {error}"));
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected a vector for {query}");
        };
        assert2::assert!(samples.len() == 1, "{query}");
        assert2::assert!(approx_eq(float_value(&samples[0].value), want), "{query}");
    }
}
