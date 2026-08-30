use super::*;

/// The histogram extrapolation is a third copy of the same geometry, and the
/// span test above only reads the shape of the result, never its numbers --
/// which is why the sample spacing, the 1.1x threshold and both half-interval
/// clamps were all free here. The three series below clamp at the end, at the
/// start, and at a gap sitting between 1.1x the spacing and the spacing plus
/// 1.1.
#[tokio::test]
pub(crate) async fn increase_extrapolates_native_histogram_counts_by_the_sample_spacing() {
    fn h(count: f64) -> NativeHistogram {
        NativeHistogram {
            schema: 0,
            is_float: true,
            reset_hint: ResetHint::No,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count,
            sum: count,
            positive_spans: vec![BucketSpan {
                offset: 0,
                length: 1,
            }],
            positive_counts: vec![count],
            negative_spans: vec![],
            negative_counts: vec![],
            custom_values: None,
            start_timestamp_ms: None,
        }
    }

    let mut store = InMemoryMetricStore::new();
    for (name, points) in [
        (
            "gh_ends_short",
            vec![
                (50_000_i64, 0.0),
                (60_000, 10.0),
                (70_000, 20.0),
                (80_000, 30.0),
            ],
        ),
        (
            "gh_starts_late",
            vec![(70_000_i64, 5.0), (80_000, 15.0), (90_000, 25.0)],
        ),
        (
            "gh_thr_edge",
            vec![(68_950_i64, 0.0), (78_950, 10.0), (88_950, 20.0)],
        ),
    ] {
        for (ts_ms, count) in points {
            store.push_histogram("tenant-a", labels(&[("__name__", name)]), ts_ms, h(count));
        }
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    for (query, want) in [
        // 20s to the range end clamps to half a 10s spacing; 10s to the start
        // does not.
        ("increase(gh_ends_short[1m])", 45.0),
        // The mirror.
        ("increase(gh_starts_late[1m])", 35.0),
        // 11.05s to the end: over 1.1x the spacing, under the spacing plus 1.1.
        ("increase(gh_thr_edge[1m])", 30.0),
    ] {
        let result = engine
            .query_instant("tenant-a", query, 100_000)
            .await
            .unwrap_or_else(|error| panic!("{query}: {error}"));
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected a vector for {query}");
        };
        assert2::assert!(samples.len() == 1, "{query}");
        let SampleValue::Histogram(histogram) = &samples[0].value else {
            panic!("expected a histogram sample for {query}");
        };
        assert2::assert!(approx_eq(histogram.count, want), "{query}");
    }
}
