use super::*;

struct ResetCase {
    description: &'static str,
    second: NativeHistogram,
    expected: f64,
}

/// `resets` on native histograms counts a reset when *any* component shrinks,
/// not just `count` and `sum`. Each case below lowers exactly one component
/// and holds the rest, so the clause under test is the only one that can
/// report the reset -- a series where two components fall together proves
/// nothing about either.
#[tokio::test]
pub(crate) async fn resets_counts_a_drop_in_any_native_histogram_component() {
    let histogram =
        |count: f64, sum: f64, zero_count: f64, positive: f64, negative: f64| NativeHistogram {
            schema: 0,
            is_float: true,
            reset_hint: ResetHint::No,
            zero_threshold: 1.0,
            zero_count,
            count,
            sum,
            positive_spans: vec![BucketSpan {
                offset: 0,
                length: 1,
            }],
            positive_counts: vec![positive],
            negative_spans: vec![BucketSpan {
                offset: 0,
                length: 1,
            }],
            negative_counts: vec![negative],
            custom_values: None,
            start_timestamp_ms: None,
        };

    for case in [
        ResetCase {
            description: "nothing drops",
            second: histogram(10.0, 10.0, 5.0, 3.0, 3.0),
            expected: 0.0,
        },
        ResetCase {
            description: "count",
            second: histogram(9.0, 10.0, 5.0, 3.0, 3.0),
            expected: 1.0,
        },
        ResetCase {
            description: "sum",
            second: histogram(10.0, 9.0, 5.0, 3.0, 3.0),
            expected: 1.0,
        },
        ResetCase {
            description: "zero bucket",
            second: histogram(10.0, 10.0, 4.0, 3.0, 3.0),
            expected: 1.0,
        },
        ResetCase {
            description: "positive bucket",
            second: histogram(10.0, 10.0, 5.0, 2.0, 3.0),
            expected: 1.0,
        },
        ResetCase {
            description: "negative bucket",
            second: histogram(10.0, 10.0, 5.0, 3.0, 2.0),
            expected: 1.0,
        },
    ] {
        let mut store = InMemoryMetricStore::new();
        store.push_histogram(
            "tenant-a",
            labels(&[crate::test_support::TestLabelPair::new("__name__", "h")]),
            0,
            histogram(10.0, 10.0, 5.0, 3.0, 3.0),
        );
        store.push_histogram(
            "tenant-a",
            labels(&[crate::test_support::TestLabelPair::new("__name__", "h")]),
            60_000,
            case.second,
        );

        let samples = instant_vector(&default_engine(store), "resets(h[5m])", 60_000).await;
        assert2::assert!(samples.len() == 1, "{}", case.description);
        assert2::assert!(
            approx_eq(float_value(&samples[0].value), case.expected),
            "{}",
            case.description
        );
    }
}
