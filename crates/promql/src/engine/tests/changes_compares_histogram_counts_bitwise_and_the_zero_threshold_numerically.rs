use super::*;

/// `changes` asks `FloatHistogram.Equals` whether a histogram sample differs
/// from the one before it, and Prometheus answers that question with two
/// different conventions in the same comparison. Every COUNT -- the observation
/// count, the sum, the zero count and each bucket -- goes through
/// `math.Float64bits`, so `+0` and `-0` are different counts and a NaN equals
/// itself. The zero THRESHOLD goes through `!=` on the float, so `+0` and `-0`
/// are the same threshold.
///
/// Each case moves exactly one field away from a base sample and then holds it
/// there for a third sample, so the count below is the first step's answer plus
/// the repeat's: a field the comparison reads bitwise changes once and then
/// stays, and a NaN that repeats does not change again.
#[tokio::test]
pub(crate) async fn changes_compares_histogram_counts_bitwise_and_the_zero_threshold_numerically() {
    struct ChangeCase {
        name: &'static str,
        mutate: fn(&mut NativeHistogram),
        expected: f64,
    }

    let base = || {
        let mut histogram = native_histogram(0.0, 0.0);
        histogram.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 1,
        }];
        histogram.positive_counts = vec![0.0];
        histogram
    };

    let cases = [
        ChangeCase {
            name: "an observation count that flips to -0",
            mutate: |histogram| histogram.count = -0.0,
            expected: 1.0,
        },
        ChangeCase {
            name: "a sum that flips to -0",
            mutate: |histogram| histogram.sum = -0.0,
            expected: 1.0,
        },
        ChangeCase {
            name: "a zero count that flips to -0",
            mutate: |histogram| histogram.zero_count = -0.0,
            expected: 1.0,
        },
        ChangeCase {
            name: "a bucket count that flips to -0",
            mutate: |histogram| histogram.positive_counts = vec![-0.0],
            expected: 1.0,
        },
        // A NaN is bitwise equal to itself, so the repeat is not a change.
        ChangeCase {
            name: "a bucket count that becomes NaN and stays NaN",
            mutate: |histogram| histogram.positive_counts = vec![f64::NAN],
            expected: 1.0,
        },
        // The threshold is the one float compared numerically, so this moves
        // nothing at all.
        ChangeCase {
            name: "a zero threshold that flips to -0",
            mutate: |histogram| histogram.zero_threshold = -0.0,
            expected: 0.0,
        },
    ];

    for case in cases {
        let mut moved = base();
        (case.mutate)(&mut moved);
        let mut store = InMemoryMetricStore::new();
        store.push_histogram(
            "tenant-a",
            labels(&[crate::test_support::TestLabelPair::new("__name__", "h")]),
            10_000,
            base(),
        );
        for timestamp in [20_000, 30_000] {
            store.push_histogram(
                "tenant-a",
                labels(&[crate::test_support::TestLabelPair::new("__name__", "h")]),
                timestamp,
                moved.clone(),
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(&tenant_id("tenant-a"), "changes(h[5m])", 30_000)
            .await
            .expect("a changes count");
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected a vector");
        };

        check!(
            samples
                .iter()
                .map(|sample| float_value(&sample.value))
                .collect::<Vec<_>>()
                == vec![case.expected],
            "{}",
            case.name
        );
    }
}
