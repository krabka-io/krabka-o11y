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
    type Case = (&'static str, fn(&mut NativeHistogram), f64);

    let base = || {
        let mut histogram = native_histogram(0.0, 0.0);
        histogram.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 1,
        }];
        histogram.positive_counts = vec![0.0];
        histogram
    };

    let cases: [Case; 6] = [
        (
            "an observation count that flips to -0",
            |h| h.count = -0.0,
            1.0,
        ),
        ("a sum that flips to -0", |h| h.sum = -0.0, 1.0),
        (
            "a zero count that flips to -0",
            |h| h.zero_count = -0.0,
            1.0,
        ),
        (
            "a bucket count that flips to -0",
            |h| h.positive_counts = vec![-0.0],
            1.0,
        ),
        // A NaN is bitwise equal to itself, so the repeat is not a change.
        (
            "a bucket count that becomes NaN and stays NaN",
            |h| h.positive_counts = vec![f64::NAN],
            1.0,
        ),
        // The threshold is the one float compared numerically, so this moves
        // nothing at all.
        (
            "a zero threshold that flips to -0",
            |h| h.zero_threshold = -0.0,
            0.0,
        ),
    ];

    for (name, mutate, expected) in cases {
        let mut moved = base();
        mutate(&mut moved);
        let mut store = InMemoryMetricStore::new();
        store.push_histogram("tenant-a", labels(&[("__name__", "h")]), 10_000, base());
        for timestamp in [20_000, 30_000] {
            store.push_histogram(
                "tenant-a",
                labels(&[("__name__", "h")]),
                timestamp,
                moved.clone(),
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "changes(h[5m])", 30_000)
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
                == vec![expected],
            "{name}"
        );
    }
}
