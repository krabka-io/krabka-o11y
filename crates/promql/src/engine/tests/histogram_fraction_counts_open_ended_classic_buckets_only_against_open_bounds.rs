use super::*;

#[derive(bon::Builder)]
struct ClassicBucket {
    upper_bound: &'static str,
    cumulative_count: f64,
}

struct ClassicHistogramFixture {
    name: &'static str,
    buckets: Vec<ClassicBucket>,
}

struct HistogramFractionCase {
    query: &'static str,
    expected: f64,
}

/// `histogram_fraction` over classic buckets, where the outermost bounds are
/// infinite. An open-ended bucket counts in full only when the query's own
/// bound is open the same way -- a finite bound never reaches into it, however
/// far out it goes. Nothing exercised those two arms, nor a query bound that
/// lands inside a bucket rather than on its edge.
#[tokio::test]
pub(crate) async fn histogram_fraction_counts_open_ended_classic_buckets_only_against_open_bounds()
{
    let mut store = InMemoryMetricStore::new();
    // Cumulative counts. The first `le` is negative, so the opening bucket runs
    // from -Inf, and the `+Inf` bucket closes the series at the other end.
    for histogram in [
        ClassicHistogramFixture {
            name: "hc",
            buckets: vec![
                ClassicBucket::builder()
                    .upper_bound("-1")
                    .cumulative_count(2.0)
                    .build(),
                ClassicBucket::builder()
                    .upper_bound("0")
                    .cumulative_count(3.0)
                    .build(),
                ClassicBucket::builder()
                    .upper_bound("1")
                    .cumulative_count(6.0)
                    .build(),
                ClassicBucket::builder()
                    .upper_bound("+Inf")
                    .cumulative_count(10.0)
                    .build(),
            ],
        },
        ClassicHistogramFixture {
            name: "hc2",
            buckets: vec![
                ClassicBucket::builder()
                    .upper_bound("-1")
                    .cumulative_count(2.0)
                    .build(),
                ClassicBucket::builder()
                    .upper_bound("+Inf")
                    .cumulative_count(5.0)
                    .build(),
            ],
        },
        // A bucket four wide, so dividing by its width is not the same as
        // multiplying by it -- which it is for every bucket above.
        ClassicHistogramFixture {
            name: "hc3",
            buckets: vec![
                ClassicBucket::builder()
                    .upper_bound("0")
                    .cumulative_count(0.0)
                    .build(),
                ClassicBucket::builder()
                    .upper_bound("4")
                    .cumulative_count(8.0)
                    .build(),
                ClassicBucket::builder()
                    .upper_bound("+Inf")
                    .cumulative_count(10.0)
                    .build(),
            ],
        },
    ] {
        for bucket in histogram.buckets {
            store.push_float(
                "tenant-a",
                labels(&[
                    crate::test_support::TestLabelPair::new("__name__", histogram.name),
                    crate::test_support::TestLabelPair::new("le", bucket.upper_bound),
                ]),
                10_000,
                bucket.cumulative_count,
            );
        }
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    for case in [
        // A range on bucket edges takes whole buckets.
        HistogramFractionCase {
            query: "histogram_fraction(0, 1, hc)",
            expected: 0.3,
        },
        // The -Inf bucket counts only against an -Inf bound...
        HistogramFractionCase {
            query: "histogram_fraction(-Inf, -1, hc)",
            expected: 0.2,
        },
        HistogramFractionCase {
            query: "histogram_fraction(-2, -1, hc)",
            expected: 0.0,
        },
        // ...and the +Inf bucket only against a +Inf bound.
        HistogramFractionCase {
            query: "histogram_fraction(1, Inf, hc)",
            expected: 0.4,
        },
        HistogramFractionCase {
            query: "histogram_fraction(1, 2, hc)",
            expected: 0.0,
        },
        // A bucket open at the top with a finite lower edge is still reached
        // through the upper arm, not the lower one.
        HistogramFractionCase {
            query: "histogram_fraction(-1, Inf, hc2)",
            expected: 0.6,
        },
        // Everything, and a bound landing inside a bucket rather than on it.
        HistogramFractionCase {
            query: "histogram_fraction(-Inf, Inf, hc)",
            expected: 1.0,
        },
        HistogramFractionCase {
            query: "histogram_fraction(0.5, 1, hc)",
            expected: 0.15,
        },
        // Partial cover of a four-wide bucket: half of it, not eight times it.
        HistogramFractionCase {
            query: "histogram_fraction(1, 3, hc3)",
            expected: 0.4,
        },
        HistogramFractionCase {
            query: "histogram_fraction(0, 4, hc3)",
            expected: 0.8,
        },
        HistogramFractionCase {
            query: "histogram_fraction(2, Inf, hc3)",
            expected: 0.6,
        },
    ] {
        assert_lone_value(&engine, case.query, case.expected).await;
    }
}
