use super::*;


mod a_classic_histogram_answers_beside_a_native_sibling {
use super::*;
use super::*;

struct BucketFixture {
    upper_bound: &'static str,
    count: f64,
}

#[derive(bon::Builder)]
struct QueryExpectation {
    query: &'static str,
    want: f64,
}

/// A classic histogram answers correctly alongside a native sibling under the
/// same name -- the mixed case, which every other classic test avoids by
/// keeping the two apart. The window bounds are the infinite ones, so the
/// buckets open at each end are the ones being read.
#[tokio::test]
pub(crate) async fn a_classic_histogram_answers_beside_a_native_sibling() {
    let mut store = InMemoryMetricStore::new();
    // A classic histogram closed at both ends by an infinity.
    let buckets = [
        BucketFixture {
            upper_bound: "-1",
            count: 2.0,
        },
        BucketFixture {
            upper_bound: "0",
            count: 3.0,
        },
        BucketFixture {
            upper_bound: "1",
            count: 6.0,
        },
        BucketFixture {
            upper_bound: "+Inf",
            count: 10.0,
        },
    ];
    for bucket_index in 0..buckets.len() {
        let bucket = &buckets[bucket_index];
        store.push_float(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "hk"),
                crate::test_support::TestLabelPair::new("kind", "classic"),
                crate::test_support::TestLabelPair::new("le", bucket.upper_bound),
            ]),
            10_000,
            bucket.count,
        );
    }
    // The native sibling that sends the call to the kernel.
    store.push_histogram(
        "tenant-a",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "hk"),
            crate::test_support::TestLabelPair::new("kind", "native"),
        ]),
        10_000,
        native_histogram(4.0, 8.0),
    );
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let queries = [
        // The bucket running from -Inf, and the one running to +Inf.
        QueryExpectation::builder()
            .query("histogram_fraction(-Inf, -1, hk)")
            .want(0.2)
            .build(),
        QueryExpectation::builder()
            .query("histogram_fraction(1, Inf, hk)")
            .want(0.4)
            .build(),
        QueryExpectation::builder()
            .query("histogram_fraction(0, 1, hk)")
            .want(0.3)
            .build(),
    ];
    for query_index in 0..queries.len() {
        let query = &queries[query_index];
        let QueryResult::InstantVector(samples) = engine
            .query_instant(&tenant_id("tenant-a"), query.query, 10_000)
            .await
            .unwrap_or_else(|error| panic!("{}: {error}", query.query))
        else {
            panic!("expected a vector for {}", query.query);
        };
        let classic = samples
            .iter()
            .find(|sample| sample.labels.get("kind") == Some("classic"))
            .unwrap_or_else(|| panic!("{}: no classic series in the result", query.query));
        assert2::assert!(
            approx_eq(float_value(&classic.value), query.want),
            "{}",
            query.query
        );
    }
}
}


mod avg_over_time_scales_a_native_histogram_by_one_over_the_count {
use super::*;
use super::*;

/// Averaging native histograms sums them and scales by one over the count.
/// Every other scaling in these tests uses a factor of -1, where multiplying
/// and dividing agree -- a half does not.
#[tokio::test]
pub(crate) async fn avg_over_time_scales_a_native_histogram_by_one_over_the_count() {
    let histogram = |value: f64| NativeHistogram {
        schema: 0,
        is_float: true,
        reset_hint: ResetHint::No,
        zero_threshold: 1.0,
        zero_count: value,
        count: value,
        sum: value,
        positive_spans: vec![],
        positive_counts: vec![],
        negative_spans: vec![],
        negative_counts: vec![],
        custom_values: None,
        start_timestamp_ms: None,
    };
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[crate::test_support::TestLabelPair::new("__name__", "h")]),
        10_000,
        histogram(4.0),
    );
    store.push_histogram(
        "tenant-a",
        labels(&[crate::test_support::TestLabelPair::new("__name__", "h")]),
        20_000,
        histogram(6.0),
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let QueryResult::InstantVector(samples) = engine
        .query_instant(&tenant_id("tenant-a"), "avg_over_time(h[5m])", 20_000)
        .await
        .expect("a histogram average")
    else {
        panic!("expected a vector");
    };
    let SampleValue::Histogram(averaged) = &samples[0].value else {
        panic!("expected a histogram sample");
    };
    check!(
        approx_eq(averaged.zero_count, 5.0),
        "the zero bucket averages"
    );
    check!(approx_eq(averaged.count, 5.0));
    check!(approx_eq(averaged.sum, 5.0));
}
}


mod changes_compares_histogram_counts_bitwise_and_the_zero_threshold_numerically {
use super::*;
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
}


mod the_histogram_folds_refuse_their_degenerate_inputs {
use super::*;
use super::*;

#[derive(bon::Builder)]
struct ClassicFoldBucket {
    upper_bound: &'static str,
    count: f64,
}

#[derive(bon::Builder)]
struct ClassicFoldSeries {
    name: &'static str,
    buckets: Vec<ClassicFoldBucket>,
}

/// The degenerate inputs to the histogram folds. A classic series without a
/// `+Inf` bucket has no total to divide by and yields NaN; two buckets are
/// enough to interpolate between; and a NaN bound, or a histogram holding
/// nothing, yields NaN rather than a number. Each guard below is a chain of
/// `||`, and no test had ever made exactly one of its clauses true -- which is
/// what tells the chain apart from the same clauses joined by `&&`. A native
/// histogram whose count is BELOW zero is not degenerate to Prometheus, which
/// refuses only an exactly-empty one, so it is not among these.
#[tokio::test]
pub(crate) async fn the_histogram_folds_refuse_their_degenerate_inputs() {
    let mut store = InMemoryMetricStore::new();
    for series in [
        // Two buckets, closed with +Inf: the smallest series that interpolates.
        ClassicFoldSeries::builder()
            .name("c2")
            .buckets(vec![
                ClassicFoldBucket::builder()
                    .upper_bound("1")
                    .count(1.0)
                    .build(),
                ClassicFoldBucket::builder()
                    .upper_bound("+Inf")
                    .count(2.0)
                    .build(),
            ])
            .build(),
        // No +Inf bucket at all, so there is no total.
        ClassicFoldSeries::builder()
            .name("cf")
            .buckets(vec![
                ClassicFoldBucket::builder()
                    .upper_bound("1")
                    .count(1.0)
                    .build(),
                ClassicFoldBucket::builder()
                    .upper_bound("2")
                    .count(2.0)
                    .build(),
            ])
            .build(),
    ] {
        for bucket in series.buckets {
            store.push_float(
                "tenant-a",
                labels(&[
                    crate::test_support::TestLabelPair::new("__name__", series.name),
                    crate::test_support::TestLabelPair::new("le", bucket.upper_bound),
                ]),
                10_000,
                bucket.count,
            );
        }
    }
    store.push_histogram(
        "tenant-a",
        labels(&[crate::test_support::TestLabelPair::new("__name__", "nh")]),
        10_000,
        native_histogram(4.0, 8.0),
    );
    store.push_histogram(
        "tenant-a",
        labels(&[crate::test_support::TestLabelPair::new("__name__", "nh0")]),
        10_000,
        native_histogram(0.0, 0.0),
    );
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let value = |query: &'static str| {
        let engine = &engine;
        async move {
            let result = engine
                .query_instant(&tenant_id("tenant-a"), query, 10_000)
                .await
                .unwrap_or_else(|error| panic!("{query}: {error}"));
            let QueryResult::InstantVector(samples) = result else {
                panic!("expected a vector for {query}");
            };
            assert2::assert!(samples.len() == 1, "{query}");
            float_value(&samples[0].value)
        }
    };

    // Two buckets are enough to interpolate: the rank lands on the first one's
    // upper bound.
    assert2::assert!(approx_eq(value("histogram_quantile(0.5, c2)").await, 1.0));

    for query in [
        // Without a +Inf bucket there is no total to divide by.
        "histogram_quantile(0.5, cf)",
        "histogram_fraction(0, 1, cf)",
        // A NaN bound poisons the fold, from either side, on either kind.
        "histogram_fraction(NaN, 1, c2)",
        "histogram_fraction(NaN, 1, nh)",
        "histogram_fraction(0, NaN, nh)",
        // A histogram holding nothing has no fraction to report.
        "histogram_fraction(0, 1, nh0)",
    ] {
        assert2::assert!(value(query).await.is_nan(), "{query}");
    }
}
}


mod unary_minus_negates_native_histogram_values_and_marks_gauge {
use super::*;
use super::*;

#[tokio::test]
pub(crate) async fn unary_minus_negates_native_histogram_values_and_marks_gauge() {
    let histogram = single_bucket_native_histogram(4.0, 10.0, 1.0, 3.0);

    let store = super::native_histogram_store::store_with_request_duration_histogram(histogram);

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "-request_duration_seconds", 10_000).await;
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job") == Some("api"));
    let SampleValue::Histogram(histogram) = &samples[0].value else {
        panic!("expected histogram");
    };
    check!(histogram.reset_hint == ResetHint::Gauge);
    check!(approx_eq(histogram.count, -4.0));
    check!(approx_eq(histogram.sum, -10.0));
    check!(approx_eq(histogram.zero_count, -1.0));
    check!(
        histogram
            .positive_counts
            .iter()
            .any(|count| approx_eq(*count, -3.0))
    );
}
}
