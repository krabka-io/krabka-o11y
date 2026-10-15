use super::*;

mod anchored_increase_does_not_treat_a_flat_counter_step_as_a_reset {
use super::*;

#[derive(bon::Builder)]
struct CounterPoint {
    timestamp_ms: i64,
    value: f64,
}

/// A counter that holds its value across a step has not reset. The anchored
/// `increase` correction fires only on a strict decrease, so a flat pair must
/// contribute nothing -- counting it as a reset adds the whole pre-step value
/// back and more than triples the answer here.
#[tokio::test]
pub(crate) async fn anchored_increase_does_not_treat_a_flat_counter_step_as_a_reset() {
    let mut store = InMemoryMetricStore::new();
    for point in [
        CounterPoint::builder().timestamp_ms(0).value(5.0).build(),
        CounterPoint::builder()
            .timestamp_ms(60_000)
            .value(5.0)
            .build(),
        CounterPoint::builder()
            .timestamp_ms(120_000)
            .value(7.0)
            .build(),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[crate::test_support::TestLabelPair::new("__name__", "ctr")]),
            point.timestamp_ms,
            point.value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "increase(anchored(ctr[5m]))", 120_000).await;
    assert2::assert!(samples.len() == 1);
    assert2::assert!(approx_eq(float_value(&samples[0].value), 2.0));

    // The same fold as `rate`, divided by the range in seconds. `increase`
    // never reaches that division.
    let QueryResult::InstantVector(samples) = engine
        .query_instant(&tenant_id("tenant-a"), "rate(anchored(ctr[5m]))", 120_000)
        .await
        .expect("an anchored rate")
    else {
        panic!("expected a vector");
    };
    assert2::assert!(approx_eq(
        float_value(&samples[0].value),
        0.006_666_666_666_666_667
    ));
}

}

mod increase_extrapolates_native_histogram_counts_by_the_sample_spacing {
use super::*;
use crate::test_support::nh;

#[derive(bon::Builder)]
struct HistogramPoint {
    timestamp_ms: i64,
    count: f64,
}

struct HistogramSeriesCase {
    name: &'static str,
    points: Vec<HistogramPoint>,
}

struct QueryExpectation {
    query: &'static str,
    want: f64,
}

/// The histogram extrapolation is a third copy of the same geometry, and the
/// span test above only reads the shape of the result, never its numbers --
/// which is why the sample spacing, the 1.1x threshold and both half-interval
/// clamps were all free here. The three series below clamp at the end, at the
/// start, and at a gap sitting between 1.1x the spacing and the spacing plus
/// 1.1.
#[tokio::test]
pub(crate) async fn increase_extrapolates_native_histogram_counts_by_the_sample_spacing() {
    let mut store = InMemoryMetricStore::new();
    let series = [
        HistogramSeriesCase {
            name: "gh_ends_short",
            points: vec![
                HistogramPoint::builder()
                    .timestamp_ms(50_000)
                    .count(0.0)
                    .build(),
                HistogramPoint::builder()
                    .timestamp_ms(60_000)
                    .count(10.0)
                    .build(),
                HistogramPoint::builder()
                    .timestamp_ms(70_000)
                    .count(20.0)
                    .build(),
                HistogramPoint::builder()
                    .timestamp_ms(80_000)
                    .count(30.0)
                    .build(),
            ],
        },
        HistogramSeriesCase {
            name: "gh_starts_late",
            points: vec![
                HistogramPoint::builder()
                    .timestamp_ms(70_000)
                    .count(5.0)
                    .build(),
                HistogramPoint::builder()
                    .timestamp_ms(80_000)
                    .count(15.0)
                    .build(),
                HistogramPoint::builder()
                    .timestamp_ms(90_000)
                    .count(25.0)
                    .build(),
            ],
        },
        HistogramSeriesCase {
            name: "gh_thr_edge",
            points: vec![
                HistogramPoint::builder()
                    .timestamp_ms(68_950)
                    .count(0.0)
                    .build(),
                HistogramPoint::builder()
                    .timestamp_ms(78_950)
                    .count(10.0)
                    .build(),
                HistogramPoint::builder()
                    .timestamp_ms(88_950)
                    .count(20.0)
                    .build(),
            ],
        },
    ];
    for series_index in 0..series.len() {
        let series = &series[series_index];
        for point_index in 0..series.points.len() {
            let point = &series.points[point_index];
            store.push_histogram(
                "tenant-a",
                labels(&[crate::test_support::TestLabelPair::new(
                    "__name__",
                    series.name,
                )]),
                point.timestamp_ms,
                nh(
                    point.count,
                    point.count,
                    0,
                    &[crate::test_support::BucketCount {
                        index: 0,
                        count: point.count,
                    }],
                ),
            );
        }
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let queries = [
        // 20s to the range end clamps to half a 10s spacing; 10s to the start
        // does not.
        QueryExpectation {
            query: "increase(gh_ends_short[1m])",
            want: 35.0,
        },
        // The mirror.
        QueryExpectation {
            query: "increase(gh_starts_late[1m])",
            want: 35.0,
        },
        // 11.05s to the end: over 1.1x the spacing, under the spacing plus 1.1.
        QueryExpectation {
            query: "increase(gh_thr_edge[1m])",
            want: 25.0,
        },
    ];
    for query_index in 0..queries.len() {
        let query = &queries[query_index];
        let result = engine
            .query_instant(&tenant_id("tenant-a"), query.query, 100_000)
            .await
            .unwrap_or_else(|error| panic!("{}: {error}", query.query));
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected a vector for {}", query.query);
        };
        assert2::assert!(samples.len() == 1, "{}", query.query);
        let SampleValue::Histogram(histogram) = &samples[0].value else {
            panic!("expected a histogram sample for {}", query.query);
        };
        assert2::assert!(approx_eq(histogram.count, query.want), "{}", query.query);
    }
}

}

mod increase_extrapolates_the_same_way_through_the_histogram_kernel {
use super::*;

#[derive(bon::Builder)]
struct HistogramFloatPoint {
    timestamp_ms: i64,
    value: f64,
}

struct HistogramSeriesCase {
    name: &'static str,
    points: Vec<HistogramFloatPoint>,
}

struct QueryExpectation {
    query: &'static str,
    expected: f64,
}

/// The same three geometries through the *other* extrapolation implementation.
///
/// `rate`/`increase`/`delta` lower onto a float-only operator leaf, which
/// cannot carry native histograms, so a selector that matches even one
/// histogram series routes the whole call through the interpreter kernel
/// instead -- a separate, byte-for-byte port of the same arithmetic. A float
/// series with a histogram sibling under its own name is the only shape that
/// reaches it, and without one the kernel's copy is never executed at all.
#[tokio::test]
pub(crate) async fn increase_extrapolates_the_same_way_through_the_histogram_kernel() {
    let mut store = InMemoryMetricStore::new();
    for case in [
        HistogramSeriesCase {
            name: "k_ends_short",
            points: vec![
                point(50_000, 0.0),
                point(60_000, 10.0),
                point(70_000, 20.0),
                point(80_000, 30.0),
            ],
        },
        HistogramSeriesCase {
            name: "k_starts_late",
            points: vec![point(70_000, 5.0), point(80_000, 15.0), point(90_000, 25.0)],
        },
        HistogramSeriesCase {
            name: "k_spans_range",
            points: vec![
                point(45_000, 2.0),
                point(60_000, 4.0),
                point(75_000, 6.0),
                point(95_000, 10.0),
            ],
        },
        // 11.05s to the range end sits between 1.1x the 10s spacing (11.0)
        // and that spacing plus 1.1 (11.1), so the threshold has to be a
        // product rather than a sum for the end to clamp at all.
        HistogramSeriesCase {
            name: "k_thr_edge",
            points: vec![point(68_950, 0.0), point(78_950, 10.0), point(88_950, 20.0)],
        },
    ] {
        for point in case.points {
            store.push_float(
                "tenant-a",
                labels(&[
                    crate::test_support::TestLabelPair::new("__name__", case.name),
                    crate::test_support::TestLabelPair::new("series", "f"),
                ]),
                point.timestamp_ms,
                point.value,
            );
        }
        // One histogram sibling is enough to route the call to the kernel.
        store.push_histogram(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", case.name),
                crate::test_support::TestLabelPair::new("series", "h"),
            ]),
            60_000,
            native_histogram(4.0, 10.0),
        );
    }
    // A float series whose only sample falls before the window contributes
    // nothing to the result.
    store.push_float(
        "tenant-a",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "k_no_points"),
            crate::test_support::TestLabelPair::new("series", "f"),
        ]),
        10_000,
        1.0,
    );
    store.push_histogram(
        "tenant-a",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "k_no_points"),
            crate::test_support::TestLabelPair::new("series", "h"),
        ]),
        60_000,
        native_histogram(4.0, 10.0),
    );
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    for case in [
        QueryExpectation {
            query: "increase(k_ends_short[1m])",
            expected: 35.0,
        },
        QueryExpectation {
            query: "increase(k_starts_late[1m])",
            expected: 35.0,
        },
        QueryExpectation {
            query: "increase(k_spans_range[1m])",
            expected: 9.6,
        },
        QueryExpectation {
            query: "rate(k_spans_range[1m])",
            expected: 0.16,
        },
        // `delta` is not a counter, so the zero-crossing cut never applies and
        // the start clamp stands on its own -- and the whole guard has to be
        // an `&&` chain, since under `||` a non-counter would take the cut.
        QueryExpectation {
            query: "delta(k_thr_edge[1m])",
            expected: 30.0,
        },
    ] {
        let result = engine
            .query_instant(&tenant_id("tenant-a"), case.query, 100_000)
            .await
            .unwrap_or_else(|error| panic!("{}: {error}", case.query));
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected a vector for {}", case.query);
        };
        let float = samples
            .iter()
            .find(|sample| sample.labels.get("series") == Some("f"))
            .unwrap_or_else(|| panic!("{}: no float series in the result", case.query));
        assert2::assert!(
            approx_eq(float_value(&float.value), case.expected),
            "{}",
            case.query
        );
    }

    let QueryResult::InstantVector(samples) = engine
        .query_instant(&tenant_id("tenant-a"), "increase(k_no_points[1m])", 100_000)
        .await
        .expect("a window with no float points is not an error")
    else {
        panic!("expected a vector");
    };
    assert2::assert!(
        !samples
            .iter()
            .any(|sample| sample.labels.get("series") == Some("f")),
        "a float series with no points in the window yields no sample"
    );
}

fn point(timestamp_ms: i64, value: f64) -> HistogramFloatPoint {
    HistogramFloatPoint::builder()
        .timestamp_ms(timestamp_ms)
        .value(value)
        .build()
}

}

mod increase_extrapolates_to_the_range_edges_by_the_sample_spacing {
use super::*;

#[derive(bon::Builder)]
struct SamplePoint {
    timestamp_ms: i64,
    value: f64,
}

#[derive(bon::Builder)]
struct SeriesCase {
    name: &'static str,
    points: Vec<SamplePoint>,
}

struct QueryExpectation {
    query: &'static str,
    want: f64,
}

/// Prometheus extrapolates a counter out to the edges of the range, but only
/// as far as 1.1x the average sample spacing; past that it settles for half a
/// spacing. Each geometry below trips a different part of that: the first
/// clamps at the end and is cut short at the start by the counter's own zero
/// crossing, the second clamps at the start with a counter that never crosses
/// zero, and the third clamps at neither edge. A series that simply spans its
/// range never enters any of them.
#[tokio::test]
pub(crate) async fn increase_extrapolates_to_the_range_edges_by_the_sample_spacing() {
    let mut store = InMemoryMetricStore::new();
    let series = [
        SeriesCase::builder()
            .name("ends_short")
            .points(vec![
                SamplePoint::builder()
                    .timestamp_ms(50_000)
                    .value(0.0)
                    .build(),
                SamplePoint::builder()
                    .timestamp_ms(60_000)
                    .value(10.0)
                    .build(),
                SamplePoint::builder()
                    .timestamp_ms(70_000)
                    .value(20.0)
                    .build(),
                SamplePoint::builder()
                    .timestamp_ms(80_000)
                    .value(30.0)
                    .build(),
            ])
            .build(),
        SeriesCase::builder()
            .name("starts_late")
            .points(vec![
                SamplePoint::builder()
                    .timestamp_ms(70_000)
                    .value(5.0)
                    .build(),
                SamplePoint::builder()
                    .timestamp_ms(80_000)
                    .value(15.0)
                    .build(),
                SamplePoint::builder()
                    .timestamp_ms(90_000)
                    .value(25.0)
                    .build(),
            ])
            .build(),
        SeriesCase::builder()
            .name("spans_range")
            .points(vec![
                SamplePoint::builder()
                    .timestamp_ms(45_000)
                    .value(2.0)
                    .build(),
                SamplePoint::builder()
                    .timestamp_ms(60_000)
                    .value(4.0)
                    .build(),
                SamplePoint::builder()
                    .timestamp_ms(75_000)
                    .value(6.0)
                    .build(),
                SamplePoint::builder()
                    .timestamp_ms(95_000)
                    .value(10.0)
                    .build(),
            ])
            .build(),
    ];
    for series_index in 0..series.len() {
        let series = &series[series_index];
        for point_index in 0..series.points.len() {
            let point = &series.points[point_index];
            store.push_float(
                "tenant-a",
                labels(&[crate::test_support::TestLabelPair::new(
                    "__name__",
                    series.name,
                )]),
                point.timestamp_ms,
                point.value,
            );
        }
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let queries = [
        // 20s to the range end is over 1.1x the 10s spacing, so it clamps to
        // half a spacing; the counter starts at zero, so the extrapolation
        // back to the range start is cut to the zero crossing instead.
        QueryExpectation {
            query: "increase(ends_short[1m])",
            want: 35.0,
        },
        // The mirror: 30s to the range start clamps, 10s to the end does not,
        // and a counter starting above zero leaves the start alone.
        QueryExpectation {
            query: "increase(starts_late[1m])",
            want: 35.0,
        },
        // Neither gap reaches the threshold, so both extrapolate in full.
        QueryExpectation {
            query: "increase(spans_range[1m])",
            want: 9.6,
        },
        // `rate` is that same extrapolation over the range in seconds.
        QueryExpectation {
            query: "rate(spans_range[1m])",
            want: 0.16,
        },
    ];
    for query_index in 0..queries.len() {
        let query = &queries[query_index];
        assert_lone_value_at(
            &engine,
            LoneValueQuery {
                query: query.query,
                time_ms: 100_000,
                want: query.want,
            },
        )
        .await;
    }
}

}

mod increase_recompacts_native_histogram_spans_around_gaps {
use super::*;

/// `increase` over native histograms recompacts the result's spans: buckets
/// that did not move are dropped from both ends, and the gap between the two
/// input spans splits what remains into two runs. Every other histogram-rate
/// test uses a single dense span where the compaction has nothing to do --
/// no edge to strip, no gap to split on, and no second span offset to add in.
#[tokio::test]
pub(crate) async fn increase_recompacts_native_histogram_spans_around_gaps() {
    let histogram = |counts: Vec<f64>| NativeHistogram {
        schema: 0,
        is_float: true,
        reset_hint: ResetHint::No,
        zero_threshold: 0.0,
        zero_count: 0.0,
        count: counts.iter().sum(),
        sum: 0.0,
        positive_spans: vec![
            BucketSpan {
                offset: 0,
                length: 3,
            },
            BucketSpan {
                offset: 2,
                length: 3,
            },
            // A third run, so the second emitted span has a non-zero
            // `previous_span_end` to subtract -- at the first one it is still
            // zero, where subtracting and adding it agree.
            BucketSpan {
                offset: 2,
                length: 3,
            },
        ],
        positive_counts: counts,
        negative_spans: vec![],
        negative_counts: vec![],
        custom_values: None,
        start_timestamp_ms: None,
    };

    let mut store = InMemoryMetricStore::new();
    // The outermost buckets hold still, as does one in the middle of a run.
    store.push_histogram(
        "tenant-a",
        labels(&[crate::test_support::TestLabelPair::new("__name__", "h")]),
        10_000,
        histogram(vec![1.0; 9]),
    );
    store.push_histogram(
        "tenant-a",
        labels(&[crate::test_support::TestLabelPair::new("__name__", "h")]),
        20_000,
        histogram(vec![1.0, 3.0, 4.0, 5.0, 6.0, 1.0, 7.0, 8.0, 1.0]),
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(&tenant_id("tenant-a"), "increase(h[5m])", 20_000)
        .await
        .expect("a histogram increase");
    let QueryResult::InstantVector(samples) = result else {
        panic!("expected a vector");
    };
    assert2::assert!(samples.len() == 1);
    let SampleValue::Histogram(histogram) = &samples[0].value else {
        panic!("expected a histogram sample");
    };

    // The input spans cover 0..=2, 5..=7 and 10..=12. Buckets 0 and 12 hold
    // still and are trimmed; the gaps at 3..=4 and 8..=9 split the rest into
    // three runs. Bucket 7 holds still too but sits inside a run, so it stays.
    check!(
        histogram.positive_spans
            == vec![
                BucketSpan {
                    offset: 1,
                    length: 2
                },
                BucketSpan {
                    offset: 2,
                    length: 3
                },
                BucketSpan {
                    offset: 2,
                    length: 2
                },
            ]
    );
    check!(histogram.positive_counts.len() == 7);
}

}

mod increase_refuses_native_histograms_that_do_not_line_up {
use super::*;

struct HistogramLayoutCase {
    field: &'static str,
    histogram: NativeHistogram,
}

/// `increase` reconciles compatible native-histogram layouts before subtraction.
#[tokio::test]
pub(crate) async fn increase_reconciles_native_histograms_that_do_not_line_up() {
    fn base() -> NativeHistogram {
        NativeHistogram {
            schema: 0,
            is_float: true,
            reset_hint: ResetHint::No,
            zero_threshold: 1.0,
            zero_count: 1.0,
            count: 6.0,
            sum: 10.0,
            positive_spans: vec![BucketSpan {
                offset: 0,
                length: 2,
            }],
            positive_counts: vec![1.0, 2.0],
            negative_spans: vec![BucketSpan {
                offset: 0,
                length: 1,
            }],
            negative_counts: vec![1.0],
            custom_values: None,
            start_timestamp_ms: None,
        }
    }
    fn grown() -> NativeHistogram {
        NativeHistogram {
            count: 12.0,
            sum: 20.0,
            positive_counts: vec![3.0, 4.0],
            negative_counts: vec![2.0],
            ..base()
        }
    }

    let variants = [
        HistogramLayoutCase {
            field: "schema",
            histogram: NativeHistogram {
                schema: 1,
                ..grown()
            },
        },
        HistogramLayoutCase {
            field: "is_float",
            histogram: NativeHistogram {
                is_float: false,
                ..grown()
            },
        },
        HistogramLayoutCase {
            field: "zero_threshold",
            histogram: NativeHistogram {
                zero_threshold: 2.0,
                ..grown()
            },
        },
        HistogramLayoutCase {
            field: "custom_values",
            histogram: NativeHistogram {
                custom_values: Some(vec![1.0]),
                ..grown()
            },
        },
        HistogramLayoutCase {
            field: "positive_spans",
            histogram: NativeHistogram {
                positive_spans: vec![BucketSpan {
                    offset: 1,
                    length: 2,
                }],
                ..grown()
            },
        },
        HistogramLayoutCase {
            field: "negative_spans",
            histogram: NativeHistogram {
                negative_spans: vec![BucketSpan {
                    offset: 1,
                    length: 1,
                }],
                ..grown()
            },
        },
        // The two counts-length clauses cannot be isolated: the store rejects
        // a histogram whose counts disagree with its spans, so equal spans
        // already force equal counts lengths. These grow both together.
        HistogramLayoutCase {
            field: "positive bucket count",
            histogram: NativeHistogram {
                positive_spans: vec![BucketSpan {
                    offset: 0,
                    length: 3,
                }],
                positive_counts: vec![3.0, 4.0, 5.0],
                ..grown()
            },
        },
        HistogramLayoutCase {
            field: "negative bucket count",
            histogram: NativeHistogram {
                negative_spans: vec![BucketSpan {
                    offset: 0,
                    length: 2,
                }],
                negative_counts: vec![2.0, 3.0],
                ..grown()
            },
        },
    ];

    let mut store = InMemoryMetricStore::new();
    for index in 0..variants.len() {
        let variant = &variants[index].histogram;
        let name = format!("hx{index}");
        store.push_histogram(
            "tenant-a",
            labels(&[crate::test_support::TestLabelPair::new("__name__", &name)]),
            10_000,
            base(),
        );
        store.push_histogram(
            "tenant-a",
            labels(&[crate::test_support::TestLabelPair::new("__name__", &name)]),
            20_000,
            variant.clone(),
        );
    }
    // A pair that does line up, so a check stuck at false is caught too.
    store.push_histogram(
        "tenant-a",
        labels(&[crate::test_support::TestLabelPair::new("__name__", "ok")]),
        10_000,
        base(),
    );
    store.push_histogram(
        "tenant-a",
        labels(&[crate::test_support::TestLabelPair::new("__name__", "ok")]),
        20_000,
        grown(),
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for index in 0..variants.len() {
        let field = variants[index].field;
        let query = format!("increase(hx{index}[5m])");
        let QueryResult::InstantVector(samples) = engine
            .query_instant(&tenant_id("tenant-a"), &query, 20_000)
            .await
            .unwrap_or_else(|error| panic!("{field}: {error}"))
        else {
            panic!("expected a vector for {field}");
        };
        assert2::assert!(samples.len() == 1, "{field} is reconciled");
    }

    let QueryResult::InstantVector(samples) = engine
        .query_instant(&tenant_id("tenant-a"), "increase(ok[5m])", 20_000)
        .await
        .expect("a matched pair")
    else {
        panic!("expected a vector");
    };
    assert2::assert!(samples.len() == 1, "a matched pair does produce a sample");
}

}

mod resets_counts_a_drop_in_any_native_histogram_component {
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

}

mod smoothed_delta_extrapolates_only_within_the_sample_interval_slack {
use super::*;

struct SmoothedSample {
    timestamp_ms: i64,
    value: f64,
}

struct DeltaExpectation {
    description: &'static str,
    eval_ms: i64,
    value: f64,
}

/// Smoothed ranges interpolate between observed samples and carry the last
/// observed value at the right boundary. They do not extend the last slope.
#[tokio::test]
pub(crate) async fn smoothed_delta_uses_the_observed_right_boundary() {
    let mut store = InMemoryMetricStore::new();
    for sample in [
        SmoothedSample {
            timestamp_ms: 0,
            value: 0.0,
        },
        SmoothedSample {
            timestamp_ms: 60_000,
            value: 60.0,
        },
    ] {
        store.push_float(
            "tenant-a",
            labels(&[crate::test_support::TestLabelPair::new("__name__", "m")]),
            sample.timestamp_ms,
            sample.value,
        );
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    for case in [
        DeltaExpectation {
            description: "start between samples at 3s",
            eval_ms: 123_000,
            value: 57.0,
        },
        DeltaExpectation {
            description: "start between samples at 6s",
            eval_ms: 126_000,
            value: 54.0,
        },
        DeltaExpectation {
            description: "start between samples at 10s",
            eval_ms: 130_000,
            value: 50.0,
        },
    ] {
        let samples = instant_vector(&engine, "delta(smoothed(m[2m]))", case.eval_ms).await;
        assert2::assert!(samples.len() == 1, "{}", case.description);
        assert2::assert!(
            approx_eq(float_value(&samples[0].value), case.value),
            "{}",
            case.description
        );
    }

    // `delta` stops at the difference; only `rate` divides by the range.
    let QueryResult::InstantVector(samples) = engine
        .query_instant(&tenant_id("tenant-a"), "rate(smoothed(m[3m]))", 123_000)
        .await
        .expect("a smoothed rate")
    else {
        panic!("expected a vector");
    };
    assert2::assert!(approx_eq(float_value(&samples[0].value), 1.0 / 3.0));
}

}


mod created_timestamps_inject_counter_zeros {
use super::*;
use super::*;

#[tokio::test]
pub(crate) async fn created_timestamps_inject_counter_zeros_in_planner_and_interpreter_windows() {
    use promql_parser::parser::Expr;

    use crate::{DurationExprContext, parse_promql_with_duration_context};

    let mut store = InMemoryMetricStore::new();
    let new_counter = labels(&[
        crate::test_support::TestLabelPair::new("__name__", "http_requests_total"),
        crate::test_support::TestLabelPair::new("job", "new"),
    ]);
    store.push_float_with_start_timestamp("t", new_counter, 240_000, 6.0, Some(180_000));

    let reset_counter = labels(&[
        crate::test_support::TestLabelPair::new("__name__", "http_requests_total"),
        crate::test_support::TestLabelPair::new("job", "reset"),
    ]);
    store.push_float_with_start_timestamp("t", reset_counter.clone(), 100_000, 5.0, Some(50_000));
    // The value rose across the restart, so value-only reset detection cannot
    // see it. The changed start timestamp marks a reset without changing the sample spacing.
    store.push_float_with_start_timestamp("t", reset_counter, 200_000, 8.0, Some(150_000));

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for query in [
        "rate(http_requests_total{job=\"new\"}[5m])",
        "increase(http_requests_total{job=\"new\"}[5m])",
    ] {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(300_000))
            .unwrap();
        let Expr::Call(call) = &expr else {
            panic!("expected call");
        };
        let rate_call = match_rate_range_call(&expr).unwrap();
        let interpreter = engine.eval_instant_call("t", call, 300_000).await.unwrap();
        let planner = engine
            .eval_rate_range_via_planner(
                "t",
                MatrixSelectorAt {
                    selector: rate_call.selector,
                    time_ms: 300_000,
                },
                rate_call.kind,
            )
            .await
            .unwrap();

        assert2::assert!(interpreter == planner, "{query}");
        let QueryResult::InstantVector(samples) = planner else {
            panic!("expected vector");
        };
        assert2::assert!(samples.len() == 1, "{query}");
        if query.starts_with("rate") {
            assert2::assert!(approx_eq(float_value(&samples[0].value), 6.0 / 300.0));
        } else {
            assert2::assert!(approx_eq(float_value(&samples[0].value), 6.0));
        }
    }

    let result = engine
        .query_instant(
            &tenant_id("t"),
            "resets(http_requests_total{job=\"reset\"}[5m])",
            300_000,
        )
        .await
        .unwrap();
    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert2::assert!(samples.len() == 1);
    assert2::assert!(approx_eq(float_value(&samples[0].value), 1.0));
}
}
