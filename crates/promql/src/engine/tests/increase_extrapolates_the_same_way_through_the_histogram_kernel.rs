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
