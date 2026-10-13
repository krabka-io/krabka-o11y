use super::*;

struct ClassicBucketCase {
    job: &'static str,
    boundary: &'static str,
    count: f64,
}

struct CounterBucketCase {
    boundary: &'static str,
    factor: f64,
}

struct QuantileQueryCase {
    query: &'static str,
    time_ms: i64,
    retains_name_until_boundary: bool,
}

#[tokio::test]
pub(crate) async fn classic_histogram_quantile_planner_path_matches_interpreter() {
    // A float-only store of classic `<metric>_bucket{le}` series exercising the
    // classic histogram_quantile fold:
    //  - `lat_bucket{job}`: a well-formed monotonic histogram with a real `+Inf`
    //    overflow bucket, in two groups (job=api / job=db) so the multi-group
    //    case and the `__name__` + `le` drop are both observable.
    //  - `nonmono_bucket`: a NON-monotonic cumulative bucket set (the le=2 count
    //    dips below le=1) so the monotonicity-forcing path is taken.
    //  - `inf_only_bucket`: a single `+Inf` bucket (<2 buckets -> NaN).
    //  - `reqs_bucket{le}`: counters for the NESTED
    //    `histogram_quantile(0.9, sum by (le) (rate(reqs_bucket[5m])))` case,
    //    whose fully-float inner plans through the rate + aggregate operators.
    let mut store = InMemoryMetricStore::new();
    for sample in [
        ClassicBucketCase {
            job: "api",
            boundary: "0.1",
            count: 1.0,
        },
        ClassicBucketCase {
            job: "api",
            boundary: "0.2",
            count: 2.0,
        },
        ClassicBucketCase {
            job: "api",
            boundary: "0.4",
            count: 4.0,
        },
        ClassicBucketCase {
            job: "api",
            boundary: "+Inf",
            count: 5.0,
        },
        ClassicBucketCase {
            job: "db",
            boundary: "0.1",
            count: 0.0,
        },
        ClassicBucketCase {
            job: "db",
            boundary: "0.2",
            count: 1.0,
        },
        ClassicBucketCase {
            job: "db",
            boundary: "0.4",
            count: 3.0,
        },
        ClassicBucketCase {
            job: "db",
            boundary: "+Inf",
            count: 3.0,
        },
    ] {
        let lbls = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "lat_bucket"),
            crate::test_support::TestLabelPair::new("job", sample.job),
            crate::test_support::TestLabelPair::new("le", sample.boundary),
        ]);
        store.push_float("t", lbls, 300_000, sample.count);
    }
    // A non-monotonic cumulative bucket set: le=2 (count 3) dips below le=1
    // (count 5); the fold must force monotonicity before interpolating.
    for sample in [
        CounterBucketCase {
            boundary: "1",
            factor: 5.0,
        },
        CounterBucketCase {
            boundary: "2",
            factor: 3.0,
        },
        CounterBucketCase {
            boundary: "+Inf",
            factor: 8.0,
        },
    ] {
        let lbls = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "nonmono_bucket"),
            crate::test_support::TestLabelPair::new("le", sample.boundary),
        ]);
        store.push_float("t", lbls, 300_000, sample.factor);
    }
    // A single `+Inf` bucket: fewer than two buckets -> NaN.
    store.push_float(
        "t",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "inf_only_bucket"),
            crate::test_support::TestLabelPair::new("le", "+Inf"),
        ]),
        300_000,
        7.0,
    );
    // Counters for the nested `histogram_quantile(.., sum by (le) (rate(...)))`
    // case (slope = factor / 60s within the 5m window).
    for sample in [
        CounterBucketCase {
            boundary: "0.1",
            factor: 1.0,
        },
        CounterBucketCase {
            boundary: "0.2",
            factor: 2.0,
        },
        CounterBucketCase {
            boundary: "0.4",
            factor: 4.0,
        },
        CounterBucketCase {
            boundary: "+Inf",
            factor: 5.0,
        },
    ] {
        let lbls = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "reqs_bucket"),
            crate::test_support::TestLabelPair::new("le", sample.boundary),
        ]);
        push_float_counter_series(&mut store, lbls, sample.factor, 5);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    // Each query must route through the operator path and match the
    // interpreter byte-for-byte (NaN-aware, bit-exact).
    let queries = [
        // Normal linear interpolation, multi-group (job=api / job=db), with the
        // `__name__` and `le` labels dropped from the output.
        QuantileQueryCase {
            query: "histogram_quantile(0.5, lat_bucket)",
            time_ms: 300_000,
            retains_name_until_boundary: true,
        },
        QuantileQueryCase {
            query: "histogram_quantile(0.9, lat_bucket)",
            time_ms: 300_000,
            retains_name_until_boundary: true,
        },
        // phi at the boundaries 0 and 1.
        QuantileQueryCase {
            query: "histogram_quantile(0, lat_bucket)",
            time_ms: 300_000,
            retains_name_until_boundary: true,
        },
        QuantileQueryCase {
            query: "histogram_quantile(1, lat_bucket)",
            time_ms: 300_000,
            retains_name_until_boundary: true,
        },
        // phi out of [0, 1]: -Inf below, +Inf above.
        QuantileQueryCase {
            query: "histogram_quantile(-0.5, lat_bucket)",
            time_ms: 300_000,
            retains_name_until_boundary: true,
        },
        QuantileQueryCase {
            query: "histogram_quantile(1.5, lat_bucket)",
            time_ms: 300_000,
            retains_name_until_boundary: true,
        },
        // A non-monotonic cumulative bucket set is forced monotonic first.
        QuantileQueryCase {
            query: "histogram_quantile(0.5, nonmono_bucket)",
            time_ms: 300_000,
            retains_name_until_boundary: true,
        },
        // A single `+Inf` bucket (<2 buckets) yields NaN.
        QuantileQueryCase {
            query: "histogram_quantile(0.5, inf_only_bucket)",
            time_ms: 300_000,
            retains_name_until_boundary: true,
        },
        // NESTED: a fully-float inner that plans through the rate + aggregate
        // operators, then the classic fold over the assembled bucket vector.
        QuantileQueryCase {
            query: "histogram_quantile(0.9, sum by (le) (rate(reqs_bucket[5m])))",
            time_ms: 300_000,
            retains_name_until_boundary: false,
        },
    ];

    for case in queries {
        let SortedParity {
            via_operators,
            via_interpreter,
        } = sorted_planned_and_interpreted(&engine, case.query, case.time_ms).await;
        assert2::assert!(instant_samples_match(&via_interpreter, &via_operators));

        // `le` is gone immediately; `__name__` remains pending until the public
        // query boundary.
        assert2::assert!(via_operators.iter().all(|sample| {
            sample.labels.get("__name__").is_some() == case.retains_name_until_boundary
                && sample.labels.get("le").is_none()
                && sample.drop_name
        }));
    }
    // The native-histogram flavor of these folds (bare selector, native
    // `histogram_quantile`, the native accessors) now routes through the
    // planner too — see `native_histogram_planner_path_matches_interpreter`.
}
