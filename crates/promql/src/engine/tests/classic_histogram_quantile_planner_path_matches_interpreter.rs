use super::*;

#[tokio::test]
pub(crate) async fn classic_histogram_quantile_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

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
    for (job, le, value) in [
        ("api", "0.1", 1.0),
        ("api", "0.2", 2.0),
        ("api", "0.4", 4.0),
        ("api", "+Inf", 5.0),
        ("db", "0.1", 0.0),
        ("db", "0.2", 1.0),
        ("db", "0.4", 3.0),
        ("db", "+Inf", 3.0),
    ] {
        let lbls = labels(&[("__name__", "lat_bucket"), ("job", job), ("le", le)]);
        store.push_float("t", lbls, 300_000, value);
    }
    // A non-monotonic cumulative bucket set: le=2 (count 3) dips below le=1
    // (count 5); the fold must force monotonicity before interpolating.
    for (le, value) in [("1", 5.0), ("2", 3.0), ("+Inf", 8.0)] {
        let lbls = labels(&[("__name__", "nonmono_bucket"), ("le", le)]);
        store.push_float("t", lbls, 300_000, value);
    }
    // A single `+Inf` bucket: fewer than two buckets -> NaN.
    store.push_float(
        "t",
        labels(&[("__name__", "inf_only_bucket"), ("le", "+Inf")]),
        300_000,
        7.0,
    );
    // Counters for the nested `histogram_quantile(.., sum by (le) (rate(...)))`
    // case (slope = factor / 60s within the 5m window).
    for (le, factor) in [("0.1", 1.0), ("0.2", 2.0), ("0.4", 4.0), ("+Inf", 5.0)] {
        let lbls = labels(&[("__name__", "reqs_bucket"), ("le", le)]);
        for step in 0..=5_i32 {
            store.push_float(
                "t",
                lbls.clone(),
                i64::from(step) * 60_000,
                f64::from(step) * factor,
            );
        }
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    // Each query must route through the operator path and match the
    // interpreter byte-for-byte (NaN-aware, bit-exact).
    let queries = [
        // Normal linear interpolation, multi-group (job=api / job=db), with the
        // `__name__` and `le` labels dropped from the output.
        ("histogram_quantile(0.5, lat_bucket)", 300_000_i64),
        ("histogram_quantile(0.9, lat_bucket)", 300_000),
        // phi at the boundaries 0 and 1.
        ("histogram_quantile(0, lat_bucket)", 300_000),
        ("histogram_quantile(1, lat_bucket)", 300_000),
        // phi out of [0, 1]: -Inf below, +Inf above.
        ("histogram_quantile(-0.5, lat_bucket)", 300_000),
        ("histogram_quantile(1.5, lat_bucket)", 300_000),
        // A non-monotonic cumulative bucket set is forced monotonic first.
        ("histogram_quantile(0.5, nonmono_bucket)", 300_000),
        // A single `+Inf` bucket (<2 buckets) yields NaN.
        ("histogram_quantile(0.5, inf_only_bucket)", 300_000),
        // NESTED: a fully-float inner that plans through the rate + aggregate
        // operators, then the classic fold over the assembled bucket vector.
        (
            "histogram_quantile(0.9, sum by (le) (rate(reqs_bucket[5m])))",
            300_000,
        ),
    ];

    for (query, time_ms) in queries {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

        // Operator path: the recursive planner must claim this query.
        let plan = engine
            .plan_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
            .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
        let via_operators = engine
            .assemble_planned_instant(plan, time_ms)
            .await
            .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));

        // Interpreter path: evaluate the same expression directly.
        let via_interpreter = engine
            .eval_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));

        let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
            let QueryResult::InstantVector(mut samples) = result else {
                panic!("expected vector for `{query}`");
            };
            samples.sort_by_key(|sample| sample.labels.fingerprint());
            samples
        };

        let via_interpreter = normalize(via_interpreter);
        let via_operators = normalize(via_operators);
        assert2::assert!(instant_samples_match(&via_interpreter, &via_operators));

        // Pin the `__name__` + `le` drop on the operator-path output.
        assert2::assert!(via_operators.iter().all(|sample| {
            sample.labels.get("__name__").is_none() && sample.labels.get("le").is_none()
        }));
    }
    // The native-histogram flavor of these folds (bare selector, native
    // `histogram_quantile`, the native accessors) now routes through the
    // planner too — see `native_histogram_planner_path_matches_interpreter`.
}
