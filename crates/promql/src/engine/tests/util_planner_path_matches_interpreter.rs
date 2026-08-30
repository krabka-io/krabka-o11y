use super::*;

#[tokio::test]
pub(crate) async fn util_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // NaN-aware vector comparison: labels + ts must match exactly; values
    // match when bit-equal or both NaN (Prometheus treats all NaNs alike).
    fn samples_match(left: &[crate::InstantSample], right: &[crate::InstantSample]) -> bool {
        if left.len() != right.len() {
            return false;
        }
        left.iter().zip(right).all(|(a, b)| {
            a.labels == b.labels
                && a.ts_ms == b.ts_ms
                && match (&a.value, &b.value) {
                    (SampleValue::Float(x), SampleValue::Float(y)) => {
                        x.to_bits() == y.to_bits() || (x.is_nan() && y.is_nan())
                    }
                    _ => false,
                }
        })
    }

    // NaN-aware whole-result comparison covering both scalar and vector
    // results, sorting vector samples by fingerprint first.
    fn results_match(left: QueryResult, right: QueryResult) -> bool {
        match (left, right) {
            (
                QueryResult::Scalar {
                    ts_ms: lt,
                    value: lv,
                },
                QueryResult::Scalar {
                    ts_ms: rt,
                    value: rv,
                },
            ) => lt == rt && (lv.to_bits() == rv.to_bits() || (lv.is_nan() && rv.is_nan())),
            (QueryResult::InstantVector(mut l), QueryResult::InstantVector(mut r)) => {
                l.sort_by_key(|sample| sample.labels.fingerprint());
                r.sort_by_key(|sample| sample.labels.fingerprint());
                samples_match(&l, &r)
            }
            _ => false,
        }
    }

    // A float-only store. `m{job}` carries distinct timestamps per series so
    // `timestamp(m)` differs per row; a single-series metric `solo` exercises
    // `scalar(single)`; `dup` (two series) exercises `scalar(multi)->NaN`. A
    // genuine-NaN row survives `timestamp`/calendar drops. `present` exists,
    // `gone` does not (for absent / absent_over_time).
    let mut store = InMemoryMetricStore::new();
    for (name, job, ts, value) in [
        // Two `m` series at different timestamps within the lookback window.
        ("m", "api", 30_000_i64, 100.0),
        ("m", "db", 60_000, 1_700_000_000.0),
        // A genuine-NaN `m` row (must survive timestamp/calendar, value-> ts).
        ("m", "nan", 45_000, f64::NAN),
        // A single-series metric for scalar(single).
        ("solo", "x", 60_000, 42.5),
        // Two series sharing a name for scalar(multi)->NaN.
        ("dup", "a", 60_000, 1.0),
        ("dup", "b", 60_000, 2.0),
        // A present series for absent(present)->empty / absent_over_time.
        ("present", "p", 55_000, 7.0),
    ] {
        store.push_float("t", labels(&[("__name__", name), ("job", job)]), ts, value);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let time_ms = 60_000_i64;

    // Each query must route through the operator path AND match the
    // interpreter (NaN-aware), covering both vector and scalar results.
    let queries = [
        // Vector-returning utilities over a plannable inner.
        "timestamp(m)",
        "timestamp(solo)",
        "day_of_week(m)",
        "day_of_month(m)",
        "day_of_year(m)",
        "days_in_month(m)",
        "hour(m)",
        "minute(m)",
        "month(m)",
        "year(m)",
        // vector(scalar) yields a single no-label series.
        "vector(42)",
        "vector(time())",
        // absent / absent_over_time, present and missing.
        "absent(present)",
        "absent(gone)",
        "absent(gone{job=\"z\"})",
        "absent_over_time(present[5m])",
        "absent_over_time(gone[5m])",
        "absent_over_time(gone{job=\"z\"}[5m])",
        // Scalar-returning utilities.
        "time()",
        "pi()",
        "scalar(solo)",
        "scalar(dup)",
        // Argless calendar forms operate on time().
        "hour()",
        "year()",
        // scalar∘scalar arithmetic folds to a scalar.
        "2 + 3 * 4",
        // calendar over a scalar-arg utility (vector arg).
        "timestamp(vector(1700000000))",
    ];

    for query in queries {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

        let plan = engine
            .plan_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
            .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
        let via_operators = engine
            .assemble_planned_instant(plan, time_ms)
            .await
            .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));
        let via_interpreter = engine
            .eval_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));

        assert2::assert!(results_match(
            via_interpreter.clone(),
            via_operators.clone()
        ));
    }

    // Pin specific behaviors the parity above relies on.
    // 1. scalar(single) returns the lone value; scalar(multi) returns NaN.
    let QueryResult::Scalar { value: single, .. } = engine
        .query_instant("t", "scalar(solo)", time_ms)
        .await
        .unwrap()
    else {
        panic!("expected scalar");
    };
    assert2::assert!(single.to_bits() == 42.5_f64.to_bits());
    let QueryResult::Scalar { value: multi, .. } = engine
        .query_instant("t", "scalar(dup)", time_ms)
        .await
        .unwrap()
    else {
        panic!("expected scalar");
    };
    assert2::assert!(multi.is_nan());

    // 2. time() and pi() are the eval-time seconds and π.
    let QueryResult::Scalar {
        ts_ms: returned_ts,
        value: eval_seconds,
    } = engine.query_instant("t", "time()", time_ms).await.unwrap()
    else {
        panic!("expected scalar");
    };
    assert2::assert!(returned_ts == time_ms);
    assert2::assert!(eval_seconds.to_bits() == 60.0_f64.to_bits());
    let QueryResult::Scalar { value: pi_v, .. } =
        engine.query_instant("t", "pi()", time_ms).await.unwrap()
    else {
        panic!("expected scalar");
    };
    assert2::assert!(pi_v.to_bits() == std::f64::consts::PI.to_bits());

    // 3. absent(present) is empty; absent(gone{job="z"}) carries the matcher
    //    label and value 1.
    let QueryResult::InstantVector(present) = engine
        .query_instant("t", "absent(present)", time_ms)
        .await
        .unwrap()
    else {
        panic!("expected vector");
    };
    assert2::assert!(present.is_empty());
    let QueryResult::InstantVector(gone) = engine
        .query_instant("t", "absent(gone{job=\"z\"})", time_ms)
        .await
        .unwrap()
    else {
        panic!("expected vector");
    };
    assert2::assert!(gone.len() == 1);
    assert2::assert!(gone[0].labels.get("job") == Some("z"));
    assert2::assert!(gone[0].labels.get("__name__") == None);
    assert2::assert!(float_value(&gone[0].value).to_bits() == 1.0_f64.to_bits());

    // 4. timestamp(m) reports each sample's own timestamp in seconds, not the
    //    eval time, and drops __name__.
    let QueryResult::InstantVector(ts_samples) = engine
        .query_instant("t", "timestamp(m)", time_ms)
        .await
        .unwrap()
    else {
        panic!("expected vector");
    };
    assert2::assert!(
        ts_samples
            .iter()
            .all(|s| s.labels.get("__name__").is_none())
    );
    let by_job: std::collections::BTreeMap<&str, f64> = ts_samples
        .iter()
        .map(|s| (s.labels.get("job").unwrap(), float_value(&s.value)))
        .collect();
    for (job, want) in [("api", 30.0_f64), ("db", 60.0), ("nan", 45.0)] {
        check!(
            by_job[&job].to_bits() == want.to_bits(),
            "timestamp {job} row"
        );
    }
}
