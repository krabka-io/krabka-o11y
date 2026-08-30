use super::*;

#[tokio::test]
pub(crate) async fn binary_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // NaN-aware sample comparison: labels and ts must match exactly; values
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

    // A float-only store with overlapping label dimensions for vector
    // matching. `left`/`right` share `{job}` for one-to-one and group_x
    // matching; `code` differentiates the many side. A NaN row and a series
    // present only on one side exercise NaN preservation and no-match drops.
    let mut store = InMemoryMetricStore::new();
    for (name, job, code, instance, value) in [
        // `left`: one per job (the "one" side for group_left).
        ("left", "api", "", "", 10.0),
        ("left", "db", "", "", 20.0),
        // `right`: many per job (`code` dimension), the "many" side.
        ("right", "api", "200", "", 1.0),
        ("right", "api", "500", "", 2.0),
        ("right", "db", "200", "", 4.0),
        // A `right` series whose job has no `left` match (no-match drop).
        ("right", "web", "200", "", 8.0),
        // `m1`/`m2` for one-to-one on/ignoring matching.
        ("m1", "api", "", "0", 3.0),
        ("m1", "api", "", "1", 5.0),
        ("m2", "api", "", "0", 7.0),
        ("m2", "api", "", "1", 11.0),
        // A genuine-NaN row that must survive vector∘scalar arithmetic.
        ("nanm", "api", "", "0", f64::NAN),
        ("nanm", "api", "", "1", 13.0),
    ] {
        let mut pairs = vec![("__name__", name), ("job", job)];
        if !code.is_empty() {
            pairs.push(("code", code));
        }
        if !instance.is_empty() {
            pairs.push(("instance", instance));
        }
        store.push_float("t", labels(&pairs), 60_000, value);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let time_ms = 60_000_i64;

    // Each query must route through the operator path and match the
    // interpreter byte-for-byte (NaN-aware).
    let queries = [
        // vector ∘ scalar (arithmetic, drops __name__).
        "m1 + 100",
        "m1 * 2",
        // scalar ∘ vector.
        "100 - m1",
        "2 ^ m1",
        // vector ∘ scalar comparison without bool (filters, keeps labelset).
        "m1 > 4",
        // vector ∘ scalar comparison with bool (keeps all, drops __name__).
        "m1 > bool 4",
        // genuine NaN must survive vector∘scalar arithmetic.
        "nanm + 1",
        // vector ∘ vector one-to-one, default matching (drops __name__).
        "m1 + m2",
        "m2 - m1",
        "m1 / m2",
        "m1 % m2",
        "m1 ^ m2",
        "m1 atan2 m2",
        // one-to-one with on / ignoring.
        "m1 + on(job, instance) m2",
        "m1 + ignoring(__name__) m2",
        // one-to-one comparison without bool (keeps LHS labelset incl. name).
        "m1 > m2",
        "m2 >= m1",
        // one-to-one comparison with bool (drops __name__).
        "m1 == bool m2",
        "m1 != bool m2",
        // group_left (many-to-one): the `right` many side copies a label
        // from the `left` one side.
        "right * on(job) group_left left",
        "right + on(job) group_left() left",
        // group_right (one-to-many): the `left` one side, many `right`.
        "left * on(job) group_right right",
        // set ops: and / or / unless, with and without on/ignoring.
        "m1 and m2",
        "m1 or m2",
        "m1 unless m2",
        "right and on(job) left",
        "right unless on(job) left",
        "left or on(job) right",
        // a no-match set op (web has no left): or keeps it, and/unless drop.
        "right and on(job) left",
    ];

    for query in queries {
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

        let interpreter = normalize(via_interpreter);
        let operators = normalize(via_operators);
        assert2::assert!(samples_match(&interpreter, &operators));
    }

    // Pin specific behaviors the parity above relies on.
    // 1. `__name__` is dropped for arithmetic.
    let arith = engine.query_instant("t", "m1 + m2", time_ms).await.unwrap();
    let QueryResult::InstantVector(arith) = arith else {
        panic!("expected vector");
    };
    assert2::assert!(arith.iter().all(|s| s.labels.get("__name__").is_none()));
    // 2. A comparison without `bool` keeps the LHS labelset (incl. __name__).
    let cmp = engine.query_instant("t", "m1 > m2", time_ms).await.unwrap();
    let QueryResult::InstantVector(cmp) = cmp else {
        panic!("expected vector");
    };
    assert2::assert!(cmp.iter().all(|s| s.labels.get("__name__") == Some("m1")));
    // 3. A no-match set op: `right and on(job) left` drops `web` (no left).
    let setop = engine
        .query_instant("t", "right and on(job) left", time_ms)
        .await
        .unwrap();
    let QueryResult::InstantVector(setop) = setop else {
        panic!("expected vector");
    };
    assert2::assert!(setop.iter().all(|s| s.labels.get("job") != Some("web")));

    // Scalar ∘ scalar now folds through the planner into a scalar planned
    // result; it must route AND match the interpreter's scalar value+ts.
    for (query, expected) in [
        ("1 + 2", 3.0_f64),
        ("3 * 4 - 1", 11.0_f64),
        ("2 > bool 1", 1.0_f64),
    ] {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap();
        let planned = engine
            .plan_instant_expr("t", &expr, time_ms)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("scalar∘scalar `{query}` must route through the planner"));
        let via_operators = engine
            .assemble_planned_instant(planned, time_ms)
            .await
            .unwrap();
        let QueryResult::Scalar { ts_ms, value } = via_operators else {
            panic!("expected scalar for `{query}`");
        };
        assert2::assert!(ts_ms == time_ms);
        assert2::assert!(value.to_bits() == expected.to_bits());
    }
}
