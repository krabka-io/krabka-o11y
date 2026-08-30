use super::*;

#[tokio::test]
pub(crate) async fn label_ops_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // NaN-aware sample comparison: labels and ts must match exactly; values
    // match when bit-equal or both NaN.
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

    // A float-only store: a multi-label gauge (with a `src` label for
    // capture-group expansion), a genuine-NaN series (must survive the
    // operator path and sort last), and an up-like metric for the nested
    // aggregate case.
    let mut store = InMemoryMetricStore::new();
    for (lbls, value) in [
        (
            labels(&[("__name__", "g"), ("l", "x"), ("src", "a-1")]),
            3.0,
        ),
        (
            labels(&[("__name__", "g"), ("l", "y"), ("src", "b-2")]),
            1.0,
        ),
        (
            labels(&[("__name__", "g"), ("l", "z"), ("src", "c-3")]),
            f64::NAN,
        ),
    ] {
        store.push_float("t", lbls, 60_000, value);
    }
    for (job, value) in [("api", 1.0), ("db", 1.0)] {
        store.push_float(
            "t",
            labels(&[("__name__", "up"), ("job", job)]),
            60_000,
            value,
        );
    }
    // Two `h` series differing only in label `a`; overwriting `a` to a
    // constant collapses them onto the same labelset (the collision case).
    for (a, value) in [("1", 10.0), ("2", 20.0)] {
        store.push_float(
            "t",
            labels(&[("__name__", "h"), ("a", a), ("b", "p")]),
            60_000,
            value,
        );
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    // Representative label-ops queries over a plannable inner vector. Each
    // must route through the operator path and match the interpreter exactly,
    // covering: capture-group `$1` expansion, no-match passthrough,
    // delete-via-empty-replacement, multi-source label_join + separator,
    // sort/sort_desc (incl. the genuine-NaN row), and a nested aggregate.
    let queries = [
        // Capture group: `src="a-1"` -> `dst="a"`.
        (
            r#"label_replace(g, "dst", "$1", "src", "(.*)-.*")"#,
            60_000_i64,
        ),
        // No match (`src` has no digit-only-prefix form here): unchanged.
        (r#"label_replace(g, "dst", "$1", "src", "(\\d+)")"#, 60_000),
        // Empty replacement writes `dst=""` (the interpreter keeps it).
        (r#"label_replace(g, "dst", "", "src", ".*")"#, 60_000),
        // Replace the metric name itself (label_replace does not drop it).
        (
            r#"label_replace(g, "__name__", "renamed", "l", "(.+)")"#,
            60_000,
        ),
        // label_join: multi-source with a separator.
        (r#"label_join(g, "dst", "/", "l", "src")"#, 60_000),
        // label_join with a single source and empty separator.
        (r#"label_join(g, "dst", "", "l")"#, 60_000),
        // sort / sort_desc over a bare selector, including the NaN row
        // (which the NaN-preserving inner sourcing must keep and place last).
        ("sort(g)", 60_000),
        ("sort_desc(g)", 60_000),
        // Nested compositional case: sort over an aggregate (NaN-free `up`,
        // so the aggregate operator path matches the interpreter exactly).
        ("sort(sum by (job) (up))", 60_000),
        // label_replace over a nested aggregate (operator inner).
        (
            r#"label_replace(sum by (job) (up), "tag", "$1", "job", "(.+)")"#,
            60_000,
        ),
        // Binary operands are now planner-supported, so label-ops over a
        // binary inner expression route through operators and must match the
        // interpreter (note: `g + 1` drops `__name__`, so `l`/`src` survive).
        (r#"label_join(g + 1, "dst", "/", "l")"#, 60_000),
        ("sort(g + 1)", 60_000),
        // sort_by_label / sort_by_label_desc over a bare selector: order by the
        // `l` label values, then by remaining labels (the canonical key
        // tiebreak). Order-sensitive (the comparator below treats `sort*`
        // queries as ordered).
        (r#"sort_by_label(g, "l")"#, 60_000),
        (r#"sort_by_label_desc(g, "l")"#, 60_000),
        // Multi-label sort_by_label: tie on `l` would fall through to `src`.
        (r#"sort_by_label(g, "l", "src")"#, 60_000),
        // sort_by_label over a nested aggregate (operator inner).
        (r#"sort_by_label(sum by (job) (up), "job")"#, 60_000),
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

        // `sort`/`sort_desc` assert ordering, so compare order-sensitively for
        // them and fingerprint-normalize the unordered label-rewrites.
        let ordered = query.starts_with("sort");
        let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
            let QueryResult::InstantVector(mut samples) = result else {
                panic!("expected vector for `{query}`");
            };
            if !ordered {
                samples.sort_by(|left, right| {
                    left.labels.fingerprint().cmp(&right.labels.fingerprint())
                });
            }
            samples
        };

        let interpreter = normalize(via_interpreter);
        let operators = normalize(via_operators);
        assert2::assert!(samples_match(&interpreter, &operators));
    }

    // A `label_replace` that collapses two series onto the same labelset must
    // error identically through the operator path and the interpreter. The
    // top-level uniqueness check enforces this for both (`query_instant`).
    let collision = r#"label_replace(h, "a", "same", "a", ".*")"#;
    let operator_err = engine
        .query_instant("t", collision, 60_000)
        .await
        .expect_err("collision must error through the operator path");
    assert2::assert!(matches!(operator_err, PromqlError::Exec(_)));
    // Confirm the operator path actually claimed the collision query (so the
    // error came from the operator path, not an interpreter fallback).
    let collision_expr =
        parse_promql_with_duration_context(collision, DurationExprContext::instant(60_000))
            .unwrap();
    assert2::assert!(
        engine
            .plan_instant_expr("t", &collision_expr, 60_000)
            .await
            .unwrap()
            .is_some()
    );

    // `sort_by_label` / `sort_by_label_desc` now route through the operator
    // path (differentially checked in the `queries` list above); pin that the
    // planner claims them and falls back on a missing label-name argument.
    for query in [r#"sort_by_label(g, "l")"#, r#"sort_by_label_desc(g, "l")"#] {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(60_000))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
        let planned = engine
            .plan_instant_expr("t", &expr, 60_000)
            .await
            .unwrap_or_else(|error| panic!("plan `{query}`: {error}"));
        assert2::assert!(planned.is_some());
    }
    // `sort_by_label(g)` with no label-name argument falls back so the
    // interpreter raises the canonical arity error.
    let no_label = parse_promql_with_duration_context(
        "sort_by_label(g)",
        DurationExprContext::instant(60_000),
    )
    .unwrap();
    assert2::assert!(
        engine
            .plan_instant_expr("t", &no_label, 60_000)
            .await
            .unwrap()
            .is_none()
    );
}
