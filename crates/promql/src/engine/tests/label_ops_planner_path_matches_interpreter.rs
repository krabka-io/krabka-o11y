use super::*;

#[derive(bon::Builder)]
struct LabelOpsFloatSample {
    labels: Labels,
    value: f64,
}

struct LabelOpsQueryCase {
    query: &'static str,
    time_ms: i64,
}

#[tokio::test]
pub(crate) async fn label_ops_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // A float-only store: a multi-label gauge (with a `src` label for
    // capture-group expansion), a genuine-NaN series (must survive the
    // operator path and sort last), and an up-like metric for the nested
    // aggregate case.
    let mut store = InMemoryMetricStore::new();
    for sample in [
        LabelOpsFloatSample::builder()
            .labels(labels(&[
                crate::test_support::TestLabelPair::new("__name__", "g"),
                crate::test_support::TestLabelPair::new("l", "x"),
                crate::test_support::TestLabelPair::new("src", "a-1"),
            ]))
            .value(3.0)
            .build(),
        LabelOpsFloatSample::builder()
            .labels(labels(&[
                crate::test_support::TestLabelPair::new("__name__", "g"),
                crate::test_support::TestLabelPair::new("l", "y"),
                crate::test_support::TestLabelPair::new("src", "b-2"),
            ]))
            .value(1.0)
            .build(),
        LabelOpsFloatSample::builder()
            .labels(labels(&[
                crate::test_support::TestLabelPair::new("__name__", "g"),
                crate::test_support::TestLabelPair::new("l", "z"),
                crate::test_support::TestLabelPair::new("src", "c-3"),
            ]))
            .value(f64::NAN)
            .build(),
    ] {
        store.push_float("t", sample.labels, 60_000, sample.value);
    }
    for job in ["api", "db"] {
        store.push_float(
            "t",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "up"),
                crate::test_support::TestLabelPair::new("job", job),
            ]),
            60_000,
            1.0,
        );
    }
    // Two `h` series differing only in label `a`; overwriting `a` to a
    // constant collapses them onto the same labelset (the collision case).
    for sample in [
        LabelOpsFloatSample::builder()
            .labels(labels(&[
                crate::test_support::TestLabelPair::new("__name__", "h"),
                crate::test_support::TestLabelPair::new("a", "1"),
                crate::test_support::TestLabelPair::new("b", "p"),
            ]))
            .value(10.0)
            .build(),
        LabelOpsFloatSample::builder()
            .labels(labels(&[
                crate::test_support::TestLabelPair::new("__name__", "h"),
                crate::test_support::TestLabelPair::new("a", "2"),
                crate::test_support::TestLabelPair::new("b", "p"),
            ]))
            .value(20.0)
            .build(),
    ] {
        store.push_float("t", sample.labels, 60_000, sample.value);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    // Representative label-ops queries over a plannable inner vector. Each
    // must route through the operator path and match the interpreter exactly,
    // covering: capture-group `$1` expansion, no-match passthrough,
    // delete-via-empty-replacement, multi-source label_join + separator,
    // sort/sort_desc (incl. the genuine-NaN row), and a nested aggregate.
    let queries = [
        // Capture group: `src="a-1"` -> `dst="a"`.
        LabelOpsQueryCase {
            query: r#"label_replace(g, "dst", "$1", "src", "(.*)-.*")"#,
            time_ms: 60_000,
        },
        // No match (`src` has no digit-only-prefix form here): unchanged.
        LabelOpsQueryCase {
            query: r#"label_replace(g, "dst", "$1", "src", "(\\d+)")"#,
            time_ms: 60_000,
        },
        // Empty replacement writes `dst=""` (the interpreter keeps it).
        LabelOpsQueryCase {
            query: r#"label_replace(g, "dst", "", "src", ".*")"#,
            time_ms: 60_000,
        },
        // Replace the metric name itself (label_replace does not drop it).
        LabelOpsQueryCase {
            query: r#"label_replace(g, "__name__", "renamed", "l", "(.+)")"#,
            time_ms: 60_000,
        },
        // label_join: multi-source with a separator.
        LabelOpsQueryCase {
            query: r#"label_join(g, "dst", "/", "l", "src")"#,
            time_ms: 60_000,
        },
        // label_join with a single source and empty separator.
        LabelOpsQueryCase {
            query: r#"label_join(g, "dst", "", "l")"#,
            time_ms: 60_000,
        },
        // sort / sort_desc over a bare selector, including the NaN row
        // (which the NaN-preserving inner sourcing must keep and place last).
        LabelOpsQueryCase {
            query: "sort(g)",
            time_ms: 60_000,
        },
        LabelOpsQueryCase {
            query: "sort_desc(g)",
            time_ms: 60_000,
        },
        // Nested compositional case: sort over an aggregate (NaN-free `up`,
        // so the aggregate operator path matches the interpreter exactly).
        LabelOpsQueryCase {
            query: "sort(sum by (job) (up))",
            time_ms: 60_000,
        },
        // label_replace over a nested aggregate (operator inner).
        LabelOpsQueryCase {
            query: r#"label_replace(sum by (job) (up), "tag", "$1", "job", "(.+)")"#,
            time_ms: 60_000,
        },
        // Binary operands are now planner-supported, so label-ops over a
        // binary inner expression route through operators and must match the
        // interpreter (note: `g + 1` drops `__name__`, so `l`/`src` survive).
        LabelOpsQueryCase {
            query: r#"label_join(g + 1, "dst", "/", "l")"#,
            time_ms: 60_000,
        },
        LabelOpsQueryCase {
            query: "sort(g + 1)",
            time_ms: 60_000,
        },
        // sort_by_label / sort_by_label_desc over a bare selector: order by the
        // `l` label values, then by remaining labels (the canonical key
        // tiebreak). Order-sensitive (the comparator below treats `sort*`
        // queries as ordered).
        LabelOpsQueryCase {
            query: r#"sort_by_label(g, "l")"#,
            time_ms: 60_000,
        },
        LabelOpsQueryCase {
            query: r#"sort_by_label_desc(g, "l")"#,
            time_ms: 60_000,
        },
        // Multi-label sort_by_label: tie on `l` would fall through to `src`.
        LabelOpsQueryCase {
            query: r#"sort_by_label(g, "l", "src")"#,
            time_ms: 60_000,
        },
        // sort_by_label over a nested aggregate (operator inner).
        LabelOpsQueryCase {
            query: r#"sort_by_label(sum by (job) (up), "job")"#,
            time_ms: 60_000,
        },
    ];

    for case in queries {
        let query = case.query;
        let parity = planned_and_interpreted(&engine, query, case.time_ms).await;
        let via_operators = parity.via_operators;
        let via_interpreter = parity.via_interpreter;

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
        assert2::assert!(nan_equal_samples(&interpreter, &operators));
    }

    // A `label_replace` that collapses two series onto the same labelset must
    // error identically through the operator path and the interpreter. The
    // top-level uniqueness check enforces this for both (`query_instant`).
    let collision = r#"label_replace(h, "a", "same", "a", ".*")"#;
    let operator_err = engine
        .query_instant(&tenant_id("t"), collision, 60_000)
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
    // planner claims them, including calls with no label-name argument.
    for query in [r#"sort_by_label(g, "l")"#, r#"sort_by_label_desc(g, "l")"#] {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(60_000))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
        let planned = engine
            .plan_instant_expr("t", &expr, 60_000)
            .await
            .unwrap_or_else(|error| panic!("plan `{query}`: {error}"));
        assert2::assert!(planned.is_some());
    }
    // An omitted label list sorts by the full labels as the tie breaker.
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
            .is_some()
    );
}
