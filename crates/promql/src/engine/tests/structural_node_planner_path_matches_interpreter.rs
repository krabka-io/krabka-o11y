use super::*;

#[derive(bon::Builder)]
struct FloatSampleCase {
    labels: krabka_blockstore::Labels,
    timestamp_ms: i64,
    value: f64,
}

struct QueryResultTypeCase {
    query: &'static str,
    time_ms: i64,
    expected_type: &'static str,
}

/// Differential parity for the top-level structural node kinds.
///
/// The node kinds are unary negation, a bare numeric literal, a bare string
/// literal, a raw matrix selector, a subquery, and the `smoothed` extended
/// selector. `query_instant` returns a `RangeMatrix` for the raw matrix
/// selector and for the subquery. Through the operator planner, each kind must
/// produce the byte-exact result that the interpreter's `eval_instant_expr`
/// produces.
#[tokio::test]
pub(crate) async fn structural_node_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    let mut store = InMemoryMetricStore::new();
    let stale_bits = stale_nan();
    for sample in [
        FloatSampleCase::builder()
            .labels(labels(&[
                crate::test_support::TestLabelPair::new("__name__", "m"),
                crate::test_support::TestLabelPair::new("job", "api"),
            ]))
            .timestamp_ms(60_000)
            .value(2.0)
            .build(),
        FloatSampleCase::builder()
            .labels(labels(&[
                crate::test_support::TestLabelPair::new("__name__", "m"),
                crate::test_support::TestLabelPair::new("job", "api"),
            ]))
            .timestamp_ms(120_000)
            .value(3.0)
            .build(),
        FloatSampleCase::builder()
            .labels(labels(&[
                crate::test_support::TestLabelPair::new("__name__", "m"),
                crate::test_support::TestLabelPair::new("job", "db"),
            ]))
            .timestamp_ms(120_000)
            .value(7.0)
            .build(),
        // A genuine NaN latest-in-window sample (kept, negated to NaN).
        FloatSampleCase::builder()
            .labels(labels(&[
                crate::test_support::TestLabelPair::new("__name__", "m"),
                crate::test_support::TestLabelPair::new("job", "nan"),
            ]))
            .timestamp_ms(120_000)
            .value(f64::NAN)
            .build(),
        // A stale marker (dropped on both paths).
        FloatSampleCase::builder()
            .labels(labels(&[
                crate::test_support::TestLabelPair::new("__name__", "m"),
                crate::test_support::TestLabelPair::new("job", "stale"),
            ]))
            .timestamp_ms(120_000)
            .value(stale_bits)
            .build(),
        // A series with a short history for the matrix / subquery / smoothed shapes.
        FloatSampleCase::builder()
            .labels(labels(&[crate::test_support::TestLabelPair::new(
                "__name__", "g",
            )]))
            .timestamp_ms(0)
            .value(1.0)
            .build(),
        FloatSampleCase::builder()
            .labels(labels(&[crate::test_support::TestLabelPair::new(
                "__name__", "g",
            )]))
            .timestamp_ms(60_000)
            .value(2.0)
            .build(),
        FloatSampleCase::builder()
            .labels(labels(&[crate::test_support::TestLabelPair::new(
                "__name__", "g",
            )]))
            .timestamp_ms(120_000)
            .value(4.0)
            .build(),
        FloatSampleCase::builder()
            .labels(labels(&[crate::test_support::TestLabelPair::new(
                "__name__", "g",
            )]))
            .timestamp_ms(180_000)
            .value(8.0)
            .build(),
        FloatSampleCase::builder()
            .labels(labels(&[crate::test_support::TestLabelPair::new(
                "__name__", "g",
            )]))
            .timestamp_ms(240_000)
            .value(16.0)
            .build(),
    ] {
        store.push_float("t", sample.labels, sample.timestamp_ms, sample.value);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let queries: &[QueryAt] = &[
        // Unary over a vector (drops `__name__`, negates each value, keeps a
        // genuine NaN, drops the stale marker).
        QueryAt {
            query: "-m",
            time_ms: 120_000,
        },
        // Unary over an aggregate result (vector).
        QueryAt {
            query: "-sum(m)",
            time_ms: 120_000,
        },
        // Unary over a scalar.
        QueryAt {
            query: "-(1 + 2)",
            time_ms: 120_000,
        },
        // Double negation.
        QueryAt {
            query: "- -m",
            time_ms: 120_000,
        },
        // Bare numeric / string literals.
        QueryAt {
            query: "42",
            time_ms: 120_000,
        },
        QueryAt {
            query: "-7.5",
            time_ms: 120_000,
        },
        QueryAt {
            query: r#""hello""#,
            time_ms: 120_000,
        },
        // Raw matrix selector / subquery (RangeMatrix from query_instant).
        QueryAt {
            query: "g[3m]",
            time_ms: 240_000,
        },
        QueryAt {
            query: "m[2m]",
            time_ms: 120_000,
        },
        QueryAt {
            query: "g[4m:1m]",
            time_ms: 240_000,
        },
        // `smoothed` extended selector (vector). The extension parser is not
        // feature-gated, so this routes in both build configs.
        QueryAt {
            query: "smoothed(g)",
            time_ms: 90_000,
        },
    ];

    // The recursive planner must claim every one of these.
    assert_call_planner_matches_interpreter(&engine, &queries).await;

    // Pin the result types the parity above relies on.
    for case in [
        QueryResultTypeCase {
            query: "42",
            time_ms: 120_000,
            expected_type: "scalar",
        },
        QueryResultTypeCase {
            query: r#""hello""#,
            time_ms: 120_000,
            expected_type: "string",
        },
        QueryResultTypeCase {
            query: "g[3m]",
            time_ms: 240_000,
            expected_type: "matrix",
        },
        QueryResultTypeCase {
            query: "g[4m:1m]",
            time_ms: 240_000,
            expected_type: "matrix",
        },
        QueryResultTypeCase {
            query: "-m",
            time_ms: 120_000,
            expected_type: "vector",
        },
        QueryResultTypeCase {
            query: "-(1 + 2)",
            time_ms: 120_000,
            expected_type: "scalar",
        },
    ] {
        let expr = parse_promql_with_duration_context(
            case.query,
            DurationExprContext::instant(case.time_ms),
        )
        .unwrap();
        let plan = engine
            .plan_instant_expr("t", &expr, case.time_ms)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("`{}` did not route through the planner", case.query));
        let result = engine
            .assemble_planned_instant(plan, case.time_ms)
            .await
            .unwrap();
        assert2::assert!(result.result_type() == case.expected_type);
    }

    // The `anchored` modifier on an instant-vector selector is the same hard
    // error on both paths.
    {
        let expr = parse_promql_with_duration_context(
            "anchored(m)",
            DurationExprContext::instant(120_000),
        )
        .unwrap();
        let planner_err = engine.plan_instant_expr("t", &expr, 120_000).await;
        let interp_err = engine.eval_instant_expr("t", &expr, 120_000).await;
        assert2::assert!(planner_err.is_err());
        assert2::assert!(interp_err.is_err());
    }
}
