use super::*;

/// Differential parity for the experimental scalar and range functions.
///
/// The functions are `max_of`, `min_of`, `double_exponential_smoothing` over a
/// bare matrix selector, and the duration helpers. Each one delegates to the
/// same interpreter method, so the result is parity-exact by construction.
#[cfg(feature = "experimental-functions")]
#[tokio::test]
pub(crate) async fn experimental_call_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    let mut store = InMemoryMetricStore::new();
    for (ts, value) in [
        (0_i64, 1.0),
        (60_000, 2.0),
        (120_000, 4.0),
        (180_000, 8.0),
        (240_000, 16.0),
    ] {
        store.push_float("t", labels(&[("__name__", "g")]), ts, value);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let queries: &[(&str, i64)] = &[
        ("max_of(1, 2)", 120_000),
        ("min_of(1, 2)", 120_000),
        ("max_of(scalar(g), 3)", 240_000),
        ("double_exponential_smoothing(g[4m], 0.5, 0.5)", 240_000),
        // Duration helpers (instant query: no range context -> 0 on both
        // paths).
        ("step()", 120_000),
        ("start()", 120_000),
        ("end()", 120_000),
        ("range()", 120_000),
    ];

    for &(query, time_ms) in queries {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
        let plan = engine
            .plan_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
            .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
        let via_operators = sort_instant_result(
            engine
                .assemble_planned_instant(plan, time_ms)
                .await
                .unwrap_or_else(|error| panic!("operator `{query}`: {error}")),
        );
        let via_interpreter = sort_instant_result(
            engine
                .eval_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}")),
        );
        assert2::assert!(query_results_match(&via_interpreter, &via_operators));
    }
}
