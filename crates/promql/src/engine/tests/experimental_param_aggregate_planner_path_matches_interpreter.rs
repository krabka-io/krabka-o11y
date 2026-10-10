#[cfg(feature = "experimental-functions")]
use super::*;

/// Differential parity for the experimental `limitk` and `limit_ratio` aggregations.
///
/// These are param aggregations, and the test covers the `InvalidRatioWarning`
/// annotation of `limit_ratio`. The planner reuses the same
/// parameter-resolution helpers and selection kernels as the interpreter, so
/// the result and the emitted annotations both match.
#[cfg(feature = "experimental-functions")]
#[tokio::test]
pub(crate) async fn experimental_param_aggregate_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 4.0)] {
        store.push_float(
            "t",
            labels(&[("__name__", "m"), ("job", "api"), ("instance", instance)]),
            120_000,
            value,
        );
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let queries: &[&str] = &[
        "limitk(2, m)",
        "limitk(10, m)",
        "limitk(0, m)",
        "limitk(2, m) by (job)",
        "limit_ratio(0.5, m)",
        "limit_ratio(-0.5, m)",
        "limit_ratio(1, m)",
        "limit_ratio(0, m)",
        // Out-of-range ratios: must emit the InvalidRatioWarning on BOTH paths.
        "limit_ratio(1.5, m)",
        "limit_ratio(-2, m)",
    ];
    let time_ms = 120_000_i64;

    for &query in queries {
        let ((via_operators, operator_annotations), (via_interpreter, interpreter_annotations)) =
            annotated_planned_and_interpreted(&engine, query, time_ms).await;
        let via_operators = sort_instant_result(via_operators);
        let via_interpreter = sort_instant_result(via_interpreter);

        assert2::assert!(query_results_match(&via_interpreter, &via_operators));
        assert2::assert!(operator_annotations == interpreter_annotations);
    }

    // Pin that an out-of-range ratio actually emits the warning (so the
    // equality above is not vacuously comparing two empty sets).
    let expr = parse_promql_with_duration_context(
        "limit_ratio(1.5, m)",
        DurationExprContext::instant(time_ms),
    )
    .unwrap();
    let annotations = super::super::ANNOTATIONS
        .scope(std::cell::RefCell::new(crate::Annotations::new()), async {
            let plan = engine
                .plan_instant_expr("t", &expr, time_ms)
                .await
                .unwrap()
                .unwrap();
            engine
                .assemble_planned_instant(plan, time_ms)
                .await
                .unwrap();
            super::super::ANNOTATIONS.with(|sink| sink.borrow().clone())
        })
        .await;
    assert2::assert!(annotations.warnings.len() == 1);
    assert2::assert!(
        annotations.warnings[0].contains("ratio value should be between -1 and 1") == true
    );
}
