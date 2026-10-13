#[cfg(feature = "experimental-functions")]
use super::*;

#[cfg(feature = "experimental-functions")]
#[derive(bon::Builder)]
struct ParamAggregateSeries {
    instance: &'static str,
    value: f64,
}

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
    for series in [
        ParamAggregateSeries::builder()
            .instance("a")
            .value(1.0)
            .build(),
        ParamAggregateSeries::builder()
            .instance("b")
            .value(2.0)
            .build(),
        ParamAggregateSeries::builder()
            .instance("c")
            .value(3.0)
            .build(),
        ParamAggregateSeries::builder()
            .instance("d")
            .value(4.0)
            .build(),
    ] {
        store.push_float(
            "t",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "m"),
                crate::test_support::TestLabelPair::new("job", "api"),
                crate::test_support::TestLabelPair::new("instance", series.instance),
            ]),
            120_000,
            series.value,
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
        let parity = annotated_planned_and_interpreted(&engine, query, time_ms).await;
        let via_operators = sort_instant_result(parity.via_operators.result);
        let via_interpreter = sort_instant_result(parity.via_interpreter.result);

        assert2::assert!(query_results_match(&via_interpreter, &via_operators));
        assert2::assert!(parity.via_operators.annotations == parity.via_interpreter.annotations);
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
