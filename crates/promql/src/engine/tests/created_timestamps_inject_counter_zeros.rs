use super::*;

#[tokio::test]
pub(crate) async fn created_timestamps_inject_counter_zeros_in_planner_and_interpreter_windows() {
    use promql_parser::parser::Expr;

    use crate::{DurationExprContext, parse_promql_with_duration_context};

    let mut store = InMemoryMetricStore::new();
    let new_counter = labels(&[("__name__", "http_requests_total"), ("job", "new")]);
    store.push_float_with_start_timestamp("t", new_counter, 240_000, 6.0, Some(180_000));

    let reset_counter = labels(&[("__name__", "http_requests_total"), ("job", "reset")]);
    store.push_float_with_start_timestamp("t", reset_counter.clone(), 100_000, 5.0, Some(50_000));
    // The value rose across the restart, so value-only reset detection cannot
    // see it. The changed start timestamp contributes a zero between samples.
    store.push_float_with_start_timestamp("t", reset_counter, 200_000, 8.0, Some(150_000));

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for query in [
        "rate(http_requests_total{job=\"new\"}[5m])",
        "increase(http_requests_total{job=\"new\"}[5m])",
    ] {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(300_000))
            .unwrap();
        let Expr::Call(call) = &expr else {
            panic!("expected call");
        };
        let (selector, kind) = match_rate_range_call(&expr).unwrap();
        let interpreter = engine.eval_instant_call("t", call, 300_000).await.unwrap();
        let planner = engine
            .eval_rate_range_via_planner("t", selector, 300_000, kind)
            .await
            .unwrap();

        assert2::assert!(interpreter == planner, "{query}");
        let QueryResult::InstantVector(samples) = planner else {
            panic!("expected vector");
        };
        assert2::assert!(samples.len() == 1, "{query}");
        if query.starts_with("rate") {
            assert2::assert!(approx_eq(float_value(&samples[0].value), 12.0 / 300.0));
        } else {
            assert2::assert!(approx_eq(float_value(&samples[0].value), 12.0));
        }
    }

    let result = engine
        .query_instant(
            &tenant_id("t"),
            "resets(http_requests_total{job=\"reset\"}[5m])",
            300_000,
        )
        .await
        .unwrap();
    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert2::assert!(samples.len() == 1);
    assert2::assert!(approx_eq(float_value(&samples[0].value), 1.0));
}
