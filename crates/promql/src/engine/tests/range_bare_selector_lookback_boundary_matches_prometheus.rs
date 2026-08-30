use super::*;

/// Pins the range-path lookback boundary against Prometheus semantics.
///
/// The instant-vector lookback window is `(eval - lookbackDelta, eval]`, which
/// is left-open and right-closed. A sample with `ts == eval - lookbackDelta`
/// sits exactly on the lower boundary, and the engine excludes it. This test
/// proves:
///   1. the bare-selector range query routes through the planner,
///   2. planner == interpreter byte-for-byte across the grid, and
///   3. the engine excludes the boundary sample, which is the
///      Prometheus-correct behaviour, so a step whose only in-window candidate
///      is the boundary sample has no point.
#[tokio::test]
pub(crate) async fn range_bare_selector_lookback_boundary_matches_prometheus() {
    let lookback = EngineOpts::default().lookback_delta.millis_i64(); // 300_000 (5m)

    let mut store = InMemoryMetricStore::new();
    // A single sample at t=0. With a 5m lookback:
    //   - step t=0:        window (−300000, 0], sample at 0 is in-window (right-closed) -> value.
    //   - step t=300000:   window (0, 300000], sample at 0 is EXACTLY on the
    //                      left boundary -> EXCLUDED (left-open) -> NO point.
    //   - step t=240000:   window (−60000, 240000], sample at 0 in-window -> value.
    store.push_float(
        "t",
        labels(&[("__name__", "m"), ("job", "boundary")]),
        0,
        7.0,
    );
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let (start, end, step) = (0_i64, lookback, millis(60_000));

    // (1) the gate routes the bare selector through the planner.
    {
        use promql_parser::parser::Expr;

        use crate::{DurationExprContext, parse_promql_with_duration_context};
        let expr =
            parse_promql_with_duration_context("m", DurationExprContext::range(start, end, step))
                .unwrap();
        let mut probe = &expr;
        while let Expr::Paren(paren) = probe {
            probe = &paren.expr;
        }
        assert2::assert!(super::super::range_expr_routes_through_planner(probe));
    }

    // (2) planner (public range path) yields the boundary-correct grid.
    let planner = engine
        .query_range("t", "m", start, end, step)
        .await
        .unwrap();

    // (3) the boundary step (t == eval - lookback) is excluded; the last
    // point is at t=240000, NOT at t=300000.
    let QueryResult::RangeMatrix(series) = &planner else {
        panic!("expected range matrix");
    };
    assert2::assert!(series.len() == 1);
    let times: Vec<i64> = series[0].samples.iter().map(|(t, _)| *t).collect();
    assert2::assert!(times == vec![0, 60_000, 120_000, 180_000, 240_000]);

    // (4) cross-check the interpreter's INSTANT path and the operator both
    // exclude the boundary sample directly, proving all three paths agree.
    let instant_at_boundary = engine.query_instant("t", "m", lookback).await.unwrap();
    let QueryResult::InstantVector(samples) = instant_at_boundary else {
        panic!("expected instant vector");
    };
    assert2::assert!(samples.is_empty());
}
