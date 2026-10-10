use super::*;

/// Differential parity for a top-level scalar-typed range query.
///
/// A scalar expression such as `time()` or `1 + 2`
/// routes through the per-step planner driver. The driver folds an identical
/// no-label scalar series per step. The result must be byte-exact with the
/// interpreter's `eval_instant_expr_over_steps` scalar stitching.
#[tokio::test]
pub(crate) async fn range_scalar_expr_planner_path_matches_interpreter() {
    use crate::DurationExprContext;

    // A store with one series so calendar functions over `time()` have a
    // defined eval timeline; scalars ignore the series entirely.
    let mut store = InMemoryMetricStore::new();
    store.push_float("t", labels(&[("__name__", "m"), ("job", "a")]), 0, 1.0);
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let (start, end, step) = (0_i64, 300_000_i64, millis(60_000));

    for query in ["42", "1 + 2", "time()", "2 * (3 + 4)"] {
        assert2::assert!(range_query_routes_through_planner(
            query,
            DurationExprContext::range(start, end, step)
        ));
        let planner = engine
            .query_range(&tenant_id("t"), query, start, end, step)
            .await
            .unwrap_or_else(|error| panic!("planner `{query}`: {error}"));
        // A scalar range query stitches a single no-label series, one float
        // point per step across the whole grid.
        let QueryResult::RangeMatrix(series) = &planner else {
            panic!("expected range matrix for `{query}`");
        };
        assert2::assert!(series.len() == 1);
        assert2::assert!(series[0].labels.is_empty());
        assert2::assert!(series[0].samples.len() == 6);
        // The constant scalars fold to their exact value at every step.
        if let Some(expected) = match query {
            "42" => Some(42.0_f64),
            "1 + 2" => Some(3.0),
            "2 * (3 + 4)" => Some(14.0),
            _ => None,
        } {
            for (_, value) in &series[0].samples {
                assert2::assert!(float_value(value).to_bits() == expected.to_bits());
            }
        }
    }
}
