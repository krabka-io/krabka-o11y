use super::*;

/// Direct totality assertion over a representative construct sweep.
///
/// For every valid query family that the corpus can produce, `plan_instant_expr`
/// must return `Ok(Some(..))` and route through the planner. It must never
/// return `Ok(None)`. For every invalid query, it must return `Err(..)`, and it
/// must never return `Ok(None)`. This test is the per-construct complement to
/// the corpus-wide counter proof.
#[tokio::test]
pub(crate) async fn plan_instant_expr_is_total_over_construct_sweep() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    let mut store = InMemoryMetricStore::new();
    let time_ms = 300_000_i64;
    for (lbls, samples) in [
        (
            labels(&[("__name__", "m"), ("job", "a")]),
            vec![(120_000_i64, 1.0_f64), (240_000, 2.0), (300_000, 3.0)],
        ),
        (
            labels(&[("__name__", "m"), ("job", "b")]),
            vec![(120_000, 4.0), (240_000, 5.0), (300_000, 6.0)],
        ),
        (
            labels(&[("__name__", "n"), ("job", "a")]),
            vec![(120_000, 7.0), (240_000, 8.0), (300_000, 9.0)],
        ),
    ] {
        for (ts, value) in samples {
            store.push_float("t", lbls.clone(), ts, value);
        }
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    // VALID families: each MUST plan to Some (never None, never Err).
    let valid: &[&str] = &[
        // Leaves / literals.
        "m",
        "42",
        "\"hello\"",
        "m offset 1m",
        "m @ 100",
        // Parenthesized.
        "(m)",
        "((m + 1))",
        // Unary.
        "-m",
        "- (m + 1)",
        // Binary: vector∘vector, vector∘scalar, scalar∘scalar, set ops.
        "m + n",
        "m * 2",
        "2 + 3",
        "m and n",
        "m or n",
        "m unless n",
        "m > 5",
        "m == bool 5",
        "sum(m) / sum(n)",
        // Simple aggregations.
        "sum(m)",
        "sum by (job) (m)",
        "avg without (job) (m)",
        "count(m)",
        "min(m)",
        "max(m)",
        "group(m)",
        // Param aggregations.
        "topk(1, m)",
        "bottomk(1, m)",
        "quantile(0.5, m)",
        "count_values(\"v\", m)",
        "stddev(m)",
        "stdvar(m)",
        "stddev by (job) (m)",
        // Rate-family + over_time range calls.
        "rate(m[5m])",
        "increase(m[5m])",
        "delta(m[5m])",
        "irate(m[5m])",
        "idelta(m[5m])",
        "avg_over_time(m[5m])",
        "sum_over_time(m[5m])",
        "count_over_time(m[5m])",
        "min_over_time(m[5m])",
        "max_over_time(m[5m])",
        "stddev_over_time(m[5m])",
        "stdvar_over_time(m[5m])",
        "last_over_time(m[5m])",
        "present_over_time(m[5m])",
        "quantile_over_time(0.5, m[5m])",
        // Aggregation over a range call (compositional).
        "sum by (job) (rate(m[5m]))",
        "max without (job) (avg_over_time(m[5m]))",
        // Scalar-math per-row calls.
        "abs(m)",
        "ceil(m)",
        "floor(m)",
        "round(m)",
        "round(m, 2)",
        "clamp(m, 1, 5)",
        "clamp_min(m, 2)",
        "clamp_max(m, 4)",
        "sqrt(m)",
        "exp(m)",
        "ln(m)",
        "log2(m)",
        "log10(m)",
        "sgn(m)",
        // Trig.
        "sin(m)",
        "cos(m)",
        "tan(m)",
        // Label ops.
        "label_replace(m, \"x\", \"y\", \"job\", \"(.*)\")",
        "label_join(m, \"x\", \"-\", \"job\")",
        "sort(m)",
        "sort_desc(m)",
        // Utilities.
        "time()",
        "pi()",
        "scalar(sum(m))",
        "vector(1)",
        "timestamp(m)",
        "absent(m)",
        "absent(nonexistent_metric)",
        "absent_over_time(m[5m])",
        "day_of_week()",
        "day_of_month(m)",
        "minute()",
        "hour()",
        // Histogram-quantile over a classic bucket vector (float series).
        "histogram_quantile(0.5, m)",
        // Top-level raw matrix selector / subquery (instant query).
        "m[5m]",
        "m[5m:1m]",
        "rate(m[5m:1m])",
        "sum_over_time(m[5m:1m])",
    ];
    for query in valid {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse valid `{query}`: {error}"));
        let planned = engine
            .plan_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("plan valid `{query}` errored: {error}"));
        assert2::assert!(planned.is_some());
    }

    // INVALID families: each MUST surface as Err (never Ok(None), never
    // Ok(Some)). These mirror the corpus `expect fail` cases that previously
    // deferred to the interpreter purely to raise the canonical error.
    let invalid: &[&str] = &[
        // Non-scalar / out-of-range / NaN scalar params.
        "quantile_over_time(m, m[5m])",
        "topk(m, m)",
        "quantile(m, m)",
        "clamp(m, m, 5)",
        "round(m, m)",
        "histogram_quantile(m, m)",
        // Non-string-literal label args.
        "label_replace(m, m, \"y\", \"job\", \"(.*)\")",
        "label_join(m, m, \"-\", \"job\")",
        "count_values(m, m)",
        "sort_by_label(m, m)",
        // Wrong arity.
        "time(m)",
        "pi(m)",
        "scalar(m, m)",
        "vector(m, m)",
        "timestamp(m, m)",
        "label_replace(m, \"x\")",
        "histogram_quantile(0.5)",
        // Type mismatch in a binary op (vector op range).
        "m + m[5m]",
    ];
    for query in invalid {
        let Ok(expr) =
            parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
        else {
            // A parse-time rejection is also a total outcome (never reaches
            // the planner), so a query the parser rejects is acceptable here.
            continue;
        };
        let outcome = engine.plan_instant_expr("t", &expr, time_ms).await;
        let _kind = match &outcome {
            Ok(Some(_)) => "Ok(Some)",
            Ok(None) => "Ok(None)",
            Err(_) => "Err",
        };
        assert2::assert!(outcome.is_err());
    }
}
