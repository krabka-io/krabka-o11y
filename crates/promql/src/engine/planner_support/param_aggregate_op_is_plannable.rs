use super::*;

/// Returns `true` when a parameterized or non-simple aggregation is plannable.
///
/// Such an aggregation routes through the operator path
/// (`plan_param_aggregate_expr`). The aggregations are `topk`/`bottomk`/
/// `quantile` (numeric-literal param), `count_values` (string-literal param),
/// `stddev`/`stdvar` (no param), and the experimental `limitk`/`limit_ratio`
/// (scalar param). The `limitk` and `limit_ratio` params resolve through the
/// SAME interpreter helpers, which include the deduplicated
/// `InvalidRatioWarning` of `limit_ratio`. This function checks the structural
/// param shape, so the range gate matches the param requirement of the per-step
/// planner. A param of the right kind but malformed still falls back at eval
/// time, and the interpreter raises the canonical error.
pub(crate) fn param_aggregate_op_is_plannable(aggregate: &AggregateExpr) -> bool {
    match aggregate.op.id() {
        T_TOPK | T_BOTTOMK | T_QUANTILE => {
            matches!(aggregate.param.as_deref(), Some(Expr::NumberLiteral(_)))
        }
        T_COUNT_VALUES => matches!(aggregate.param.as_deref(), Some(Expr::StringLiteral(_))),
        T_STDDEV | T_STDVAR => aggregate.param.is_none(),
        // `limitk`/`limit_ratio` carry a scalar parameter resolved through the
        // interpreter helpers; the planner short-circuits a 0 param and applies
        // the shared selection kernel.
        #[cfg(feature = "experimental-functions")]
        T_LIMITK | T_LIMIT_RATIO => aggregate
            .param
            .as_deref()
            .is_some_and(|param| param.value_type() == ValueType::Scalar),
        _ => false,
    }
}
