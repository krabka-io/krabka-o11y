use super::{Expr, instant_expr_is_plannable};

/// Gate for routing a range query through the per-step operator planner.
///
/// A range query routes through the per-step operator driver if and only if its
/// top-level shape is per-step planner-supported (`instant_expr_is_plannable`).
/// This includes a bare instant-vector selector and a top-level scalar-typed
/// expression:
///
/// - Bare instant-vector selector. The selector chain uses the Prometheus
///   left-open, right-closed lookback window `(eval - lookbackDelta, eval]`
///   (`promql/engine.go::vectorSelectorSingle` rejects `t <= eval - lookback`),
///   so it excludes a sample exactly on the lookback boundary.
///
/// - Scalar-typed expression (`time()`, `1 + 2`, the argless calendar forms).
///   The driver folds a no-label scalar series per step (empty label set,
///   `SampleValue::Float`).
///
/// An aggregation over a rate-family or `*_over_time` range call
/// (`sum(rate(m[5m]))`, `avg by(l)(increase(...))`, ...) routes through the
/// planner too. The rate and `*_over_time` UDFs emit NULL, not a NaN sentinel,
/// for a no-value window. The aggregate planner drops those NULL rows before it
/// groups, and the built-in NaN-ignoring aggregates skip NULL. A no-value series
/// is therefore excluded from the group, and an all-no-value group returns no
/// result row. A genuine NaN value is non-null and propagates through the
/// aggregate.
///
/// A top-level raw matrix selector or subquery is not plannable here. It returns
/// a range vector, which the dedicated matrix and subquery range paths own, so
/// `instant_expr_is_plannable` already excludes it.
///
/// A top-level bare selector with an `@ start()` or `@ end()` modifier also
/// routes through the planner. The per-step planner range driver scopes the
/// `[start, end]` bounds of the query in `AT_MODIFIER_BOUNDS`. The selector
/// planner (`PromqlEngine::plan_instant_selector`) then resolves `@ start()` and
/// `@ end()` to those bounds as Prometheus does: one fixed eval instant repeated
/// at every grid step.
pub(crate) fn range_expr_routes_through_planner(expr: &Expr) -> bool {
    instant_expr_is_plannable(expr)
}
