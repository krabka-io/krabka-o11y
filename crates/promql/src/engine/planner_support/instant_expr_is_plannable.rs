use super::{Expr, match_rate_range_call, match_over_time_range_call, match_experimental_over_time_range_call, is_extended_range_fold_call, scalar_math_op_from_function_name, label_ops_kind_from_function_name, histogram_accessor_from_function_name, match_subquery_range_call, util_call_is_plannable, simple_aggregate_op, param_aggregate_op_is_plannable, binary_operand_is_plannable};

/// Returns `true` when the operator planner handles every node of `expr`.
///
/// The operator planner is `PromqlEngine::plan_instant_expr`. A range query over
/// a plannable `expr` can route through the per-step planner. This predicate
/// mirrors the dispatch of `plan_instant_expr`, that is, which node kinds and
/// function names route to the operator path, and it recurses into vector-typed
/// inner expressions the same way. It is structural and never touches the store,
/// because the set of constructs that the operator path understands does not
/// change with the evaluation timestamp.
///
/// This predicate does NOT model the data-dependent fallbacks: histogram-bearing
/// or empty-valued-label series, wrong call arity, a non-scalar bound argument,
/// and an invalid `label_replace` regex. Those still appear at evaluation time,
/// when `plan_instant_expr` returns `None` or an `Err`. The per-step range driver
/// treats any such per-step `None` as a whole-query fallback to the interpreter.
/// This predicate therefore only needs to gate out the node kinds that cannot
/// nest as an operand or stitch across a step grid: string literals, raw matrix
/// selectors, and subqueries, whose results are not a numeric scalar or an
/// instant vector.
///
/// Scalar-typed sub-expressions, a bound argument or a scalar binary operand,
/// are always plannable. The planner evaluates them through the pure scalar path
/// of the interpreter, which has no staleness or NaN subtlety.
pub(crate) fn instant_expr_is_plannable(expr: &Expr) -> bool {
    match expr {
        Expr::Paren(paren) => instant_expr_is_plannable(&paren.expr),
        // Bare selectors, numeric literals, and extended selectors all have
        // dedicated planner paths. Histogram-bearing vectors can still fall
        // back per step inside those paths.
        Expr::VectorSelector(_) | Expr::NumberLiteral(_) | Expr::Extension(_) => true,
        Expr::Call(call) => {
            // Rate-family or `*_over_time` range call (incl. the experimental
            // `mad`/`first`/`ts_of_*_over_time` members) over a bare matrix
            // selector. The matchers already require a plain `MatrixSelector`
            // argument; histogram inputs fall back per-step. A bad
            // `quantile_over_time` phi no longer falls back - it evaluates to
            // signed `+/-Inf` / `NaN` plus an `InvalidQuantileWarning`.
            if match_rate_range_call(expr).is_some()
                || match_over_time_range_call(expr).is_some()
                || match_experimental_over_time_range_call(expr).is_some()
            {
                return true;
            }
            // A RESIDUAL range-vector fold the fast matchers don't claim:
            // `changes`/`resets`/`deriv`/`predict_linear`/
            // `double_exponential_smoothing` over a plain matrix, or ANY rate /
            // `*_over_time` fold over an `anchored`/`smoothed` extended selector.
            // These route through `plan_extended_range_fold_call` (delegating to
            // the shared interpreter dispatch), so they are plannable - including
            // nested under an aggregate / binary and range-stitched per step.
            if is_extended_range_fold_call(call) {
                return true;
            }
            // The EXPERIMENTAL scalar-returning helpers handled by
            // `plan_experimental_call`: the duration helpers `range`/`step`/
            // `start`/`end` (which read the scoped range context, also scoped by the
            // per-step planner range driver) and `max_of`/`min_of` (scalar of scalar
            // extrema). These fold to a `PrecomputedScalar`, so they nest and
            // range-stitch like any scalar expression.
            #[cfg(feature = "experimental-functions")]
            if matches!(
                call.func.name,
                "range" | "step" | "start" | "end" | "max_of" | "min_of"
            ) {
                return true;
            }
            // A per-row scalar-math call: the inner vector argument (the first
            // positional arg) must itself be plannable. The trailing bound
            // args are scalars resolved through the interpreter.
            if scalar_math_op_from_function_name(call.func.name).is_some() {
                return call
                    .args
                    .args
                    .first()
                    .is_some_and(|arg| instant_expr_is_plannable(arg));
            }
            // A label-rewrite / ordering call: the inner vector argument (the
            // first positional arg) must be plannable; the rest are string
            // literals validated per-step.
            if label_ops_kind_from_function_name(call.func.name).is_some() {
                return call
                    .args
                    .args
                    .first()
                    .is_some_and(|arg| instant_expr_is_plannable(arg));
            }
            // A `histogram_quantile(phi, v)` (classic OR native): the inner bucket
            // vector (the second positional arg) must be plannable. `phi` (the
            // first arg) is a scalar resolved through the interpreter.
            if call.func.name == "histogram_quantile" {
                return call
                    .args
                    .args
                    .get(1)
                    .is_some_and(|arg| instant_expr_is_plannable(arg));
            }
            // The experimental `histogram_quantiles(label, v, phi...)`: the inner
            // bucket vector (the FIRST positional arg) must be plannable. The label
            // name and the trailing scalar `phi`s are resolved per-step.
            #[cfg(feature = "experimental-functions")]
            if call.func.name == "histogram_quantiles" {
                return call
                    .args
                    .args
                    .first()
                    .is_some_and(|arg| instant_expr_is_plannable(arg));
            }
            // A native accessor (`histogram_count`/`sum`/`avg`/`stddev`/`stdvar`):
            // the single instant-vector operand must be plannable.
            if histogram_accessor_from_function_name(call.func.name).is_some() {
                return call
                    .args
                    .args
                    .first()
                    .is_some_and(|arg| instant_expr_is_plannable(arg));
            }
            // `histogram_fraction(lower, upper, v)`: the inner vector (the third
            // positional arg) must be plannable. The two scalar bounds are
            // resolved through the interpreter.
            if call.func.name == "histogram_fraction" {
                return call
                    .args
                    .args
                    .get(2)
                    .is_some_and(|arg| instant_expr_is_plannable(arg));
            }
            // `info(v [, data_label_selector])`: the input vector `v` (the first
            // positional arg) must be plannable. The data-label selector is a
            // vector-selector literal validated at eval time (a non-vector-selector
            // arg / wrong arity surfaces as an `Err` from `plan_info_call`, which
            // the per-step driver treats as a whole-query fallback).
            if call.func.name == "info" {
                return call
                    .args
                    .args
                    .first()
                    .is_some_and(|arg| instant_expr_is_plannable(arg));
            }
            // A range/`*_over_time` call whose argument is a subquery: the
            // subquery's inner instant expression must itself be plannable. The
            // outer scalar params (quantile/predict_linear/double_exp) are
            // resolved through the interpreter; a non-positive step / invalid
            // param falls back inside `plan_subquery_range_call`. This is what lets
            // nested subqueries and subquery calls inside an aggregate/binary route
            // through the planner.
            if let Some((subquery, _)) = match_subquery_range_call(call) {
                return instant_expr_is_plannable(&subquery.expr);
            }
            // The float UTILITY functions handled by `plan_util_call`.
            util_call_is_plannable(call)
        }
        // A simple (no-param) float aggregation, or a parameterized aggregation
        // (topk/bottomk/quantile/count_values/stddev/stdvar), over a plannable
        // inner vector. `limitk`/`limit_ratio` are not plannable.
        Expr::Aggregate(aggregate) => {
            let simple = simple_aggregate_op(aggregate.op).is_some() && aggregate.param.is_none();
            (simple || param_aggregate_op_is_plannable(aggregate))
                && instant_expr_is_plannable(&aggregate.expr)
        }
        // A binary op: each operand must itself be plannable; scalar operands are
        // always fine (folded via the interpreter's pure scalar path). A
        // scalar of scalar fold is carried through `PrecomputedScalar`.
        Expr::Binary(binary) => {
            binary_operand_is_plannable(&binary.lhs) && binary_operand_is_plannable(&binary.rhs)
        }
        // A unary `-`/`+` over a plannable operand. A scalar operand folds to a
        // scalar; a vector operand to a vector. Both nest and range-stitch.
        Expr::Unary(unary) => instant_expr_is_plannable(&unary.expr),
        // A string literal (no numeric/vector result to nest or range-stitch) and
        // a raw matrix selector / subquery (range-vector result, only meaningful
        // at the top level of an instant query) are handled directly in the
        // top-level `plan_instant_expr` dispatch, not through this nesting /
        // range-stitching predicate.
        Expr::StringLiteral(_) | Expr::MatrixSelector(_) | Expr::Subquery(_) => false,
    }
}
