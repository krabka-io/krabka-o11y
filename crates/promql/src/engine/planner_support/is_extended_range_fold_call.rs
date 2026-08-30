use super::{Call, Expr, ExtendedSelectorExpr, range_fold_range_arg_index};

/// Recognizes a residual range-vector fold call, see `range_fold_range_arg_index`.
///
/// The range-vector argument of the call, after the removal of parentheses, is a
/// plain [`Expr::MatrixSelector`] or an `anchored`/`smoothed`
/// [`Expr::Extension`] over a selector. [`match_subquery_range_call`] already
/// claims subquery range arguments, and [`match_rate_range_call`] and
/// [`match_over_time_range_call`] already claim the fast plain-matrix `rate` and
/// `*_over_time` paths earlier in the dispatch. This matcher makes the planner
/// TOTAL over the remaining shapes: `changes`/`resets`/`deriv` over a plain
/// matrix, and ANY of these folds over an anchored or smoothed selector. It
/// routes them into the SHARED interpreter `eval_*_call`, which is parity-exact.
///
/// Returns `true` when the call should route through
/// `PromqlEngine::plan_extended_range_fold_call`.
pub(crate) fn is_extended_range_fold_call(call: &Call) -> bool {
    let Some(index) = range_fold_range_arg_index(call) else {
        return false;
    };
    let Some(range_arg) = call.args.args.get(index) else {
        return false;
    };
    let mut arg = range_arg.as_ref();
    while let Expr::Paren(paren) = arg {
        arg = paren.expr.as_ref();
    }
    match arg {
        Expr::MatrixSelector(_) => true,
        // An `anchored`/`smoothed` extended selector wraps a `MatrixSelector`
        // child (`anchored(m[5m])`), so the interpreter's `eval_range_arg` can
        // build its windowed range vector.
        Expr::Extension(extension) => extension
            .expr
            .as_any()
            .downcast_ref::<ExtendedSelectorExpr>()
            .is_some_and(|extended| matches!(extended.child(), Some(Expr::MatrixSelector(_)))),
        _ => false,
    }
}
