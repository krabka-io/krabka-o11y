use super::*;

/// Matches a top-level `*_over_time` range call eligible for the operator path.
///
/// A call is eligible if and only if `expr` is a [`Call`] whose function is one
/// of the non-experimental members (`sum|avg|count|min|max|stddev|stdvar|
/// last|present_over_time`, or `quantile_over_time`), and whose range argument
/// is a plain [`Expr::MatrixSelector`] after the removal of parentheses. For
/// `quantile_over_time` this function returns the leading `phi` argument for
/// separate scalar resolution. For every other family it returns `None`.
///
/// [`match_experimental_over_time_range_call`] matches the experimental members
/// (`mad_over_time`, `first_over_time`, the `ts_of_*_over_time` family), which
/// route through the shared kernel and not through this float UDF-chain path.
/// `absent_over_time`, subquery range arguments, `anchored`/`smoothed` selectors
/// (which parse to [`Expr::Extension`]), and nested forms stay on the
/// interpreter, and this function returns `None` for them.
pub(crate) fn match_over_time_range_call(
    expr: &Expr,
) -> Option<(&MatrixSelector, OverTimeFamily, Option<&Expr>)> {
    let Expr::Call(call) = expr else {
        return None;
    };
    let family = over_time_family_from_function_name(call.func.name)?;
    let (range_arg, phi_arg) = if matches!(family, OverTimeFamily::Quantile) {
        let [phi, range] = call.args.args.as_slice() else {
            return None;
        };
        (range.as_ref(), Some(phi.as_ref()))
    } else {
        let [range] = call.args.args.as_slice() else {
            return None;
        };
        (range.as_ref(), None)
    };
    let mut arg = range_arg;
    while let Expr::Paren(paren) = arg {
        arg = paren.expr.as_ref();
    }
    let Expr::MatrixSelector(selector) = arg else {
        return None;
    };
    Some((selector, family, phi_arg))
}
