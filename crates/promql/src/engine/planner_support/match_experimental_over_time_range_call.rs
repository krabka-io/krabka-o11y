use super::{Expr, MatrixSelector, OverTimeFn};

/// Matches a top-level EXPERIMENTAL `*_over_time` member range call.
///
/// A call is eligible for the shared-kernel operator path if and only if `expr`
/// is a [`Call`] whose function is one of `mad_over_time`, `first_over_time`, or
/// the `ts_of_{first,last,min,max}_over_time` family, with exactly one argument
/// that is a plain [`Expr::MatrixSelector`] after the removal of parentheses.
/// This function returns the selector and the matching [`OverTimeFn`]. These
/// members have no operator-leaf UDF, so they route through the shared
/// `apply_outer_range_fn` kernel and not through the float UDF chain.
///
/// `absent_over_time`, subquery range arguments, `anchored`/`smoothed` selectors
/// (which parse to [`Expr::Extension`]), and nested forms stay on the
/// interpreter, and this function returns `None` for them.
/// [`match_over_time_range_call`] matches the non-experimental members instead.
pub(crate) fn match_experimental_over_time_range_call(
    expr: &Expr,
) -> Option<(&MatrixSelector, OverTimeFn)> {
    let Expr::Call(call) = expr else {
        return None;
    };
    let kind = match call.func.name {
        "mad_over_time" => OverTimeFn::Mad,
        "first_over_time" => OverTimeFn::First,
        "ts_of_first_over_time" => OverTimeFn::TsOfFirst,
        "ts_of_last_over_time" => OverTimeFn::TsOfLast,
        "ts_of_min_over_time" => OverTimeFn::TsOfMin,
        "ts_of_max_over_time" => OverTimeFn::TsOfMax,
        _ => return None,
    };
    let [range] = call.args.args.as_slice() else {
        return None;
    };
    let mut arg = range.as_ref();
    while let Expr::Paren(paren) = arg {
        arg = paren.expr.as_ref();
    }
    let Expr::MatrixSelector(selector) = arg else {
        return None;
    };
    Some((selector, kind))
}
