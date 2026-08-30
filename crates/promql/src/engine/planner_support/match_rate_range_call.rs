use super::*;

/// Recognizes a top-level `f(selector[range])` rate-family call.
///
/// This function returns the inner [`MatrixSelector`] and the UDF kind for the
/// operator path. A call is eligible if and only if `expr` is a [`Call`] whose
/// function is one of `rate|increase|delta|irate|idelta`, with exactly one
/// argument that is a plain [`Expr::MatrixSelector`] after the removal of
/// parentheses. An `anchored`/`smoothed` selector parses to [`Expr::Extension`],
/// not to a plain `MatrixSelector`, so this function rejects it and it stays on
/// the interpreter. Nested forms (`sum(rate(...))`), `_over_time`, subqueries,
/// and every other function also stay on the interpreter.
pub(crate) fn match_rate_range_call(expr: &Expr) -> Option<(&MatrixSelector, RateUdfKind)> {
    let Expr::Call(call) = expr else {
        return None;
    };
    let kind = RateUdfKind::from_function_name(call.func.name)?;
    let [arg] = call.args.args.as_slice() else {
        return None;
    };
    let mut arg = arg.as_ref();
    while let Expr::Paren(paren) = arg {
        arg = paren.expr.as_ref();
    }
    let Expr::MatrixSelector(selector) = arg else {
        return None;
    };
    Some((selector, kind))
}
