use super::{Call, Expr};

/// Returns the value of the string-literal call argument at `index`.
///
/// Parentheses around the literal are transparent, because Prometheus's parser
/// treats `(("dst"))` as the string `dst`. Returns `None` when the argument is
/// absent or is not a string literal. Unlike `string_literal_arg`, this function
/// never returns an error. The label-ops planner uses it to probe the call shape
/// and falls back on any mismatch.
pub(crate) fn string_literal_value(call: &Call, index: usize) -> Option<String> {
    let mut arg = call.args.args.get(index).map(Box::as_ref)?;
    while let Expr::Paren(paren) = arg {
        arg = &paren.expr;
    }
    match arg {
        Expr::StringLiteral(value) => Some(value.val.clone()),
        _ => None,
    }
}
