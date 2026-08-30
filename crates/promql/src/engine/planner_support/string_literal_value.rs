use super::*;

/// Returns the value of the string-literal call argument at `index`.
///
/// Returns `None` when the argument is absent or is not a string literal. Unlike
/// `string_literal_arg`, this function never returns an error. The label-ops
/// planner uses it to probe the call shape and falls back to the interpreter on
/// any mismatch. The interpreter then raises the canonical error.
pub(crate) fn string_literal_value(call: &Call, index: usize) -> Option<String> {
    match call.args.args.get(index).map(Box::as_ref) {
        Some(Expr::StringLiteral(value)) => Some(value.val.clone()),
        _ => None,
    }
}
