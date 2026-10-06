use super::Call;

/// Returns the value of the string-literal call argument at `index`.
///
/// Parentheses around the literal are transparent, because Prometheus's parser
/// treats `(("dst"))` as the string `dst`. Returns `None` when the argument is
/// absent or is not a string literal. Unlike `string_literal_arg`, this function
/// never returns an error. The label-ops planner uses it to probe the call shape
/// and falls back on any mismatch.
pub(crate) fn string_literal_value(call: &Call, index: usize) -> Option<crate::PromqlString> {
    crate::planner::byte_string_expr::string_expr_value(call.args.args.get(index)?.as_ref())
}
