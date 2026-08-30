use super::{CompareRegexCache, SpansetExpr, collect_field_expr_regexes};

/// Collects every `=~`/`!~` literal pattern in the selection.
///
/// This function compiles each pattern once into `cache`. It skips a pattern
/// that does not compile.
pub(crate) fn collect_selection_regexes(selection: &SpansetExpr, cache: &mut CompareRegexCache) {
    match selection {
        SpansetExpr::Selector(fe) => collect_field_expr_regexes(fe, cache),
        SpansetExpr::And(lhs, rhs) | SpansetExpr::Or(lhs, rhs) => {
            collect_selection_regexes(lhs, cache);
            collect_selection_regexes(rhs, cache);
        }
        SpansetExpr::Structural { .. } => {}
    }
}
