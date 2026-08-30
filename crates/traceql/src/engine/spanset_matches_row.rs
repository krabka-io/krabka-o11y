use super::*;

pub(crate) fn spanset_matches_row(
    selection: &SpansetExpr,
    row: &CompareRow,
    regexes: &CompareRegexCache,
) -> bool {
    match selection {
        SpansetExpr::Selector(fe) => field_expr_matches_row(fe, row, regexes),
        SpansetExpr::And(lhs, rhs) => {
            spanset_matches_row(lhs, row, regexes) && spanset_matches_row(rhs, row, regexes)
        }
        SpansetExpr::Or(lhs, rhs) => {
            spanset_matches_row(lhs, row, regexes) || spanset_matches_row(rhs, row, regexes)
        }
        // Rejected by validate_compare_selection; treat as non-match defensively.
        SpansetExpr::Structural { .. } => false,
    }
}
