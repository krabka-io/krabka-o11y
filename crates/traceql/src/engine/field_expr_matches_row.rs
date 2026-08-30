use super::*;

pub(crate) fn field_expr_matches_row(fe: &FieldExpr, row: &CompareRow, regexes: &CompareRegexCache) -> bool {
    match fe {
        FieldExpr::Const(value) => *value,
        FieldExpr::And(a, b) => {
            field_expr_matches_row(a, row, regexes) && field_expr_matches_row(b, row, regexes)
        }
        FieldExpr::Or(a, b) => {
            field_expr_matches_row(a, row, regexes) || field_expr_matches_row(b, row, regexes)
        }
        FieldExpr::Not(inner) => !field_expr_matches_row(inner, row, regexes),
        FieldExpr::Field(field) => compare_field_present(field, row),
        FieldExpr::Comparison { lhs, op, rhs } => {
            compare_comparison_matches(lhs, *op, rhs, row, regexes)
        }
    }
}
