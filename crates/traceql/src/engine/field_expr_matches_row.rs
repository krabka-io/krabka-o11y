use super::{
    CompareRegexCache, CompareRow, FieldExpr, compare_comparison_matches, compare_field_present,
};

pub(crate) fn field_expr_matches_row(
    fe: &FieldExpr,
    row: &CompareRow,
    regexes: &CompareRegexCache,
) -> bool {
    match fe {
        FieldExpr::ExpressionComparison { lhs, op, rhs } => {
            super::scalar_expression_value(lhs, row)
                .ok()
                .zip(super::scalar_expression_value(rhs, row).ok())
                .is_some_and(|(lhs, rhs)| super::field_comparison::scalar_matches(&lhs, *op, &rhs))
        }
        FieldExpr::Const(value) => *value,
        FieldExpr::And(a, b) => {
            field_expr_matches_row(a, row, regexes) && field_expr_matches_row(b, row, regexes)
        }
        FieldExpr::Or(a, b) => {
            field_expr_matches_row(a, row, regexes) || field_expr_matches_row(b, row, regexes)
        }
        FieldExpr::Not(inner) => !field_expr_matches_row(inner, row, regexes),
        FieldExpr::Field(field) => compare_field_present(field, row),
        FieldExpr::FieldComparison { lhs, op, rhs } => {
            super::field_comparison::field_comparison_matches(lhs, *op, rhs, row)
        }
        FieldExpr::Comparison { lhs, op, rhs } => {
            compare_comparison_matches(lhs, *op, rhs, row, regexes)
        }
    }
}
