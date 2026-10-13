use super::{FieldExpr, SpanMatcher, field_expr_to_matcher_disjuncts::build_disjuncts};

pub(crate) fn field_expr_to_negated_matcher_disjuncts(
    fe: &FieldExpr,
) -> Option<Vec<Vec<SpanMatcher>>> {
    build_disjuncts(fe, true)
}
