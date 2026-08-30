use super::*;

pub(crate) fn matcher_from_field_expr(fe: &FieldExpr) -> Option<SpanMatcher> {
    match fe {
        FieldExpr::Comparison { lhs, op, rhs } => Some(SpanMatcher {
            scope: match_scope(&lhs.scope),
            key: matcher_key(lhs),
            op: match_cmp(*op),
            value: match_value(rhs),
            negated: false,
        }),
        FieldExpr::Field(field) => Some(SpanMatcher {
            scope: match_scope(&field.scope),
            key: matcher_key(field),
            op: MatchCmp::Neq,
            value: MatchValue::Nil,
            negated: false,
        }),
        FieldExpr::And(_, _) | FieldExpr::Or(_, _) | FieldExpr::Not(_) | FieldExpr::Const(_) => {
            None
        }
    }
}
