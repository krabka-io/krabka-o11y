use super::*;

pub(crate) fn field_expr_to_matchers(fe: &FieldExpr) -> Vec<SpanMatcher> {
    match fe {
        FieldExpr::And(a, b) => {
            let mut out = field_expr_to_matchers(a);
            out.extend(field_expr_to_matchers(b));
            out
        }
        FieldExpr::Comparison { .. } => matcher_from_field_expr(fe).into_iter().collect(),
        FieldExpr::Not(inner) if has_nested_scope(inner) => {
            field_expr_to_negated_matcher_disjuncts(inner)
                .filter(|disjuncts| disjuncts.len() == 1)
                .and_then(|mut disjuncts| disjuncts.pop())
                .unwrap_or_default()
        }
        // A constant filter carries no per-span matcher; the SQL predicate
        // (`TRUE`/`FALSE`) is authoritative, so it contributes no pre-filter.
        FieldExpr::Or(_, _) | FieldExpr::Not(_) | FieldExpr::Field(_) | FieldExpr::Const(_) => {
            vec![]
        }
    }
}
