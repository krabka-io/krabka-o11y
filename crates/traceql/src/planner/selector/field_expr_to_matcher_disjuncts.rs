use super::{
    FieldExpr, SpanMatcher, field_expr_to_negated_matcher_disjuncts, has_nested_scope,
    matcher_from_field_expr,
};

pub(crate) fn field_expr_to_matcher_disjuncts(fe: &FieldExpr) -> Option<Vec<Vec<SpanMatcher>>> {
    match fe {
        FieldExpr::Comparison { .. } | FieldExpr::Field(_) => {
            Some(vec![vec![matcher_from_field_expr(fe).expect(
                "comparison and field expressions lower to matchers",
            )]])
        }
        // A constant filter is the identity/annihilator of the matcher DNF, not
        // "unrepresentable": `true` is match-all (one disjunct with no matchers,
        // the AND-identity) and `false` is match-none (zero disjuncts). Returning
        // `None` here would poison an enclosing `And` via `?`, dropping the
        // sibling's matchers and the attribute columns they project (e.g.
        // `{ span.http.method != nil && true }` would lose the `attr.http.method`
        // projection and fail to plan).
        FieldExpr::Const(value) => Some(if *value { vec![vec![]] } else { vec![] }),
        FieldExpr::And(a, b) => {
            let left = field_expr_to_matcher_disjuncts(a)?;
            let right = field_expr_to_matcher_disjuncts(b)?;
            Some(
                left.iter()
                    .flat_map(|l| {
                        right.iter().map(move |r| {
                            let mut out = l.clone();
                            out.extend(r.clone());
                            out
                        })
                    })
                    .collect(),
            )
        }
        FieldExpr::Or(a, b) => {
            let mut out = field_expr_to_matcher_disjuncts(a)?;
            out.extend(field_expr_to_matcher_disjuncts(b)?);
            Some(out)
        }
        FieldExpr::Not(inner) if has_nested_scope(inner) => {
            field_expr_to_negated_matcher_disjuncts(inner)
        }
        FieldExpr::Not(_) => None,
    }
}
