use super::*;

pub(crate) fn field_expr_to_negated_matcher_disjuncts(fe: &FieldExpr) -> Option<Vec<Vec<SpanMatcher>>> {
    match fe {
        FieldExpr::Comparison { .. } | FieldExpr::Field(_) => {
            matcher_from_field_expr(fe).map(|matcher| vec![vec![negate_matcher(matcher)]])
        }
        FieldExpr::Or(a, b) => {
            let left = field_expr_to_negated_matcher_disjuncts(a)?;
            let right = field_expr_to_negated_matcher_disjuncts(b)?;
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
        FieldExpr::And(a, b) => {
            let mut out = field_expr_to_negated_matcher_disjuncts(a)?;
            out.extend(field_expr_to_negated_matcher_disjuncts(b)?);
            Some(out)
        }
        FieldExpr::Not(inner) => field_expr_to_matcher_disjuncts(inner),
        // Negated constant: `!true` is match-none (zero disjuncts), `!false` is
        // match-all (one empty disjunct). Mirrors the non-negated identity so a
        // `Const` sibling never poisons an enclosing conjunction.
        FieldExpr::Const(value) => Some(if *value { vec![] } else { vec![vec![]] }),
    }
}
