use super::{FieldExpr, SpanMatcher, has_nested_scope, matcher_from_field_expr};

pub(crate) fn field_expr_to_matcher_disjuncts(fe: &FieldExpr) -> Option<Vec<Vec<SpanMatcher>>> {
    build_disjuncts(fe, false)
}

pub(super) fn build_disjuncts(fe: &FieldExpr, negated: bool) -> Option<Vec<Vec<SpanMatcher>>> {
    match fe {
        FieldExpr::Comparison { .. } | FieldExpr::Field(_) => {
            let matcher = matcher_from_field_expr(fe);
            if negated {
                matcher.map(|matcher| vec![vec![super::negate_matcher(matcher)]])
            } else {
                Some(vec![vec![matcher.expect(
                    "comparison and field expressions lower to matchers",
                )]])
            }
        }
        // A constant filter is the identity/annihilator of the matcher DNF, not
        // "unrepresentable": `true` is match-all (one disjunct with no matchers,
        // the AND-identity) and `false` is match-none (zero disjuncts). Returning
        // `None` here would poison an enclosing `And` via `?`, dropping the
        // sibling's matchers and the attribute columns they project (e.g.
        // `{ span.http.method != nil && true }` would lose the `attr.http.method`
        // projection and fail to plan).
        FieldExpr::Const(value) => Some(if *value ^ negated {
            vec![vec![]]
        } else {
            vec![]
        }),
        FieldExpr::And(a, b) | FieldExpr::Or(a, b) => {
            let left = build_disjuncts(a, negated)?;
            let right = build_disjuncts(b, negated)?;
            let is_and = matches!(fe, FieldExpr::And(..));
            if is_and ^ negated {
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
            } else {
                let mut out = left;
                out.extend(right);
                Some(out)
            }
        }
        FieldExpr::Not(inner) if has_nested_scope(inner) => {
            if negated {
                Some(build_disjuncts(inner, false)?)
            } else {
                Some(build_disjuncts(inner, true)?)
            }
        }
        FieldExpr::Not(inner) if negated => Some(build_disjuncts(inner, false)?),
        FieldExpr::ExpressionComparison { .. }
        | FieldExpr::FieldComparison { .. }
        | FieldExpr::Not(_) => None,
    }
}
