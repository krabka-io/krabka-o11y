use super::{FieldExpr, Intrinsic, Scope};

pub(crate) fn has_nested_scope(fe: &FieldExpr) -> bool {
    match fe {
        FieldExpr::ExpressionComparison { lhs, rhs, .. } => {
            let mut fields = Vec::new();
            lhs.collect_fields(&mut fields);
            rhs.collect_fields(&mut fields);
            fields
                .into_iter()
                .any(|field| has_nested_scope(&FieldExpr::Field(field.clone())))
        }
        FieldExpr::FieldComparison { lhs, rhs, .. } => {
            has_nested_scope(&FieldExpr::Field(lhs.clone()))
                || has_nested_scope(&FieldExpr::Field(rhs.clone()))
        }
        FieldExpr::Comparison { lhs, .. } | FieldExpr::Field(lhs) => {
            matches!(lhs.scope, Scope::Event | Scope::Link)
                || matches!(
                    lhs.scope,
                    Scope::Intrinsic(
                        Intrinsic::EventName
                            | Intrinsic::EventTimeSinceStart
                            | Intrinsic::LinkTraceId
                            | Intrinsic::LinkSpanId
                    )
                )
        }
        FieldExpr::And(a, b) | FieldExpr::Or(a, b) => has_nested_scope(a) || has_nested_scope(b),
        FieldExpr::Not(inner) => has_nested_scope(inner),
        FieldExpr::Const(_) => false,
    }
}
