use super::*;

pub(crate) fn has_nested_scope(fe: &FieldExpr) -> bool {
    match fe {
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
