use super::{FieldExpr, Intrinsic, Scope, field_expr_has_field::field_expr_has_field};

pub(crate) fn has_nested_scope(fe: &FieldExpr) -> bool {
    field_expr_has_field(fe, &|field| {
        matches!(field.scope, Scope::Event | Scope::Link)
            || matches!(
                field.scope,
                Scope::Intrinsic(
                    Intrinsic::EventName
                        | Intrinsic::EventTimeSinceStart
                        | Intrinsic::LinkTraceId
                        | Intrinsic::LinkSpanId
                )
            )
    })
}
