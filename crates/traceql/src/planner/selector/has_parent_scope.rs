use super::{FieldExpr, Scope, field_expr_has_field::field_expr_has_field};

pub(crate) fn has_parent_scope(fe: &FieldExpr) -> bool {
    field_expr_has_field(fe, &|field| matches!(field.scope, Scope::Parent))
}
