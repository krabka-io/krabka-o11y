use super::{FieldExpr, has_nested_scope, has_parent_scope};

pub(crate) fn needs_unfiltered_parent_table(fe: &FieldExpr) -> bool {
    has_nested_scope(fe) && has_parent_scope(fe)
}
