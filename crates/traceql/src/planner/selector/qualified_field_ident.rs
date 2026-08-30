use super::{Field, Scope, field_to_column, ident};

pub(crate) fn qualified_field_ident(field: &Field, span_alias: &str, parent_alias: &str) -> String {
    let alias = if matches!(field.scope, Scope::Parent) {
        parent_alias
    } else {
        span_alias
    };
    format!("{alias}.{}", ident(&field_to_column(field)))
}
