use super::{FieldExpr, Scope};

pub(crate) fn has_parent_scope(fe: &FieldExpr) -> bool {
    match fe {
        FieldExpr::Comparison { lhs, .. } | FieldExpr::Field(lhs) => {
            matches!(lhs.scope, Scope::Parent)
        }
        FieldExpr::And(a, b) | FieldExpr::Or(a, b) => has_parent_scope(a) || has_parent_scope(b),
        FieldExpr::Not(inner) => has_parent_scope(inner),
        FieldExpr::Const(_) => false,
    }
}
