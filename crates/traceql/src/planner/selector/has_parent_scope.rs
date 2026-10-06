use super::{FieldExpr, Scope};

pub(crate) fn has_parent_scope(fe: &FieldExpr) -> bool {
    match fe {
        FieldExpr::ExpressionComparison { lhs, rhs, .. } => {
            let mut fields = Vec::new();
            lhs.collect_fields(&mut fields);
            rhs.collect_fields(&mut fields);
            fields
                .into_iter()
                .any(|field| has_parent_scope(&FieldExpr::Field(field.clone())))
        }
        FieldExpr::FieldComparison { lhs, rhs, .. } => {
            has_parent_scope(&FieldExpr::Field(lhs.clone()))
                || has_parent_scope(&FieldExpr::Field(rhs.clone()))
        }
        FieldExpr::Comparison { lhs, .. } | FieldExpr::Field(lhs) => {
            matches!(lhs.scope, Scope::Parent)
        }
        FieldExpr::And(a, b) | FieldExpr::Or(a, b) => has_parent_scope(a) || has_parent_scope(b),
        FieldExpr::Not(inner) => has_parent_scope(inner),
        FieldExpr::Const(_) => false,
    }
}
