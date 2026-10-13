use super::{Field, FieldExpr};

pub(super) fn field_expr_has_field(fe: &FieldExpr, predicate: &impl Fn(&Field) -> bool) -> bool {
    match fe {
        FieldExpr::ExpressionComparison { lhs, rhs, .. } => {
            let mut fields = Vec::new();
            lhs.collect_fields(&mut fields);
            rhs.collect_fields(&mut fields);
            fields.into_iter().any(|field| predicate(&field))
        }
        FieldExpr::FieldComparison { lhs, rhs, .. } => predicate(lhs) || predicate(rhs),
        FieldExpr::Comparison { lhs, .. } | FieldExpr::Field(lhs) => predicate(lhs),
        FieldExpr::And(a, b) | FieldExpr::Or(a, b) => {
            field_expr_has_field(a, predicate) || field_expr_has_field(b, predicate)
        }
        FieldExpr::Not(inner) => field_expr_has_field(inner, predicate),
        FieldExpr::Const(_) => false,
    }
}
