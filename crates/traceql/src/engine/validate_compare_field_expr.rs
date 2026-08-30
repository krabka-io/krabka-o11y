use super::*;

pub(crate) fn validate_compare_field_expr(fe: &FieldExpr) -> Result<()> {
    match fe {
        FieldExpr::Comparison { lhs, .. } | FieldExpr::Field(lhs) => {
            compare_field_class(lhs).map(|_| ())
        }
        FieldExpr::And(a, b) | FieldExpr::Or(a, b) => {
            validate_compare_field_expr(a)?;
            validate_compare_field_expr(b)
        }
        FieldExpr::Not(inner) => validate_compare_field_expr(inner),
        FieldExpr::Const(_) => Ok(()),
    }
}
