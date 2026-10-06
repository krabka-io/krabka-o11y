use super::{FieldExpr, Result, compare_field_class};

pub(crate) fn validate_compare_field_expr(fe: &FieldExpr) -> Result<()> {
    match fe {
        FieldExpr::ExpressionComparison { lhs, rhs, .. } => {
            let mut fields = Vec::new();
            lhs.collect_fields(&mut fields);
            rhs.collect_fields(&mut fields);
            for field in fields {
                compare_field_class(field)?;
            }
            Ok(())
        }
        FieldExpr::FieldComparison { lhs, rhs, .. } => {
            compare_field_class(lhs)?;
            compare_field_class(rhs).map(|_| ())
        }
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
