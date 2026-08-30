use super::{
    ComparisonOp, Field, Result, TraceqlError, Value, anchored, comparison_value_sql,
    field_to_column, ident, string_lit,
};

pub(crate) fn comparison_to_sql(field: &Field, op: ComparisonOp, value: &Value) -> Result<String> {
    let col = ident(&field_to_column(field));
    Ok(match (op, value) {
        (ComparisonOp::Eq, Value::Nil) => format!("{col} IS NULL"),
        (ComparisonOp::Neq, Value::Nil) => format!("{col} IS NOT NULL"),
        (ComparisonOp::Re, Value::Str(pattern)) => {
            format!("regexp_like({col}, {})", string_lit(&anchored(pattern)))
        }
        (ComparisonOp::Nre, Value::Str(pattern)) => {
            format!("NOT regexp_like({col}, {})", string_lit(&anchored(pattern)))
        }
        (ComparisonOp::Eq, v) => format!("{col} = {}", comparison_value_sql(field, v)?),
        (ComparisonOp::Neq, v) => format!("{col} != {}", comparison_value_sql(field, v)?),
        (ComparisonOp::Lt, v) => format!("{col} < {}", comparison_value_sql(field, v)?),
        (ComparisonOp::Lte, v) => format!("{col} <= {}", comparison_value_sql(field, v)?),
        (ComparisonOp::Gt, v) => format!("{col} > {}", comparison_value_sql(field, v)?),
        (ComparisonOp::Gte, v) => format!("{col} >= {}", comparison_value_sql(field, v)?),
        (ComparisonOp::Re | ComparisonOp::Nre, _) => {
            return Err(TraceqlError::Plan(
                "regex comparison requires string value".into(),
            ));
        }
    })
}
