use super::{
    ComparisonOp, Field, Result, TraceqlError, Value, anchored, comparison_value_sql, string_lit,
};

/// A comparison of an already-quoted column expression against a value.
#[derive(Clone, Copy)]
pub(crate) struct ColumnComparison<'a> {
    pub(crate) col: &'a str,
    /// Decides how the comparison value is typed.
    pub(crate) field: &'a Field,
    pub(crate) op: ComparisonOp,
    pub(crate) operand: &'a Value,
}

/// Renders `col <op> operand`.
pub(crate) fn column_comparison_sql(comparison: ColumnComparison<'_>) -> Result<String> {
    let ColumnComparison {
        col,
        field,
        op,
        operand: value,
    } = comparison;
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
