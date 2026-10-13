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
    Ok(
        match (ColumnComparison {
            col,
            field,
            op,
            operand: value,
        }) {
            ColumnComparison {
                op: ComparisonOp::Eq,
                operand: Value::Nil,
                ..
            } => format!("{col} IS NULL"),
            ColumnComparison {
                op: ComparisonOp::Neq,
                operand: Value::Nil,
                ..
            } => format!("{col} IS NOT NULL"),
            ColumnComparison {
                op: ComparisonOp::Re,
                operand: Value::Str(pattern),
                ..
            } => {
                format!("regexp_like({col}, {})", string_lit(&anchored(pattern)))
            }
            ColumnComparison {
                op: ComparisonOp::Nre,
                operand: Value::Str(pattern),
                ..
            } => {
                format!("NOT regexp_like({col}, {})", string_lit(&anchored(pattern)))
            }
            ColumnComparison {
                op: ComparisonOp::Eq,
                operand: v,
                ..
            } => format!("{col} = {}", comparison_value_sql(field, v)?),
            ColumnComparison {
                op: ComparisonOp::Neq,
                operand: v,
                ..
            } => format!("{col} != {}", comparison_value_sql(field, v)?),
            ColumnComparison {
                op: ComparisonOp::Lt,
                operand: v,
                ..
            } => format!("{col} < {}", comparison_value_sql(field, v)?),
            ColumnComparison {
                op: ComparisonOp::Lte,
                operand: v,
                ..
            } => format!("{col} <= {}", comparison_value_sql(field, v)?),
            ColumnComparison {
                op: ComparisonOp::Gt,
                operand: v,
                ..
            } => format!("{col} > {}", comparison_value_sql(field, v)?),
            ColumnComparison {
                op: ComparisonOp::Gte,
                operand: v,
                ..
            } => format!("{col} >= {}", comparison_value_sql(field, v)?),
            ColumnComparison {
                op: ComparisonOp::Re | ComparisonOp::Nre,
                ..
            } => {
                return Err(TraceqlError::Plan(
                    "regex comparison requires string value".into(),
                ));
            }
        },
    )
}
