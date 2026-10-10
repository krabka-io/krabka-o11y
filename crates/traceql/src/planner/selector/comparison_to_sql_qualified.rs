use super::{
    ColumnComparison, ComparisonOp, Field, Result, Value, column_comparison_sql,
    qualified_field_ident,
};

/// A `field <op> operand` comparison inside a span-to-parent join, whose
/// span and parent sides are the tables aliased `span_alias` and
/// `parent_alias`.
pub(crate) struct QualifiedComparison<'a> {
    pub(crate) field: &'a Field,
    pub(crate) op: ComparisonOp,
    pub(crate) operand: &'a Value,
    pub(crate) span_alias: &'a str,
    pub(crate) parent_alias: &'a str,
}

pub(crate) fn comparison_to_sql_qualified(comparison: &QualifiedComparison<'_>) -> Result<String> {
    let col = qualified_field_ident(
        comparison.field,
        comparison.span_alias,
        comparison.parent_alias,
    );
    column_comparison_sql(ColumnComparison {
        col: &col,
        field: comparison.field,
        op: comparison.op,
        operand: comparison.operand,
    })
}
