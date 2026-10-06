use std::sync::Arc;

use arrow::{
    array::BooleanArray,
    datatypes::{DataType, Field, Schema},
};

use super::{
    FieldExpr, RecordBatch, Result, SessionContext, SpansetExpr, collect_table, register_batches,
};
use crate::{ast::field_comparison_column, engine::field_comparison_column_values};

pub(crate) async fn register_field_comparison_columns(
    ctx: &SessionContext,
    table: &str,
    selectors: &[&FieldExpr],
) -> Result<()> {
    let mut comparisons = Vec::new();
    for selector in selectors {
        if selector.has_field_comparison() && !comparisons.contains(selector) {
            comparisons.push(*selector);
        }
    }
    if comparisons.is_empty() {
        return Ok(());
    }
    let table_schema = ctx.table(table).await?.schema().as_arrow().clone();
    let mut batches = collect_table(ctx, table).await?;
    if batches.is_empty() {
        batches.push(RecordBatch::new_empty(Arc::new(table_schema)));
    }
    for batch in &mut batches {
        let mut fields = batch.schema().fields().to_vec();
        let mut columns = batch.columns().to_vec();
        for expr in &comparisons {
            fields.push(Arc::new(Field::new(
                field_comparison_column(expr),
                DataType::Boolean,
                false,
            )));
            columns.push(Arc::new(BooleanArray::from(
                field_comparison_column_values(expr, batch)?,
            )));
        }
        *batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .map_err(|error| crate::TraceqlError::Exec(error.to_string()))?;
    }
    ctx.deregister_table(table)?;
    register_batches(ctx, table, batches)
}

pub(crate) fn collect_field_selectors<'a>(expr: &'a SpansetExpr, out: &mut Vec<&'a FieldExpr>) {
    match expr {
        SpansetExpr::Selector(expr) => out.push(expr),
        SpansetExpr::And(lhs, rhs)
        | SpansetExpr::Or(lhs, rhs)
        | SpansetExpr::Structural { lhs, rhs, .. } => {
            collect_field_selectors(lhs, out);
            collect_field_selectors(rhs, out);
        }
    }
}
