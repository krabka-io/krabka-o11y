use arrow::{
    array::BooleanArray,
    datatypes::{DataType, Field, Schema},
};
use crate::{ast::field_comparison_column, engine::field_comparison_column_values};
use super::{
    Arc, FieldExpr, MemTable, PlannerContext, RecordBatch, Result, SessionContext, SpanStore,
    SpansetExpr, collect_nested_selectors, collect_table, selector,
};

pub(crate) fn register_batches(
    ctx: &SessionContext,
    table_name: &str,
    batches: Vec<RecordBatch>,
) -> Result<()> {
    let schema = batches
        .first()
        .map_or_else(crate::span_columns::span_schema, RecordBatch::schema);
    let table = MemTable::try_new(schema, vec![batches])?;
    ctx.register_table(table_name, Arc::new(table))?;
    Ok(())
}

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

pub(crate) struct NestedSelectorTable {
    pub(crate) selector: FieldExpr,
    pub(crate) table_name: String,
}

pub(crate) async fn register_nested_selector_tables<S: SpanStore>(
    store: &S,
    ctx: &PlannerContext,
    target_ctx: &SessionContext,
    root: &SpansetExpr,
) -> Result<Vec<NestedSelectorTable>> {
    let mut selectors = Vec::new();
    collect_nested_selectors(root, &mut selectors);

    let mut tables = Vec::new();
    for entry in selectors.into_iter().enumerate() {
        let idx = entry.0;
        let selector = entry.1;
        let table_name = format!("nested_selector_{idx}");
        let scan = store
            .scan_with_options(
                &ctx.tenant,
                &selector::field_expr_to_matchers(&selector),
                ctx.start_ns.into(),
                ctx.end_ns.into(),
                &ctx.scan_options,
            )
            .await?;
        super::register_field_comparison_columns(&scan.ctx, &scan.span_table, &[&selector]).await?;
        let batches = collect_table(&scan.ctx, &scan.span_table).await?;
        register_batches(target_ctx, &table_name, batches)?;
        tables.push(NestedSelectorTable {
            selector,
            table_name,
        });
    }
    Ok(tables)
}
