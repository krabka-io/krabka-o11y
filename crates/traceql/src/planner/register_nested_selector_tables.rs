use super::{
    FieldExpr, PlannerContext, Result, SessionContext, SpanStore, SpansetExpr,
    collect_nested_selectors, collect_table, register_batches, selector,
};

pub(crate) async fn register_nested_selector_tables<S: SpanStore>(
    store: &S,
    ctx: &PlannerContext,
    target_ctx: &SessionContext,
    root: &SpansetExpr,
) -> Result<Vec<(FieldExpr, String)>> {
    let mut selectors = Vec::new();
    collect_nested_selectors(root, &mut selectors);

    let mut tables = Vec::new();
    for (idx, selector) in selectors.into_iter().enumerate() {
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
        let batches = collect_table(&scan.ctx, &scan.span_table).await?;
        register_batches(target_ctx, &table_name, batches)?;
        tables.push((selector, table_name));
    }
    Ok(tables)
}
