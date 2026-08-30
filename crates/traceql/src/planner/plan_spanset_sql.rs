use super::{
    Pipeline, PlannedSpanset, PlannerContext, Result, SpanStore, SpansetExpr, pipeline_to_sql,
    register_nested_selector_tables, scan_options_with_pipeline_projections, selector,
    spanset_to_sql,
};

pub(crate) async fn plan_spanset_sql<S: SpanStore>(
    store: &S,
    ctx: &PlannerContext,
    root: &SpansetExpr,
    pipeline: &[Pipeline],
) -> Result<PlannedSpanset> {
    let scan_options = scan_options_with_pipeline_projections(&ctx.scan_options, pipeline);
    let scan = store
        .scan_with_options(
            &ctx.tenant,
            &[],
            ctx.start_ns.into(),
            ctx.end_ns.into(),
            &scan_options,
        )
        .await?;
    let inspected = scan.inspected;
    let nested_tables = register_nested_selector_tables(store, ctx, &scan.ctx, root).await?;
    let spanset_sql = spanset_to_sql(root, &selector::ident(&scan.span_table), &nested_tables)?;
    let sql = pipeline_to_sql(&spanset_sql, pipeline)?;
    let df = scan.ctx.sql(&sql).await?;
    let plan = df.into_unoptimized_plan();
    Ok(PlannedSpanset {
        ctx: scan.ctx,
        plan,
        inspected,
    })
}
