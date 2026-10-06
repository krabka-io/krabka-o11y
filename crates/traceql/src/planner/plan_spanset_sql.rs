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
    let (span_table, sampling_factor) =
        super::sample_metric_scan::sample_metric_scan(&scan.ctx, &scan.span_table, &scan_options)
            .await?;
    let mut selectors = Vec::new();
    super::collect_field_selectors(root, &mut selectors);
    super::register_field_comparison_columns(&scan.ctx, &span_table, &selectors).await?;
    let inspected = scan.inspected;
    let nested_tables = register_nested_selector_tables(store, ctx, &scan.ctx, root).await?;
    let spanset_sql = spanset_to_sql(root, &selector::ident(&span_table), &nested_tables)?;
    if super::apply_expression_pipeline::uses_expression_pipeline(pipeline) {
        let batches = scan.ctx.sql(&spanset_sql).await?.collect().await?;
        let spanset_pipeline_had_input = batches.iter().any(|batch| batch.num_rows() > 0);
        let batches =
            super::apply_expression_pipeline::apply_expression_pipeline(&batches, pipeline)?;
        super::register_batches(&scan.ctx, "expression_pipeline", batches)?;
        return Ok(PlannedSpanset {
            plan: scan
                .ctx
                .table("expression_pipeline")
                .await?
                .into_unoptimized_plan(),
            ctx: scan.ctx,
            inspected,
            sampling_factor,
            spanset_pipeline_had_input,
        });
    }
    let sql = pipeline_to_sql(&spanset_sql, pipeline)?;
    let df = scan.ctx.sql(&sql).await?;
    let plan = df.into_unoptimized_plan();
    Ok(PlannedSpanset {
        ctx: scan.ctx,
        plan,
        inspected,
        sampling_factor,
        spanset_pipeline_had_input: false,
    })
}
