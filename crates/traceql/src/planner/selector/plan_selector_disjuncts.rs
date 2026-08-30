use super::*;

pub(crate) async fn plan_selector_disjuncts<S: SpanStore>(
    store: &S,
    ctx: &PlannerContext,
    disjuncts: &[Vec<SpanMatcher>],
) -> Result<PlannedSpanset> {
    let mut batches = Vec::new();
    let mut schema = None;
    let mut inspected = <ByteSize as ByteSizeExt>::ZERO;
    for matchers in disjuncts {
        let scan = store
            .scan_with_options(
                &ctx.tenant,
                matchers,
                ctx.start_ns.into(),
                ctx.end_ns.into(),
                &ctx.scan_options,
            )
            .await?;
        inspected += scan.inspected;
        let mut scan_batches = collect_table(&scan.ctx, &scan.span_table).await?;
        if schema.is_none() {
            schema = scan_batches.first().map(RecordBatch::schema);
        }
        batches.append(&mut scan_batches);
    }

    let schema = schema.unwrap_or_else(crate::span_columns::span_schema);
    let ctx = SessionContext::new();
    let table = MemTable::try_new(schema, vec![batches])?;
    ctx.register_table("spans", Arc::new(table))?;
    let df = ctx.sql("SELECT DISTINCT * FROM spans").await?;
    let plan = df.into_unoptimized_plan();
    Ok(PlannedSpanset {
        ctx,
        plan,
        inspected,
    })
}
