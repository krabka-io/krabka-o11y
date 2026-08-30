use super::{
    FieldExpr, PlannedSpanset, PlannerContext, Result, SpanStore, field_expr_to_matcher_disjuncts,
    field_expr_to_matchers, has_nested_scope, has_parent_scope, ident,
    needs_unfiltered_parent_table, plan_selector_disjuncts, register_unfiltered_parent_table,
    selector_sql_with_parent_table,
};

pub(crate) async fn plan_selector<S: SpanStore>(
    store: &S,
    ctx: &PlannerContext,
    fe: &FieldExpr,
) -> Result<PlannedSpanset> {
    if has_nested_scope(fe)
        && let Some(disjuncts) = field_expr_to_matcher_disjuncts(fe)
        && disjuncts.len() > 1
    {
        return plan_selector_disjuncts(store, ctx, &disjuncts).await;
    }

    let matchers = field_expr_to_matchers(fe);
    let scan = store
        .scan_with_options(
            &ctx.tenant,
            &matchers,
            ctx.start_ns.into(),
            ctx.end_ns.into(),
            &ctx.scan_options,
        )
        .await?;
    let inspected = scan.inspected;
    let parent_table = if needs_unfiltered_parent_table(fe) {
        register_unfiltered_parent_table(store, ctx, &scan.ctx).await?
    } else {
        scan.span_table.clone()
    };
    if !has_nested_scope(fe)
        && !has_parent_scope(fe)
        && field_expr_to_matcher_disjuncts(fe).is_some_and(|disjuncts| disjuncts.len() == 1)
    {
        let plan = scan
            .ctx
            .table(&scan.span_table)
            .await?
            .into_unoptimized_plan();
        return Ok(PlannedSpanset {
            ctx: scan.ctx,
            plan,
            inspected,
        });
    }
    let table = ident(&scan.span_table);
    let sql = selector_sql_with_parent_table(&table, &ident(&parent_table), fe)?;
    let df = scan.ctx.sql(&sql).await?;
    let plan = df.into_unoptimized_plan();
    Ok(PlannedSpanset {
        ctx: scan.ctx,
        plan,
        inspected,
    })
}
