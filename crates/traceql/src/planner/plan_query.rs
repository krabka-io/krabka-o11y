use super::{
    PlannedSpanset, PlannerContext, Query, Result, SpanStore, SpansetExpr, plan_spanset_sql,
    selector,
};

pub(crate) async fn plan_query<S: SpanStore>(
    store: &S,
    ctx: &PlannerContext,
    q: &Query,
) -> Result<PlannedSpanset> {
    if !q.pipeline.is_empty() {
        return plan_spanset_sql(store, ctx, &q.root, &q.pipeline).await;
    }
    match &q.root {
        SpansetExpr::Selector(fe) => selector::plan_selector(store, ctx, fe).await,
        SpansetExpr::And(_, _) | SpansetExpr::Or(_, _) | SpansetExpr::Structural { .. } => {
            plan_spanset_sql(store, ctx, &q.root, &[]).await
        }
    }
}
