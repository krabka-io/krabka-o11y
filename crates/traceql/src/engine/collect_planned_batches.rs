use super::*;

pub(crate) async fn collect_planned_batches(
    planned: crate::planner::PlannedSpanset,
) -> Result<Vec<RecordBatch>> {
    Ok(planned
        .ctx
        .execute_logical_plan(planned.plan)
        .await?
        .collect()
        .await?)
}
