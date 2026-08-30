use super::*;

pub(crate) async fn execute_metric_query_range_with_hot_tail_frontier_and_deletes(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    evaluation: (TimeRange, i64),
    hot_tail: QueryHotTail<'_>,
) -> Result<Value, QueryError> {
    let (eval_range, step_ns) = evaluation;
    if step_ns <= 0 {
        return Err(QueryError::InvalidStep(step_ns));
    }

    let eval_times = eval_times(eval_range, step_ns);
    let mut samples = BTreeMap::new();

    if !plan.blocks.is_empty() && !plan.fingerprints.is_empty() {
        let ctx = SessionContext::new();
        register_log_blocks(&ctx, "logs", root, &plan.blocks)?;
        let sql = metric_plan_scan_sql(plan, query, eval_range)?;
        let batches = ctx.sql(&sql).await?.collect().await?;
        samples = metric_samples_from_batches(
            &batches,
            plan,
            query,
            label_index,
            &eval_times,
            hot_tail.delete_filters,
        )?;
    }

    for record in hot_tail.records {
        append_matching_hot_metric_record(
            &mut samples,
            plan,
            record,
            hot_tail.frontier,
            MetricWindow {
                query,
                eval_times: &eval_times,
                range_ns: query.range_ns.0,
                delete_filters: hot_tail.delete_filters,
            },
        )?;
    }
    apply_absent_over_time(&mut samples, query, &eval_times);

    Ok(loki_matrix_response(format_metric_samples(samples, query)))
}
