use super::*;

pub(crate) async fn execute_metric_query_range_from_object_store_with_hot_tail_frontier(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    evaluation: (TimeRange, i64),
    hot_tail: QueryHotTail<'_>,
) -> Result<Value, QueryError> {
    execute_metric_query_range_from_object_store_with_hot_tail_frontier_and_deletes(
        store,
        prefix,
        plan,
        query,
        label_index,
        evaluation,
        hot_tail,
    )
    .await
}

pub(crate) async fn execute_metric_query_range_from_object_store_with_hot_tail_frontier_and_deletes(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
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
    let mut warnings = Vec::new();

    if !plan.blocks.is_empty() && !plan.fingerprints.is_empty() {
        for block in &plan.blocks {
            let Ok(batches) = collect_object_store_metric_log_batches(
                Arc::clone(&store),
                prefix,
                block,
                plan,
                query,
                eval_range,
            )
            .await
            else {
                warnings.push(format!("failed to read block {}", block.key.object_key()));
                continue;
            };
            let block_samples = metric_samples_from_batches(
                &batches,
                plan,
                query,
                label_index,
                &eval_times,
                hot_tail.delete_filters,
            )?;
            merge_metric_samples(&mut samples, block_samples);
        }
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

    Ok(loki_matrix_response_with_warnings(
        format_metric_samples(samples, query),
        &warnings,
    ))
}
