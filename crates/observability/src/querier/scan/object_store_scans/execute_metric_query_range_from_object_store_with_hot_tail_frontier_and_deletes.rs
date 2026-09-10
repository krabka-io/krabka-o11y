use super::{
    Arc, BTreeMap, ColdBlockScan, LabelIndex, MetricQuery, MetricWindow, QueryError, QueryHotTail,
    StreamPlan, TimeRange, Value, append_matching_hot_metric_record, apply_absent_over_time,
    collect_object_store_metric_log_batches, eval_times, format_metric_samples,
    loki_matrix_response_with_warnings, merge_metric_samples, metric_samples_from_batches,
};

pub(crate) async fn execute_metric_query_range_from_object_store_with_hot_tail_frontier_and_deletes(
    cold: ColdBlockScan<'_>,
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
        // One object-store round trip per planned block, so fetching them one
        // at a time puts the store's latency on the query's critical path once
        // per block. A range query behind a Grafana graph panel plans as many
        // blocks as its window covers, and every one of them was serial.
        // Blocks are merged into `samples` in planned order once a batch
        // lands, so the batching only changes when the reads happen, not what
        // they add up to.
        for block_batch in plan.blocks.chunks(cold.block_fetch_concurrency.get()) {
            let results = futures_util::future::join_all(block_batch.iter().map(|block| {
                let store = Arc::clone(&cold.store);
                async move {
                    let result = collect_object_store_metric_log_batches(
                        store,
                        cold.prefix,
                        block,
                        plan,
                        query,
                        eval_range,
                    )
                    .await;
                    (block, result)
                }
            }))
            .await;

            for (block, result) in results {
                let Ok(batches) = result else {
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
