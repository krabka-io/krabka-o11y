use std::borrow::Borrow;

use super::{
    Arc, BTreeMap, ColdBlockScan, HotTailMetricSamples, MetricRangeScanRequest,
    PreparedMetricRangeScan, QueryError, Value, collect_object_store_metric_log_batches,
    loki_matrix_response_with_warnings, merge_metric_samples, metric_samples_from_batches,
};
use crate::{BlockDescriptor, RecordBatch, WalLogRecord};

struct MetricBlockCollectionResult<'a> {
    block: &'a BlockDescriptor,
    result: Result<Vec<RecordBatch>, QueryError>,
}

pub(crate) async fn execute_metric_query_range_from_object_store_with_hot_tail_frontier_and_deletes<
    R: Borrow<WalLogRecord> + Sync,
>(
    cold: ColdBlockScan<'_>,
    request: MetricRangeScanRequest<'_, R>,
) -> Result<Value, QueryError> {
    let PreparedMetricRangeScan {
        plan,
        query,
        label_index,
        eval_range,
        eval_times,
        hot_tail,
    } = request.prepare()?;
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
                    MetricBlockCollectionResult { block, result }
                }
            }))
            .await;

            for result in results {
                let block = result.block;
                let Ok(batches) = result.result else {
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

    let series = HotTailMetricSamples {
        plan,
        query,
        eval_times: &eval_times,
        hot_tail,
    }
    .merge_into(samples)?;

    Ok(loki_matrix_response_with_warnings(series, &warnings))
}
