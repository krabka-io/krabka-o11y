use std::{borrow::Borrow, path::Path};

use datafusion::{arrow::record_batch::RecordBatch, prelude::SessionContext};

use crate::{
    Arc, BTreeMap, BlockDescriptor, HotTailMetricSamples, LabelIndex, MetricQuery, ObjectPath,
    ObjectStore, QueryError, QueryHotTail, StreamPlan, TimeRange, Value, WalLogRecord,
    checked_eval_times, collect_object_store_metric_log_batches,
    loki_matrix_response_with_warnings, merge_metric_samples, metric_samples_from_batches,
    register_log_blocks, register_log_blocks_from_object_store,
};

pub(crate) enum MetricRangeScanSource<'a> {
    Local(&'a Path),
    ObjectStore(object_store_scans::ColdBlockScan<'a>),
}

struct MetricBlockCollectionResult<'a> {
    block: &'a BlockDescriptor,
    result: Result<Vec<RecordBatch>, QueryError>,
}

pub(crate) async fn execute_metric_range_scan_with_deletes<R>(
    source: MetricRangeScanSource<'_>,
    request: MetricRangeScanRequest<'_, R>,
) -> Result<Value, QueryError>
where
    R: Borrow<WalLogRecord> + Sync,
{
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
        match source {
            MetricRangeScanSource::Local(root) => {
                let ctx = registered_local_log_session(root, &plan.blocks)?;
                let sql = stream_scans::metric_plan_scan_sql(plan, query, eval_range)?;
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
            MetricRangeScanSource::ObjectStore(cold) => {
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
                            warnings
                                .push(format!("failed to read block {}", block.key.object_key()));
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

pub(crate) async fn collect_object_store_log_batches(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    block: &BlockDescriptor,
    sql: &str,
) -> Result<Vec<RecordBatch>, QueryError> {
    let ctx = SessionContext::new();
    register_log_blocks_from_object_store(
        &ctx,
        "logs",
        store,
        prefix,
        std::slice::from_ref(block),
    )?;
    Ok(ctx.sql(sql).await?.collect().await?)
}

pub(crate) fn registered_local_log_session(
    root: impl AsRef<Path>,
    blocks: &[BlockDescriptor],
) -> Result<SessionContext, QueryError> {
    let ctx = SessionContext::new();
    register_log_blocks(&ctx, "logs", root, blocks)?;
    Ok(ctx)
}

pub(crate) struct MetricRangeScanRequest<'a, R> {
    pub(crate) plan: &'a StreamPlan,
    pub(crate) query: &'a MetricQuery,
    pub(crate) label_index: &'a LabelIndex,
    pub(crate) evaluation: MetricEvaluationRange,
    pub(crate) hot_tail: QueryHotTail<'a, R>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetricEvaluationRange {
    pub range: TimeRange,
    pub step_ns: i64,
}

impl MetricEvaluationRange {
    #[must_use]
    pub const fn new(range: TimeRange, step_ns: i64) -> Self {
        Self { range, step_ns }
    }
}

pub(crate) struct PreparedMetricRangeScan<'a, R> {
    pub(crate) plan: &'a StreamPlan,
    pub(crate) query: &'a MetricQuery,
    pub(crate) label_index: &'a LabelIndex,
    pub(crate) eval_range: TimeRange,
    pub(crate) eval_times: Vec<i64>,
    pub(crate) hot_tail: QueryHotTail<'a, R>,
}

struct MetricEvaluationTimes {
    range: TimeRange,
    times: Vec<i64>,
}

impl<'a, R> MetricRangeScanRequest<'a, R> {
    pub(crate) fn new(
        plan: &'a StreamPlan,
        query: &'a MetricQuery,
        label_index: &'a LabelIndex,
        evaluation: MetricEvaluationRange,
        hot_tail: QueryHotTail<'a, R>,
    ) -> Self {
        Self {
            plan,
            query,
            label_index,
            evaluation,
            hot_tail,
        }
    }

    pub(crate) fn prepare(self) -> Result<PreparedMetricRangeScan<'a, R>, QueryError> {
        let Self {
            plan,
            query,
            label_index,
            evaluation,
            hot_tail,
        } = self;
        let evaluation_times = metric_range_scan_eval_times(evaluation)?;
        Ok(PreparedMetricRangeScan {
            plan,
            query,
            label_index,
            eval_range: evaluation_times.range,
            eval_times: evaluation_times.times,
            hot_tail,
        })
    }
}

fn metric_range_scan_eval_times(
    evaluation: MetricEvaluationRange,
) -> Result<MetricEvaluationTimes, QueryError> {
    Ok(MetricEvaluationTimes {
        range: evaluation.range,
        times: checked_eval_times(evaluation.range, evaluation.step_ns)?,
    })
}

pub(crate) mod stream_scans;
pub use stream_scans::{
    execute_stream_query, execute_stream_query_from_object_store,
    execute_stream_query_with_hot_tail, execute_stream_query_with_hot_tail_frontier,
    metric_plan_scan_sql, stream_plan_scan_sql,
};
pub(crate) mod metric_scans;
pub use metric_scans::{
    execute_metric_query, execute_metric_query_from_object_store, execute_metric_query_range,
    execute_metric_query_range_from_object_store, execute_metric_query_range_with_hot_tail,
    execute_metric_query_range_with_hot_tail_frontier, execute_metric_query_with_hot_tail,
    execute_metric_query_with_hot_tail_frontier, execute_tail_query,
    execute_tail_query_with_frontier,
};
pub(crate) mod object_store_scans;
