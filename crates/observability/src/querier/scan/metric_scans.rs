use std::borrow::Borrow;

pub(crate) use super::{MetricEvaluationRange, MetricRangeScanRequest};
use super::{MetricRangeScanSource, Value, execute_metric_range_scan_with_deletes};
use crate::{
    ActiveLogDeleteFilter, Arc, BTreeMap, BlockDescriptor, ColdBlockScan, CompactionFrontier,
    FormattedMetricSeries, FsPath, LabelIndex, Labels, LokiStreamEncoding, LokiStreamEntry,
    MetricQuery, MetricSamples, MetricWindow, ObjectPath, ObjectStore, QueryError, QueryHotTail,
    RecordBatch, StreamPlan, TimeRange, WalLogRecord, append_matching_hot_log_record,
    append_matching_hot_metric_record, append_matching_log_row, apply_absent_over_time,
    apply_distinct_to_streams, default_block_fetch_concurrency,
    execute_metric_query_range_from_object_store_with_hot_tail_frontier,
    execute_metric_query_range_from_object_store_with_hot_tail_frontier_and_deletes,
    for_each_query_row, format_metric_samples, json, loki_stream_results, metric_plan_scan_sql,
    sort_loki_stream_values,
};

pub(crate) fn append_matching_log_batches(
    streams: &mut BTreeMap<Labels, Vec<LokiStreamEntry>>,
    plan: &StreamPlan,
    label_index: &LabelIndex,
    batches: &[RecordBatch],
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<(), QueryError> {
    for_each_query_row(batches, &plan.fingerprints, |row| {
        append_matching_log_row(streams, plan, label_index, row, delete_filters)
    })
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn execute_metric_query(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
) -> Result<Value, QueryError> {
    execute_metric_query_with_deletes(root, plan, query, label_index, &[]).await
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn execute_metric_query_from_object_store(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
) -> Result<Value, QueryError> {
    execute_metric_query_from_object_store_with_hot_tail_frontier(
        ColdBlockScan {
            store,
            prefix,
            block_fetch_concurrency: default_block_fetch_concurrency(),
        },
        plan,
        query,
        label_index,
        &[],
        &CompactionFrontier::new(i64::MAX),
    )
    .await
}

pub(crate) async fn execute_metric_query_from_object_store_with_hot_tail_frontier(
    cold: ColdBlockScan<'_>,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
) -> Result<Value, QueryError> {
    execute_metric_query_from_object_store_with_hot_tail_frontier_and_deletes(
        cold,
        plan,
        query,
        label_index,
        QueryHotTail {
            records: hot_tail,
            frontier,
            delete_filters: &[],
        },
    )
    .await
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn execute_metric_query_range(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    eval_range: TimeRange,
    step_ns: i64,
) -> Result<Value, QueryError> {
    execute_metric_query_range_with_deletes(
        root,
        plan,
        query,
        label_index,
        eval_range,
        step_ns,
        &[],
    )
    .await
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn execute_metric_query_range_with_hot_tail(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    evaluation: MetricEvaluationRange,
    hot_tail: &[WalLogRecord],
    compacted_through_ns: i64,
) -> Result<Value, QueryError> {
    execute_metric_query_range_with_hot_tail_frontier(
        root,
        plan,
        query,
        label_index,
        evaluation,
        hot_tail,
        &CompactionFrontier::new(compacted_through_ns),
    )
    .await
}

pub(crate) async fn execute_metric_query_with_deletes(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<Value, QueryError> {
    let eval_range = TimeRange::new(plan.time_range.end_ns, plan.time_range.end_ns)?;
    execute_metric_query_range_with_deletes(
        root,
        plan,
        query,
        label_index,
        eval_range,
        1,
        delete_filters,
    )
    .await
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn execute_metric_query_with_hot_tail(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    hot_tail: &[WalLogRecord],
    compacted_through_ns: i64,
) -> Result<Value, QueryError> {
    execute_metric_query_with_hot_tail_frontier(
        root,
        plan,
        query,
        label_index,
        hot_tail,
        &CompactionFrontier::new(compacted_through_ns),
    )
    .await
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn execute_metric_query_with_hot_tail_frontier(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
) -> Result<Value, QueryError> {
    execute_metric_query_with_hot_tail_frontier_and_deletes(
        root,
        plan,
        query,
        label_index,
        hot_tail,
        frontier,
        &[],
    )
    .await
}

#[must_use]
pub fn execute_tail_query(
    plan: &StreamPlan,
    hot_tail: &[WalLogRecord],
    compacted_through_ns: i64,
) -> Value {
    execute_tail_query_with_frontier(
        plan,
        hot_tail,
        &CompactionFrontier::new(compacted_through_ns),
    )
}

#[must_use]
pub fn execute_tail_query_with_frontier(
    plan: &StreamPlan,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
) -> Value {
    execute_tail_query_with_frontier_and_deletes(
        plan,
        hot_tail,
        frontier,
        &[],
        LokiStreamEncoding::Folded,
        false,
    )
}

pub(crate) async fn collect_object_store_metric_log_batches(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    block: &BlockDescriptor,
    plan: &StreamPlan,
    query: &MetricQuery,
    eval_range: TimeRange,
) -> Result<Vec<RecordBatch>, QueryError> {
    super::collect_object_store_log_batches(
        store,
        prefix,
        block,
        &metric_plan_scan_sql(plan, query, eval_range)?,
    )
    .await
}

pub(crate) async fn execute_metric_query_from_object_store_with_hot_tail_frontier_and_deletes<
    R: Borrow<WalLogRecord> + Sync,
>(
    cold: ColdBlockScan<'_>,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    hot_tail: QueryHotTail<'_, R>,
) -> Result<Value, QueryError> {
    let eval_range = TimeRange::new(plan.time_range.end_ns, plan.time_range.end_ns)?;
    execute_metric_query_range_from_object_store_with_hot_tail_frontier_and_deletes(
        cold,
        MetricRangeScanRequest::new(
            plan,
            query,
            label_index,
            MetricEvaluationRange::new(eval_range, 1),
            hot_tail,
        ),
    )
    .await
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn execute_metric_query_range_from_object_store(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    eval_range: TimeRange,
    step_ns: i64,
) -> Result<Value, QueryError> {
    execute_metric_query_range_from_object_store_with_hot_tail_frontier(
        ColdBlockScan {
            store,
            prefix,
            block_fetch_concurrency: default_block_fetch_concurrency(),
        },
        plan,
        query,
        label_index,
        MetricEvaluationRange::new(eval_range, step_ns),
        QueryHotTail::<crate::WalLogRecord> {
            records: &[],
            frontier: &CompactionFrontier::new(i64::MAX),
            delete_filters: &[],
        },
    )
    .await
}

pub(crate) async fn execute_metric_query_range_with_deletes(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    eval_range: TimeRange,
    step_ns: i64,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<Value, QueryError> {
    execute_metric_query_range_with_hot_tail_frontier_and_deletes(
        root,
        MetricRangeScanRequest::new(
            plan,
            query,
            label_index,
            MetricEvaluationRange::new(eval_range, step_ns),
            QueryHotTail::<crate::WalLogRecord> {
                records: &[],
                frontier: &CompactionFrontier::new(i64::MAX),
                delete_filters,
            },
        ),
    )
    .await
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn execute_metric_query_range_with_hot_tail_frontier(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    evaluation: MetricEvaluationRange,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
) -> Result<Value, QueryError> {
    execute_metric_query_range_with_hot_tail_frontier_and_deletes(
        root,
        MetricRangeScanRequest::new(
            plan,
            query,
            label_index,
            evaluation,
            QueryHotTail {
                records: hot_tail,
                frontier,
                delete_filters: &[],
            },
        ),
    )
    .await
}

pub(crate) async fn execute_metric_query_range_with_hot_tail_frontier_and_deletes<
    R: Borrow<WalLogRecord> + Sync,
>(
    root: impl AsRef<std::path::Path>,
    request: MetricRangeScanRequest<'_, R>,
) -> Result<Value, QueryError> {
    execute_metric_range_scan_with_deletes(MetricRangeScanSource::Local(root.as_ref()), request)
        .await
}

pub(crate) async fn execute_metric_query_with_hot_tail_frontier_and_deletes<
    R: Borrow<WalLogRecord> + Sync,
>(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    hot_tail: &[R],
    frontier: &CompactionFrontier,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<Value, QueryError> {
    let eval_range = TimeRange::new(plan.time_range.end_ns, plan.time_range.end_ns)?;
    execute_metric_query_range_with_hot_tail_frontier_and_deletes(
        root,
        MetricRangeScanRequest::new(
            plan,
            query,
            label_index,
            MetricEvaluationRange::new(eval_range, 1),
            QueryHotTail {
                records: hot_tail,
                frontier,
                delete_filters,
            },
        ),
    )
    .await
}

/// Builds one tail frame, in the encoding the tail request asked for.
///
/// A tail frame carries the same two encodings a query answer does, and Loki is
/// not consistent about the default one: the frame it backfills when a tail
/// opens folds an entry's structured metadata into the stream's labels, as
/// `/query_range` does, while the frames it streams afterwards drop the
/// metadata instead of folding it. `live` distinguishes those two frames.
pub(crate) fn execute_tail_query_with_frontier_and_deletes(
    plan: &StreamPlan,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
    delete_filters: &[ActiveLogDeleteFilter],
    encoding: LokiStreamEncoding,
    live: bool,
) -> Value {
    let mut streams: BTreeMap<Labels, Vec<LokiStreamEntry>> = BTreeMap::new();
    for record in hot_tail {
        append_matching_hot_log_record(&mut streams, plan, record, frontier, delete_filters, true);
    }
    if live && matches!(encoding, LokiStreamEncoding::Folded) && plan.query.pipeline.is_empty() {
        let mut live_streams: BTreeMap<Labels, Vec<LokiStreamEntry>> = BTreeMap::new();
        for entries in streams.into_values() {
            for mut entry in entries {
                entry.structured_metadata.clear();
                entry.parsed.remove("detected_level");
                let mut labels = entry.source_labels.clone();
                labels.remove("detected_level");
                labels.extend(entry.parsed.clone());
                live_streams.entry(labels).or_default().push(entry);
            }
        }
        streams = live_streams;
    }
    sort_loki_stream_values(&mut streams);
    apply_distinct_to_streams(&mut streams, &plan.query);

    json!({ "streams": loki_stream_results(streams, encoding) })
}

/// The WAL hot tail of a metric range query, and what its records are
/// matched and windowed against.
pub(crate) struct HotTailMetricSamples<'a, R> {
    pub(crate) plan: &'a StreamPlan,
    pub(crate) query: &'a MetricQuery,
    pub(crate) eval_times: &'a [i64],
    pub(crate) hot_tail: QueryHotTail<'a, R>,
}

impl<R: Borrow<WalLogRecord>> HotTailMetricSamples<'_, R> {
    /// Adds the hot tail's matching records to the blocks' `samples`, fills
    /// in what `absent_over_time` reports, and formats the series.
    pub(crate) fn merge_into(
        self,
        mut samples: MetricSamples,
    ) -> Result<FormattedMetricSeries, QueryError> {
        let Self {
            plan,
            query,
            eval_times,
            hot_tail,
        } = self;
        for record in hot_tail.records {
            let record: &WalLogRecord = record.borrow();
            append_matching_hot_metric_record(
                &mut samples,
                plan,
                record,
                hot_tail.frontier,
                MetricWindow {
                    query,
                    eval_times,
                    range_ns: query.range_ns.0,
                    delete_filters: hot_tail.delete_filters,
                },
            )?;
        }
        apply_absent_over_time(&mut samples, query, eval_times);
        Ok(format_metric_samples(samples, query))
    }
}
