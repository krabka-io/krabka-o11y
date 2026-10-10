use super::{
    ActiveLogDeleteFilter, BTreeMap, LabelIndex, MetricQuery, MetricSamples, MetricWindow,
    QueryError, StreamPlan, append_matching_metric_row, for_each_query_row,
};

pub(crate) fn metric_samples_from_batches(
    batches: &[datafusion::arrow::record_batch::RecordBatch],
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    eval_times: &[i64],
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<MetricSamples, QueryError> {
    let mut samples: MetricSamples = BTreeMap::new();

    for_each_query_row(batches, &plan.fingerprints, |row| {
        append_matching_metric_row(
            &mut samples,
            plan,
            label_index,
            row,
            MetricWindow {
                query,
                eval_times,
                range_ns: query.range_ns.0,
                delete_filters,
            },
        )
    })?;

    Ok(samples)
}
