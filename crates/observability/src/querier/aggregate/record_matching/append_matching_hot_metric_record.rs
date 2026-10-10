use super::{
    CompactionFrontier, MatchedEntry, MetricSamples, MetricWindow, QueryError, StreamPlan,
    WalLogRecord, is_deleted_log_entry, matching_loki_metric_sample, record_matching_metric_sample,
};

pub(crate) fn append_matching_hot_metric_record(
    samples: &mut MetricSamples,
    plan: &StreamPlan,
    record: &WalLogRecord,
    frontier: &CompactionFrontier,
    window: MetricWindow<'_>,
) -> Result<(), QueryError> {
    let MetricWindow {
        query,
        delete_filters,
        ..
    } = window;
    if record.tenant != plan.tenant || frontier.is_compacted(record) {
        return Ok(());
    }

    if is_deleted_log_entry(
        delete_filters,
        &record.labels,
        &record.line,
        &record.structured_metadata,
        record.timestamp_ns,
    ) {
        return Ok(());
    }

    if let Some((metric_labels, line, unwrap_sample)) = matching_loki_metric_sample(
        query,
        &record.labels,
        &record.line,
        &record.structured_metadata,
        record.timestamp_ns,
    )? {
        record_matching_metric_sample(
            samples,
            window,
            MatchedEntry {
                timestamp_ns: record.timestamp_ns,
                metric_labels,
                line,
                unwrap_sample,
            },
        );
    }
    Ok(())
}
