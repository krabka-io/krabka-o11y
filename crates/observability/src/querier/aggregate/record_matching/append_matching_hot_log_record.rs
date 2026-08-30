use super::*;

pub(crate) fn append_matching_hot_log_record(
    streams: &mut BTreeMap<Labels, Vec<[String; 2]>>,
    plan: &StreamPlan,
    record: &WalLogRecord,
    frontier: &CompactionFrontier,
    delete_filters: &[ActiveLogDeleteFilter],
) {
    if record.tenant != plan.tenant
        || frontier.is_compacted(record)
        || record.timestamp_ns < plan.time_range.start_ns
        || record.timestamp_ns > plan.time_range.end_ns
    {
        return;
    }

    if is_deleted_log_entry(
        delete_filters,
        &record.labels,
        &record.line,
        &record.structured_metadata,
        record.timestamp_ns,
    ) {
        return;
    }

    if let Some((stream_labels, current_line)) = matching_loki_stream_entry(
        &plan.query,
        &record.labels,
        &record.line,
        &record.structured_metadata,
        record.timestamp_ns,
    ) {
        streams
            .entry(stream_labels)
            .or_default()
            .push([record.timestamp_ns.to_string(), current_line]);
    }
}
