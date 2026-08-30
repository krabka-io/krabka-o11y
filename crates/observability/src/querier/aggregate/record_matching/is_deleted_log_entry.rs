use super::{ActiveLogDeleteFilter, Labels};

pub(crate) fn is_deleted_log_entry(
    delete_filters: &[ActiveLogDeleteFilter],
    labels: &Labels,
    line: &str,
    structured_metadata: &Labels,
    timestamp_ns: i64,
) -> bool {
    delete_filters.iter().any(|filter| {
        timestamp_ns >= filter.time_range.start_ns
            && timestamp_ns <= filter.time_range.end_ns
            && filter
                .query
                .matches_with_fields(labels, line, structured_metadata)
    })
}
