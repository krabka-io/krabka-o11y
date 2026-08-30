use super::{MetricQuery, QueryError, TimeRange};

pub(crate) fn metric_scan_range(
    query: &MetricQuery,
    eval_range: TimeRange,
) -> Result<TimeRange, QueryError> {
    let scan_end_ns = eval_range.end_ns.saturating_sub(query.offset_ns.0);
    let scan_start_ns = eval_range
        .start_ns
        .saturating_sub(query.offset_ns.0)
        .saturating_sub(query.range_ns.0);
    Ok(TimeRange::new(scan_start_ns, scan_end_ns)?)
}
