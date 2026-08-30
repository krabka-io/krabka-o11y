use super::*;

pub(crate) fn default_metric_range_step(time_range: TimeRange) -> i64 {
    time_range.end_ns.saturating_sub(time_range.start_ns).max(1)
}
