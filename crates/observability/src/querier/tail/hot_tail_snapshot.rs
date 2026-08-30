use super::*;

/// Snapshots the hot-tail records that overlap `time_range`, plus the
/// compaction frontier.
///
/// `time_range` must be the planned scan range (`plan.time_range`). Every
/// hot-tail query path re-applies the exact per-record time bound downstream,
/// and that bound is always inside the plan's scan range, which is the stream
/// query range, or [`metric_scan_range`] for metric queries. A prune to the
/// plan range drops only records the downstream filter would reject
/// anyway. Results are identical to a full-buffer scan, and a narrow window
/// avoids a touch of the whole retained buffer.
pub(crate) fn hot_tail_snapshot(
    state: &QuerierState,
    time_range: TimeRange,
) -> (Vec<WalLogRecord>, CompactionFrontier) {
    state.hot_tail.as_ref().map_or(
        (Vec::new(), CompactionFrontier::new(i64::MAX)),
        |hot_tail| {
            (
                hot_tail
                    .source
                    .records_in_range(time_range.start_ns, time_range.end_ns),
                hot_tail.frontier.snapshot(),
            )
        },
    )
}
