use super::{StreamPlan, TimeRange, line_filter_sql_predicates};

/// The SQL one planned block scan runs.
///
/// # Why there is no `LIMIT`
///
/// A Loki stream query carries a `limit`, and [`StreamScanOptions`] holds it,
/// but it stops the block loop between chunks and never bounds what a single
/// block returns. Pushing it into this statement would be wrong twice over,
/// and neither problem is fixable from here:
///
/// - **The scan is not the whole filter.** Only the leading line filters reach
///   SQL. Label filters, parser stages, `ip(...)` matchers and delete filters
///   are applied row by row afterwards, by `matching_loki_stream_entry` and
///   `is_deleted_log_entry`. A `LIMIT n` truncates before any of them run, so
///   a block whose first `n` surviving rows are all dropped in Rust yields
///   nothing, while the rows that would have matched were discarded unread.
///   That is a wrong answer, not a smaller one.
/// - **The scan's order is not the answer's order.** `apply_loki_stream_limit`
///   fills the response stream by stream in label order, giving each stream
///   everything that is left of the budget. Labels are not fingerprints and
///   are not timestamps, so no `ORDER BY` this statement can carry makes the
///   first `n` rows of a block a superset of the rows that truncation keeps.
///   Take the newest `n` of a block and a stream that sorts early loses its
///   entries to a busier stream that sorts late.
///
/// Both conditions do lift for one shape -- a single planned fingerprint, a
/// pipeline of nothing but pushed line filters, no delete filters and no
/// `end_exclusive` -- and only for that shape. It is not the shape a Grafana
/// logs panel sends, so the pushdown stays unwritten rather than sitting
/// behind a guard that is wrong the moment any one of those four stops
/// holding.
///
/// [`StreamScanOptions`]: super::StreamScanOptions
pub(crate) fn stream_plan_scan_sql_for_time_range(
    plan: &StreamPlan,
    time_range: TimeRange,
) -> String {
    let mut predicates = vec![format!(
        "timestamp_ns >= {} and timestamp_ns <= {}",
        time_range.start_ns, time_range.end_ns
    )];
    if !plan.fingerprints.is_empty() {
        let fingerprints = plan
            .fingerprints
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        predicates.push(format!("series_fingerprint in ({fingerprints})"));
    }
    predicates.extend(line_filter_sql_predicates(&plan.query.pipeline));
    format!(
        "select series_fingerprint, timestamp_ns, line, structured_metadata \
         from logs \
         where {} \
         order by series_fingerprint, timestamp_ns",
        predicates.join(" and ")
    )
}
