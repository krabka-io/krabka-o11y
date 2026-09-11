use super::{Limits, Time, TimeExt, TimeRange};

/// Moves a query's start forward to the oldest instant `max_query_lookback`
/// allows.
///
/// `Loki` clamps rather than refuses: "the request will not fail, but will be
/// modified to only query data within the allowed time range". A query that
/// starts before the boundary is answered from the boundary forward.
///
/// Krabka diverges in one case. When the whole window is older than the
/// boundary, `Loki` skips the query and answers with an empty body, and Krabka
/// instead runs the query over the single instant at the boundary. The answer
/// is empty for any data older than the boundary, which is every case this
/// branch exists for, and the response keeps the shape the engine gives every
/// other query.
pub(crate) fn clamp_query_lookback(limits: &Limits, range: TimeRange, now_ns: i64) -> TimeRange {
    if limits.max_query_lookback <= Time::ZERO {
        return range;
    }
    let oldest_ns = now_ns.saturating_sub(limits.max_query_lookback.nanos_i64());
    if range.start_ns >= oldest_ns {
        return range;
    }
    let end_ns = range.end_ns.max(oldest_ns);
    TimeRange::new(oldest_ns, end_ns).unwrap_or(range)
}
