use super::*;

/// Default query-range step when the request supplies none.
///
/// This aims for about 100 buckets over the range, rounded up to a whole
/// second, with a 1s floor. It mirrors Tempo's `DefaultQueryRangeStep` closely
/// enough for a usable series.
pub(crate) fn default_query_range_step_ns(start_ns: UnixNano, end_ns: UnixNano) -> i64 {
    const SECOND_NS: i64 = 1_000_000_000;
    let delta = end_ns.0.saturating_sub(start_ns.0).max(0);
    let raw = delta / 100;
    let rounded = raw.saturating_add(SECOND_NS - 1) / SECOND_NS * SECOND_NS;
    rounded.max(SECOND_NS)
}
