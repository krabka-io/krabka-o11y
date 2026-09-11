use super::*;

/// `max_query_lookback` clamps rather than refuses, which is what `Loki`
/// does. A query that starts before the boundary is answered from the
/// boundary forward, and a query that starts after it is left alone.
///
/// The end is never moved backwards. A clamp that moved the end would turn
/// a legitimate query into an empty one, and the caller would see a
/// successful response with no rows and no reason.
#[test]
pub(crate) fn the_lookback_clamp_moves_the_start_and_leaves_the_end() {
    let now_ns = 1_000_000_000_000_000_i64;
    let hour_ns = hours(1).nanos_i64();
    let limits = |max_query_lookback| Limits {
        max_query_lookback,
        ..Limits::unenforced()
    };
    let range = |start_ns, end_ns| TimeRange::new(start_ns, end_ns).expect("a valid range");

    // Off: the range is handed back untouched.
    let asked = range(0, now_ns);
    check!(clamp_query_lookback(&limits(Time::ZERO), asked, now_ns) == asked);

    // Wholly inside the window: untouched.
    let inside = range(now_ns - hour_ns / 2, now_ns);
    check!(clamp_query_lookback(&limits(hours(1)), inside, now_ns) == inside);

    // Exactly at the boundary: untouched, because the boundary instant is
    // itself within the lookback.
    let boundary = range(now_ns - hour_ns, now_ns);
    check!(clamp_query_lookback(&limits(hours(1)), boundary, now_ns) == boundary);

    // Starting before the boundary: the start moves, the end does not.
    let clamped = clamp_query_lookback(&limits(hours(1)), range(0, now_ns), now_ns);
    check!(clamped.start_ns == now_ns - hour_ns);
    check!(clamped.end_ns == now_ns, "the end is never moved");

    // Wholly before the boundary: the whole window collapses onto the
    // boundary instant, so the query reads nothing the lookback forbids.
    let outside = clamp_query_lookback(&limits(hours(1)), range(0, now_ns - 2 * hour_ns), now_ns);
    check!(outside.start_ns == now_ns - hour_ns);
    check!(outside.end_ns == now_ns - hour_ns);
}
