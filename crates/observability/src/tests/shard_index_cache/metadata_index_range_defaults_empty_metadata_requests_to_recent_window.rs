use super::*;

#[test]
pub(crate) fn metadata_index_range_defaults_empty_metadata_requests_to_recent_window() {
    const SIX_HOURS_NS: i64 = 6 * 60 * 60 * 1_000_000_000;
    let before = current_unix_time_ns();
    let range = metadata_index_range(&SeriesParams::default()).unwrap();
    let after = current_unix_time_ns();

    check!(
        range.start_ns >= before - SIX_HOURS_NS,
        "default metadata index start should be within Loki's default recent window"
    );
    check!(
        range.end_ns <= after,
        "default metadata index end should be now-ish, got {} after {}",
        range.end_ns,
        after
    );
    check!(
        range.end_ns - range.start_ns <= SIX_HOURS_NS,
        "default metadata index range should not be all time"
    );
}
