/// Prometheus 3.14 start-time resets distinguish deltas from unknown starts.
pub(crate) fn start_timestamp_reset(
    previous_start: i64,
    previous_time: i64,
    start: i64,
    time: i64,
) -> bool {
    start != 0
        && start < time
        && (start > previous_time
            || start == previous_time && previous_start != 0 && previous_start < previous_time)
}

#[cfg(test)]
mod tests {
    use super::start_timestamp_reset;
    #[test]
    fn distinguishes_delta_unknown_invalid_and_overlapping_starts() {
        assert2::assert!(start_timestamp_reset(50, 100, 100, 200));
        for previous in [0, 100, 150] {
            assert2::assert!(!start_timestamp_reset(previous, 100, 100, 200));
        }
        assert2::assert!(start_timestamp_reset(0, 100, 150, 200));
        for start in [0, 50, 200, 250] {
            assert2::assert!(!start_timestamp_reset(50, 100, start, 200));
        }
    }
}
