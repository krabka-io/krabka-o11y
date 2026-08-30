use super::*;

pub(crate) fn metric_binary_integer_timestamp_ns_candidates(timestamp: i64) -> Vec<i64> {
    let mut candidates = vec![timestamp];
    if let Some(seconds_timestamp) = timestamp.checked_mul(1_000_000_000) {
        candidates.push(seconds_timestamp);
    }
    candidates.sort_unstable();
    candidates.dedup();
    candidates
}
