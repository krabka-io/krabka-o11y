use super::*;

/// Brute-force oracle: the records a full linear scan keeps for an inclusive window.
pub(crate) fn brute_force_in_range(
    records: &[WalLogRecord],
    start_ns: i64,
    end_ns: i64,
) -> Vec<WalLogRecord> {
    records
        .iter()
        .filter(|record| record.timestamp_ns >= start_ns && record.timestamp_ns <= end_ns)
        .cloned()
        .collect()
}
