use super::*;

pub(crate) fn eligible_tail_record_count(records: &[WalLogRecord], delay_for: i64) -> usize {
    if delay_for <= 0 {
        return records.len();
    }

    let cutoff = current_unix_time_ns().saturating_sub(delay_for);
    records
        .iter()
        .take_while(|record| record.timestamp_ns <= cutoff)
        .count()
}
