use super::WalRecord;

pub(crate) fn wal_record_max_timestamp_ms(record: &WalRecord) -> Option<i64> {
    record
        .payload
        .timestamp_ms()
        .into_iter()
        .chain(
            record
                .exemplars
                .iter()
                .map(|exemplar| exemplar.timestamp_ms),
        )
        .max()
}
