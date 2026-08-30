use super::*;

pub(crate) fn wal_record_time_range(
    records: &[WalLogRecord],
) -> Result<TimeRange, CompactionError> {
    let first = records.first().ok_or(CompactionError::EmptyWalBatch)?;
    let mut start_ns = first.timestamp_ns;
    let mut end_ns = first.timestamp_ns;
    for record in records.iter().skip(1) {
        start_ns = start_ns.min(record.timestamp_ns);
        end_ns = end_ns.max(record.timestamp_ns);
    }
    Ok(TimeRange::new(start_ns, end_ns)?)
}
