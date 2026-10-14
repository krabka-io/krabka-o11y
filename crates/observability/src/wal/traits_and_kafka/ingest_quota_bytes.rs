use super::{ByteSize, WalLogRecord, measured_size};

pub(crate) fn ingest_quota_bytes(records: &[WalLogRecord]) -> ByteSize {
    measured_size(
        records
            .iter()
            .map(|record| {
                record.tenant.len()
                    + record.line.len()
                    + std::mem::size_of_val(&record.timestamp_ns)
                    + record
                        .labels
                        .iter()
                        .map(|entry| entry.0.len() + entry.1.len())
                        .sum::<usize>()
                    + record
                        .structured_metadata
                        .iter()
                        .map(|entry| entry.0.len() + entry.1.len())
                        .sum::<usize>()
            })
            .sum(),
    )
}
