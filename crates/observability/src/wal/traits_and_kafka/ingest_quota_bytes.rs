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
                        .map(|(name, value)| name.len() + value.len())
                        .sum::<usize>()
                    + record
                        .structured_metadata
                        .iter()
                        .map(|(name, value)| name.len() + value.len())
                        .sum::<usize>()
            })
            .sum(),
    )
}
