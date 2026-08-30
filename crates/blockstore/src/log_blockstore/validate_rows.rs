use super::{BlockKey, BlockStoreError, LogRow};

pub(crate) fn validate_rows(key: &BlockKey, rows: &[LogRow]) -> Result<(), BlockStoreError> {
    if let Some(row) = rows.iter().find(|row| {
        row.timestamp_ns < key.time_range.start_ns || row.timestamp_ns > key.time_range.end_ns
    }) {
        return Err(BlockStoreError::RowOutsideBlockTimeRange {
            timestamp_ns: row.timestamp_ns,
            start_ns: key.time_range.start_ns,
            end_ns: key.time_range.end_ns,
        });
    }
    Ok(())
}
