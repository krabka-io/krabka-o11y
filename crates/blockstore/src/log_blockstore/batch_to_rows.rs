use super::*;

pub(crate) fn batch_to_rows(batch: &RecordBatch) -> Result<Vec<LogRow>, BlockStoreError> {
    let fingerprints = batch
        .column_by_name("series_fingerprint")
        .and_then(|array| array.as_any().downcast_ref::<UInt64Array>())
        .ok_or(BlockStoreError::InvalidBlockColumn {
            column: "series_fingerprint",
            expected: "UInt64",
        })?;
    let timestamps = batch
        .column_by_name("timestamp_ns")
        .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
        .ok_or(BlockStoreError::InvalidBlockColumn {
            column: "timestamp_ns",
            expected: "Int64",
        })?;
    let lines = batch
        .column_by_name("line")
        .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        .ok_or(BlockStoreError::InvalidBlockColumn {
            column: "line",
            expected: "Utf8",
        })?;
    let metadata = batch
        .column_by_name("structured_metadata")
        .and_then(|array| array.as_any().downcast_ref::<MapArray>())
        .ok_or(BlockStoreError::InvalidBlockColumn {
            column: "structured_metadata",
            expected: "Map<Utf8, Utf8>",
        })?;

    (0..batch.num_rows())
        .map(|row| {
            Ok(LogRow::new(
                fingerprints.value(row),
                timestamps.value(row),
                lines.value(row),
                structured_metadata_value(metadata, row)?,
            ))
        })
        .collect()
}
