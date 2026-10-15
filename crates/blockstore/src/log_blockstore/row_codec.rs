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

pub(crate) fn rows_to_batch(
    rows: &[LogRow],
    schema: Arc<Schema>,
) -> Result<RecordBatch, BlockStoreError> {
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(
                rows.iter()
                    .map(|row| row.series_fingerprint)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.timestamp_ns).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.line.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(structured_metadata_array(rows)?),
        ],
    )?)
}

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

pub(crate) fn encode_log_block(rows: &[LogRow]) -> Result<Vec<u8>, BlockStoreError> {
    let schema = log_block_schema();
    let batch = rows_to_batch(rows, Arc::clone(&schema))?;
    let mut writer = ArrowWriter::try_new(
        Cursor::new(Vec::new()),
        schema,
        Some(crate::persisted_format::persisted_block_writer_properties()),
    )?;
    writer.write(&batch)?;
    Ok(writer.into_inner()?.into_inner())
}
