use super::{BlockStoreError, LogRow, ParquetRecordBatchReaderBuilder, batch_to_rows};

pub(crate) fn read_log_block_from_reader(
    reader: impl datafusion::parquet::file::reader::ChunkReader + 'static,
) -> Result<Vec<LogRow>, BlockStoreError> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(reader)?;
    crate::validate_persisted_block_format(builder.metadata())
        .map_err(BlockStoreError::UnsupportedBlockFormat)?;
    let reader = builder.build()?;
    let mut rows = Vec::new();
    for batch in reader {
        rows.extend(batch_to_rows(&batch?)?);
    }
    Ok(rows)
}
