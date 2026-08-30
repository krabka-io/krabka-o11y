use super::*;

pub(crate) fn read_log_block_from_reader(
    reader: impl datafusion::parquet::file::reader::ChunkReader + 'static,
) -> Result<Vec<LogRow>, BlockStoreError> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(reader)?.build()?;
    let mut rows = Vec::new();
    for batch in reader {
        rows.extend(batch_to_rows(&batch?)?);
    }
    Ok(rows)
}
