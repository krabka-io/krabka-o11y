use super::{Arc, ArrowWriter, BlockStoreError, Cursor, LogRow, log_block_schema, rows_to_batch};

pub(crate) fn encode_log_block(rows: &[LogRow]) -> Result<Vec<u8>, BlockStoreError> {
    let schema = log_block_schema();
    let batch = rows_to_batch(rows, Arc::clone(&schema))?;
    let mut writer = ArrowWriter::try_new(Cursor::new(Vec::new()), schema, None)?;
    writer.write(&batch)?;
    Ok(writer.into_inner()?.into_inner())
}
