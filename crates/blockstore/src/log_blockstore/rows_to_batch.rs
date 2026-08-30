use super::*;

pub(crate) fn rows_to_batch(rows: &[LogRow], schema: Arc<Schema>) -> Result<RecordBatch, BlockStoreError> {
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
