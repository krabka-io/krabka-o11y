use super::*;

pub(crate) async fn load_batches(
    store: &Arc<dyn ObjectStore>,
    block_key: &str,
) -> Result<Vec<RecordBatch>, ProfilesError> {
    let bytes = store
        .get(&Path::from(block_key))
        .await
        .map_err(|err| ProfilesError::Block(err.to_string()))?
        .bytes()
        .await
        .map_err(|err| ProfilesError::Block(err.to_string()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|err| ProfilesError::Block(err.to_string()))?
        .build()
        .map_err(|err| ProfilesError::Block(err.to_string()))?;
    reader
        .map(|batch| batch.map_err(|err| ProfilesError::Block(err.to_string())))
        .collect()
}
