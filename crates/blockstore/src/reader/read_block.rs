use super::{
    Arc, DEFAULT_BLOCK_READ_MAX, ObjectStore, RecordBatch, Result, read_block_with_max_bytes,
};

/// Reads every `RecordBatch` from the Parquet block at `object_key`.
///
/// The reader rejects the block with an error when its on-disk size exceeds
/// [`DEFAULT_BLOCK_READ_MAX`], before it streams any bytes.
///
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn read_block(store: Arc<dyn ObjectStore>, object_key: &str) -> Result<Vec<RecordBatch>> {
    read_block_with_max_bytes(store, object_key, DEFAULT_BLOCK_READ_MAX).await
}
