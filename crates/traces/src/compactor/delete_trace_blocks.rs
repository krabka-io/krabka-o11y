use super::{Arc, BlockDeletion, BlockDeletionReport, ObjectStore, delete_blocks};

/// Deletes span block objects that the index no longer names.
///
/// A traces block is one object. The profiles path writes a `.symdb` beside
/// each block and the metrics path writes a `.index`, and each of those has to
/// be deleted with its block. Traces writes neither, so every deletion here
/// carries an empty [`BlockDeletion::sidecars`].
///
/// Call this only after the index that dropped the blocks is durable. One
/// object that will not delete does not stop the rest; the report names it.
pub async fn delete_trace_blocks(
    store: &Arc<dyn ObjectStore>,
    object_keys: &[String],
) -> BlockDeletionReport {
    let deletions: Vec<BlockDeletion> = object_keys
        .iter()
        .map(|object_key| BlockDeletion {
            object_key: object_key.clone(),
            sidecars: Vec::new(),
        })
        .collect();
    delete_blocks(store, &deletions).await
}
