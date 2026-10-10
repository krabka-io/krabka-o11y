use super::{Arc, BlockStoreError, ByteSize, ByteSizeExt as _, Bytes, ObjectStore, Path, Result};
use crate::index::capped_read_error;

/// Reads one shard payload, refusing to buffer more than `max_bytes` of it.
///
/// The cap is per payload rather than per index, which is the tighter bound of
/// the two: a corrupt or hostile object under a shared prefix can no longer be
/// as large as a whole fleet index and still be read.
pub(crate) async fn read_shard_payload(
    store: &Arc<dyn ObjectStore>,
    object_key: &str,
    max_bytes: ByteSize,
) -> Result<Bytes> {
    let path = Path::from(object_key);
    match krabka_object_store::v013::read_capped(store, &path, max_bytes.bytes_u64()).await {
        Ok(bytes) => Ok(bytes),
        Err(error) => Err(match error {
            krabka_object_store::v013::ObjectStoreError::TooLarge {
                size, max_bytes, ..
            } => BlockStoreError::InvalidBlock(format!(
                "index shard payload `{object_key}` is {size} bytes, exceeds cap of {max_bytes} bytes"
            )),
            other => capped_read_error(store, &path, other).await,
        }),
    }
}
