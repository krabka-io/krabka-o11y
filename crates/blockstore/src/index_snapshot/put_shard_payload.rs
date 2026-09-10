use super::{
    Arc, IndexShardRange, ObjectStore, ObjectStoreExt as _, Path, PutPayload, Result,
    shard_payload_object_key,
};

/// Writes one shard payload under the key its content hash names.
///
/// The key is a function of the bytes, so the write is idempotent and can
/// never overwrite a payload some other generation still names: encoding the
/// same shard twice puts the same bytes at the same key, and encoding a
/// different shard puts different bytes at a different key. `content` is the
/// hash the caller already computed to decide whether the write was needed at
/// all.
pub(crate) async fn put_shard_payload(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    tenant: &str,
    range: IndexShardRange,
    content: &str,
    bytes: Vec<u8>,
) -> Result<()> {
    let object_key = shard_payload_object_key(key, tenant, range, content);
    store
        .put(&Path::from(object_key), PutPayload::from(bytes))
        .await?;
    Ok(())
}
