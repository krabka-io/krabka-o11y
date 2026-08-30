use super::{Arc, BlockMeta, ObjectStore, ProfileIndex, ProfilesError, compact_blocks_with_policy};

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub async fn compact_blocks(
    store: &Arc<dyn ObjectStore>,
    index: &mut ProfileIndex,
    tenant: &str,
    input_keys: &[String],
    output_key: &str,
) -> Result<BlockMeta, ProfilesError> {
    compact_blocks_with_policy(store, index, tenant, input_keys, output_key, None).await
}
