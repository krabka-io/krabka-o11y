use super::*;

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub async fn compact_once(
    store: &Arc<dyn ObjectStore>,
    index: &mut ProfileIndex,
    max_blocks_per_job: usize,
) -> Result<Vec<BlockMeta>, ProfilesError> {
    compact_once_with_policy(store, index, max_blocks_per_job, None).await
}
