use super::{
    Arc, CompactionPass, CompactionPolicy, ObjectStore, ProfileIndex, ProfilesError,
    compact_once_with_policy,
};

/// Runs one compaction pass over the whole index, without downsampling.
///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub async fn compact_once(
    store: &Arc<dyn ObjectStore>,
    index: &mut ProfileIndex,
    policy: CompactionPolicy,
) -> Result<CompactionPass, ProfilesError> {
    compact_once_with_policy(store, index, policy, None).await
}
