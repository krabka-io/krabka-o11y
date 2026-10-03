use super::{
    Arc, CompactionIndexManifest, CompactionManifestError, ObjectStore,
    list_compaction_index_objects, read_compaction_manifests,
};

/// Reads every live `.index` manifest under the metrics prefix, in key order.
///
/// A live manifest is one that queries read. The manifests of a TSDB import
/// that is not published yet are not live, and this function does not fetch
/// them. See [`list_compaction_index`](super::list_compaction_index) for the
/// listing and the checks.
///
/// # Errors
/// Returns the errors of [`list_compaction_index`](super::list_compaction_index).
pub async fn list_compaction_manifests(
    store: &Arc<dyn ObjectStore>,
) -> Result<Vec<CompactionIndexManifest>, CompactionManifestError> {
    let listing = list_compaction_index_objects(store).await?;
    read_compaction_manifests(store, listing.live).await
}
