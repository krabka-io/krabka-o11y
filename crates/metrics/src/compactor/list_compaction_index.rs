use super::{
    Arc, CompactionIndex, CompactionManifestError, ObjectStore, list_compaction_index_objects,
    read_compaction_manifests,
};

/// Reads every `.index` manifest and publication marker under the metrics
/// prefix, in key order.
///
/// Metrics holds no index in memory. The manifests beside the blocks are the
/// index, so a caller that wants to know which blocks are live reads them all.
/// The whole prefix is listed rather than one prefix per tenant: a tenant with
/// no entry in the overrides file still has blocks, and a per-tenant listing
/// would never reach it.
///
/// Each manifest must name the key it was read from. A manifest found under
/// another key describes another block, and a caller that deleted or merged
/// what it names could lose a live block, so the listing fails instead.
///
/// # Errors
/// Returns [`CompactionManifestError::ObjectStore`] when the prefix cannot be
/// listed or a manifest cannot be fetched, [`CompactionManifestError::Index`]
/// when a manifest cannot be decoded, and
/// [`CompactionManifestError::KeyMismatch`] when a manifest does not name the
/// key it was read from.
pub async fn list_compaction_index(
    store: &Arc<dyn ObjectStore>,
) -> Result<CompactionIndex, CompactionManifestError> {
    let listing = list_compaction_index_objects(store).await?;
    Ok(CompactionIndex {
        live: read_compaction_manifests(store, listing.live, None).await?,
        pending: read_compaction_manifests(store, listing.pending, None).await?,
        markers: listing
            .markers
            .into_iter()
            .map(|object| object.location.to_string())
            .collect(),
    })
}
