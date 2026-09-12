use super::{
    Arc, COMPACTION_OBJECT_PREFIX, CompactionIndexManifest, CompactionManifestError, ObjectStore,
    ObjectStoreExt, Path, TryStreamExt,
};

/// Reads every `.index` manifest under the metrics prefix, in key order.
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
pub async fn list_compaction_manifests(
    store: &Arc<dyn ObjectStore>,
) -> Result<Vec<CompactionIndexManifest>, CompactionManifestError> {
    let mut objects = store
        .list(Some(&Path::from(COMPACTION_OBJECT_PREFIX)))
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| CompactionManifestError::ObjectStore(error.to_string()))?;
    objects.sort_by(|left, right| left.location.cmp(&right.location));

    let mut manifests = Vec::new();
    for object in objects {
        let key = object.location.as_ref();
        if !std::path::Path::new(key)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("index"))
        {
            continue;
        }
        let bytes = store
            .get(&object.location)
            .await
            .map_err(|error| CompactionManifestError::ObjectStore(error.to_string()))?
            .bytes()
            .await
            .map_err(|error| CompactionManifestError::ObjectStore(error.to_string()))?;
        let manifest = CompactionIndexManifest::decode(&bytes)?;
        if manifest.index_key != key {
            return Err(CompactionManifestError::KeyMismatch {
                listed: key.to_string(),
                manifest: manifest.index_key,
            });
        }
        manifests.push(manifest);
    }
    Ok(manifests)
}
