use super::{
    Arc, CompactionIndexManifest, CompactionManifestError, ObjectMeta, ObjectStore, ObjectStoreExt,
};

/// Reads the `.index` manifests at `objects`, in their order.
///
/// Each manifest must name the key it was read from. A manifest found under
/// another key describes another block, and a caller that deleted or merged
/// what it names could lose a live block, so the read fails instead.
pub(super) async fn read_compaction_manifests(
    store: &Arc<dyn ObjectStore>,
    objects: Vec<ObjectMeta>,
) -> Result<Vec<CompactionIndexManifest>, CompactionManifestError> {
    let mut manifests = Vec::with_capacity(objects.len());
    for object in objects {
        let key = object.location.as_ref();
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
