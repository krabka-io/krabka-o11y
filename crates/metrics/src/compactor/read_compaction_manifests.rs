use super::{
    Arc, BTreeSet, CompactionIndexManifest, CompactionManifestCache, CompactionManifestError,
    ObjectMeta, ObjectStore, ObjectStoreExt,
};

/// Reads the `.index` manifests at `objects`, in their order.
///
/// Each manifest must name the key it was read from. A manifest found under
/// another key describes another block, and a caller that deleted or merged
/// what it names could lose a live block, so the read fails instead.
pub(super) async fn read_compaction_manifests(
    store: &Arc<dyn ObjectStore>,
    objects: Vec<ObjectMeta>,
    cache: Option<&mut CompactionManifestCache>,
) -> Result<Vec<CompactionIndexManifest>, CompactionManifestError> {
    let mut cache = cache.filter(|cache| Arc::ptr_eq(&cache.store, store));
    if let Some(cache) = cache.as_mut() {
        let live = objects
            .iter()
            .map(|object| object.location.as_ref())
            .collect::<BTreeSet<_>>();
        cache.retain(&live);
    }
    let mut manifests = Vec::with_capacity(objects.len());
    for object in objects {
        let key = object.location.as_ref();
        let cached = cache.as_ref().and_then(|cache| cache.get(&object));
        let (bytes, fetched_meta) = if let Some(bytes) = cached {
            (bytes, None)
        } else {
            let result = store
                .get(&object.location)
                .await
                .map_err(|error| CompactionManifestError::ObjectStore(error.to_string()))?;
            let meta = cache.as_ref().map(|_| result.meta.clone());
            let bytes = result
                .bytes()
                .await
                .map_err(|error| CompactionManifestError::ObjectStore(error.to_string()))?;
            (bytes, meta)
        };
        let manifest = CompactionIndexManifest::decode(&bytes)?;
        if manifest.index_key != key {
            return Err(CompactionManifestError::KeyMismatch {
                listed: key.to_string(),
                manifest: manifest.index_key,
            });
        }
        if let Some(cache) = cache.as_mut()
            && let Some(meta) = fetched_meta
        {
            cache.insert(object, &meta, bytes);
        }
        manifests.push(manifest);
    }
    Ok(manifests)
}
