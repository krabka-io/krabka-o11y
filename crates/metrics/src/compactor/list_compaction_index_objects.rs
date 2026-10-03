use super::{
    Arc, COMPACTION_OBJECT_PREFIX, CompactionIndexListing, CompactionManifestError, ObjectMeta,
    ObjectStore, Path, TryStreamExt,
};

/// Lists the metrics prefix in key order, and splits the manifests and
/// publication markers by [`CompactionIndexListing`].
pub(super) async fn list_compaction_index_objects(
    store: &Arc<dyn ObjectStore>,
) -> Result<CompactionIndexListing<ObjectMeta>, CompactionManifestError> {
    let mut objects = store
        .list(Some(&Path::from(COMPACTION_OBJECT_PREFIX)))
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| CompactionManifestError::ObjectStore(error.to_string()))?;
    objects.sort_by(|left, right| left.location.cmp(&right.location));
    Ok(CompactionIndexListing::new(objects, |object| {
        object.location.as_ref()
    }))
}
