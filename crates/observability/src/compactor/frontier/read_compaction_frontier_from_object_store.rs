use super::{
    CompactionFrontier, CompactionFrontierManifest, CompactionFrontierStoreError, ObjectPath,
    ObjectStore, ObjectStoreExt, compaction_frontier_manifest_object_path,
};

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn read_compaction_frontier_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
) -> Result<CompactionFrontier, CompactionFrontierStoreError> {
    let bytes = store
        .get(&compaction_frontier_manifest_object_path(prefix))
        .await?
        .bytes()
        .await?;
    let manifest: CompactionFrontierManifest = serde_json::from_slice(&bytes)?;
    manifest.try_into()
}
