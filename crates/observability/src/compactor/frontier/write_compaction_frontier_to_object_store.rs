use super::*;

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn write_compaction_frontier_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    frontier: &CompactionFrontier,
) -> Result<(), CompactionFrontierStoreError> {
    let payload = serde_json::to_vec_pretty(&CompactionFrontierManifest::from(frontier))?;
    store
        .put(
            &compaction_frontier_manifest_object_path(prefix),
            payload.into(),
        )
        .await?;
    Ok(())
}
