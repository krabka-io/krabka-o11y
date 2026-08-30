use super::{
    Arc, CompactionIndexManifest, MetricsServiceError, ObjectStore,
    load_compaction_manifests_filtered,
};

///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub async fn load_compaction_manifests(
    store: Arc<dyn ObjectStore>,
    manifest_prefix: &str,
) -> Result<Vec<CompactionIndexManifest>, MetricsServiceError> {
    load_compaction_manifests_filtered(store, manifest_prefix, None).await
}
