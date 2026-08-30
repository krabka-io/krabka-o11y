use super::{
    Arc, CompactionIndexManifest, MetricsServiceError, ObjectStore,
    load_compaction_manifests_filtered,
};

///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub async fn load_compaction_manifests_for_range(
    store: Arc<dyn ObjectStore>,
    manifest_prefix: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<Vec<CompactionIndexManifest>, MetricsServiceError> {
    load_compaction_manifests_filtered(store, manifest_prefix, Some((start_ms, end_ms))).await
}
