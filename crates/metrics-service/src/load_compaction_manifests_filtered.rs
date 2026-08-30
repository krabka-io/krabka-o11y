use super::{
    Arc, CompactionIndexManifest, MetricsServiceError, ObjectStore,
    load_compaction_manifests_filtered_with_cache,
};

pub(crate) async fn load_compaction_manifests_filtered(
    store: Arc<dyn ObjectStore>,
    manifest_prefix: &str,
    time_range: Option<(i64, i64)>,
) -> Result<Vec<CompactionIndexManifest>, MetricsServiceError> {
    load_compaction_manifests_filtered_with_cache(store, manifest_prefix, time_range, None).await
}
