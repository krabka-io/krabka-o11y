use super::{
    Arc, BTreeMap, CompactionIndexManifest, MetricsServiceError, ObjectStore,
    load_compaction_manifests_filtered_with_cache,
};

pub(crate) async fn load_compaction_manifests_for_range_with_cache(
    store: Arc<dyn ObjectStore>,
    manifest_prefix: &str,
    start_ms: i64,
    end_ms: i64,
    cache: &tokio::sync::RwLock<BTreeMap<String, Arc<CompactionIndexManifest>>>,
) -> Result<Vec<Arc<CompactionIndexManifest>>, MetricsServiceError> {
    load_compaction_manifests_filtered_with_cache(
        store,
        manifest_prefix,
        Some((start_ms, end_ms)),
        Some(cache),
    )
    .await
}
