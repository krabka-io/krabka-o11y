use super::{
    Arc, CompactionIndexManifest, ManifestTimeRange, MetricsServiceError, ObjectStore,
    load_compaction_manifests_filtered_with_cache,
};

pub(crate) async fn load_compaction_manifests_filtered(
    store: Arc<dyn ObjectStore>,
    manifest_prefix: &str,
    time_range: Option<ManifestTimeRange>,
) -> Result<Vec<CompactionIndexManifest>, MetricsServiceError> {
    Ok(
        load_compaction_manifests_filtered_with_cache(store, manifest_prefix, time_range, None)
            .await?
            .into_iter()
            .map(Arc::unwrap_or_clone)
            .collect(),
    )
}
