use super::{
    Arc, BlockStore, MetricBlockStore, MetricsServiceError, ObjectStore, Router, Url,
    load_compaction_manifests, prometheus_router_for_store,
};

///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub async fn blockstore_prometheus_router(
    store: Arc<dyn ObjectStore>,
    base: Url,
    manifest_prefix: &str,
) -> Result<Router, MetricsServiceError> {
    let manifests = load_compaction_manifests(store.clone(), manifest_prefix).await?;
    let metric_store = MetricBlockStore::from_compaction_manifests(
        BlockStore::new(store.clone(), base.clone()),
        Some(BlockStore::new(store, base)),
        &manifests,
    );
    Ok(prometheus_router_for_store(metric_store))
}
