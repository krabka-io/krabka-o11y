use super::{
    Arc, ObjectStore, RefreshingMetricBlockStore, Router, Url, WalHead, prometheus_router_for_store,
};

pub fn refreshing_blockstore_prometheus_router_with_hot_store(
    store: Arc<dyn ObjectStore>,
    base: Url,
    manifest_prefix: impl Into<String>,
    hot_store: WalHead,
) -> Router {
    prometheus_router_for_store(RefreshingMetricBlockStore::new(
        store,
        base,
        manifest_prefix,
        hot_store,
    ))
}
