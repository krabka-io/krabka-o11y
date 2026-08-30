use super::{
    Arc, ObjectStore, Router, Url, WalHead, refreshing_blockstore_prometheus_router_with_hot_store,
};

pub fn refreshing_blockstore_prometheus_router(
    store: Arc<dyn ObjectStore>,
    base: Url,
    manifest_prefix: impl Into<String>,
) -> Router {
    refreshing_blockstore_prometheus_router_with_hot_store(
        store,
        base,
        manifest_prefix,
        WalHead::new(),
    )
}
