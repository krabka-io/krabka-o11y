use krabka_blockstore::MeteredObjectStore;
use krabka_metrics_service::RefreshingMetricBlockStore;

use super::{Arc, Cli, ObjectStore, RoleReadiness, WalHead};

/// The object store a serving role reads blocks from, metered and tracked by
/// the role's readiness.
pub(crate) struct RoleObjectStore {
    pub(crate) url: url::Url,
    pub(crate) store: Arc<dyn ObjectStore>,
}

impl RoleObjectStore {
    /// Opens `--object-store-url` under its path prefix.
    pub(crate) fn open(
        cli: &Cli,
        metrics: &krabka_promql::metrics::ServiceMetrics,
        readiness: &RoleReadiness,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let url = url::Url::parse(&cli.object_store_url)?;
        let (store, prefix) = object_store::parse_url_opts(&url, std::env::vars())?;
        let store: Arc<dyn ObjectStore> =
            Arc::new(object_store::prefix::PrefixStore::new(store, prefix));
        let object_store_metrics = metrics.object_store.clone();
        readiness.track_object_store(object_store_metrics.clone());
        Ok(Self {
            url,
            store: MeteredObjectStore::wrap(store, object_store_metrics),
        })
    }

    /// The block store over `--manifest-prefix` that `head` tops up with the
    /// recent window, cached as the CLI configures.
    pub(crate) fn refreshing_metric_store(
        &self,
        cli: &Cli,
        head: WalHead,
    ) -> RefreshingMetricBlockStore {
        RefreshingMetricBlockStore::new(
            Arc::clone(&self.store),
            self.url.clone(),
            &cli.manifest_prefix,
            head,
        )
        .with_cold_cache_ttl(cli.cold_cache_ttl)
        .with_unbounded_compatibility_lookback(cli.unbounded_compatibility_lookback)
    }
}
