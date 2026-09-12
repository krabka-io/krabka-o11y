use krabka_blockstore::{MeteredObjectStore, ObjectStoreMetrics};
use object_store::prefix::PrefixStore;

use super::ConfiguredObjectStore;

/// Builds the profiles object store, wrapped so every request it serves is
/// counted in `metrics`.
///
/// The wrap happens here and nowhere else. Every reader and writer in the role
/// gets its store from this function, `DataFusion` included, so one decorator
/// covers the whole role.
pub(crate) fn build_object_store(
    url: &str,
    metrics: ObjectStoreMetrics,
) -> Result<ConfiguredObjectStore, Box<dyn std::error::Error + Send + Sync>> {
    let parsed = url::Url::parse(url)?;
    let (store, prefix) = object_store::parse_url_opts(&parsed, std::env::vars())?;
    Ok(ConfiguredObjectStore {
        store: MeteredObjectStore::wrap(
            std::sync::Arc::new(PrefixStore::new(store, prefix)),
            metrics,
        ),
    })
}
