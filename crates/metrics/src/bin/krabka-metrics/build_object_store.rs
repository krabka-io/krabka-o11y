use krabka_blockstore::{MeteredObjectStore, ObjectStoreMetrics};

use super::{Arc, ObjectStore};

/// Builds the metrics object store, wrapped so every request it serves is
/// counted in `metrics`.
///
/// The wrap happens here and nowhere else. Every reader and writer in the role
/// gets its store from this function, `DataFusion` included, so one decorator
/// covers the whole role.
pub(crate) fn build_object_store(
    url: &str,
    metrics: ObjectStoreMetrics,
) -> Result<Arc<dyn ObjectStore>, Box<dyn std::error::Error>> {
    let parsed = url::Url::parse(url)?;
    let (store, _prefix) = object_store::parse_url_opts(&parsed, std::env::vars())?;
    Ok(MeteredObjectStore::wrap(Arc::from(store), metrics))
}
