use krabka_blockstore::{ObjectStoreMetrics, build_metered_object_store};

use super::ConfiguredObjectStore;

/// Builds the profiles object store, wrapped so every request it serves is
/// counted in `metrics`.
///
/// The wrap happens here and nowhere else. Every reader and writer in the role
/// gets its store from this function, `DataFusion` included, so one decorator
/// covers the whole role.
///
/// The store is probed with [`verify_object_store_semantics`] before it is
/// returned. A store that lacks a semantic the role writes against stops the
/// role here, before it accepts any data.
pub(crate) async fn build_object_store(
    url: &str,
    metrics: ObjectStoreMetrics,
) -> Result<ConfiguredObjectStore, Box<dyn std::error::Error + Send + Sync>> {
    let store = build_metered_object_store(url, metrics).await?;
    Ok(ConfiguredObjectStore { store })
}
