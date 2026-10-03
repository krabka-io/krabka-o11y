use krabka_blockstore::{
    ConditionalUpdateRequirement, MeteredObjectStore, ObjectStoreMetrics,
    verify_object_store_semantics,
};
use object_store::{path::Path, prefix::PrefixStore};

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
    let parsed = url::Url::parse(url)?;
    let (store, prefix) = object_store::parse_url_opts(&parsed, std::env::vars())?;
    let store = MeteredObjectStore::wrap(
        std::sync::Arc::new(PrefixStore::new(store, prefix)),
        metrics,
    );
    verify_object_store_semantics(
        store.as_ref(),
        &Path::from(""),
        ConditionalUpdateRequirement::for_object_store_url(url),
    )
    .await?;
    Ok(ConfiguredObjectStore { store })
}
