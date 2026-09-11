use krabka_blockstore::{MeteredObjectStore, ObjectStoreMetrics};

use super::{Arc, Cli, ConfiguredObjectStore, Url};

/// Builds the traces object store, wrapped so every request it serves is
/// counted in `metrics`.
///
/// The wrap happens here and nowhere else. Every reader and writer in the role
/// gets its store from this function, `DataFusion` included, so one decorator
/// covers the whole role.
pub(crate) fn build_object_store(
    cli: &Cli,
    metrics: ObjectStoreMetrics,
) -> Result<ConfiguredObjectStore, Box<dyn std::error::Error + Send + Sync>> {
    let root = Url::parse(&cli.object_store_url)?;
    let (store, prefix) = object_store::parse_url_opts(&root, std::env::vars())?;
    let configured = ConfiguredObjectStore {
        store: MeteredObjectStore::wrap(Arc::from(store), metrics),
        root,
        prefix,
    };
    tracing::debug!(
        object_store_url = %configured.root,
        object_store_prefix = %configured.prefix,
        "configured traces object store"
    );
    Ok(configured)
}
