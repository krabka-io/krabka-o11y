use krabka_blockstore::{
    ConditionalUpdateRequirement, MeteredObjectStore, ObjectStoreMetrics,
    verify_object_store_semantics,
};
use object_store::{path::Path, prefix::PrefixStore};

use super::{Arc, ObjectStore};

/// Builds the metrics object store, wrapped so every request it serves is
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
) -> Result<Arc<dyn ObjectStore>, Box<dyn std::error::Error + Send + Sync>> {
    let parsed = url::Url::parse(url)?;
    let (store, prefix) = object_store::parse_url_opts(&parsed, std::env::vars())?;
    let store = MeteredObjectStore::wrap(Arc::new(PrefixStore::new(store, prefix)), metrics);
    verify_object_store_semantics(
        store.as_ref(),
        &Path::from(""),
        ConditionalUpdateRequirement::for_object_store_url(url),
    )
    .await?;
    Ok(store)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use object_store::{ObjectStoreExt as _, path::Path};

    use super::*;

    #[tokio::test]
    async fn writes_below_the_configured_prefix() {
        let root = tempfile::tempdir().unwrap();
        let prefix = root.path().join("metrics");
        let url = url::Url::from_directory_path(&prefix).unwrap();
        let store = build_object_store(url.as_str(), ObjectStoreMetrics::unregistered())
            .await
            .unwrap();

        store
            .put(&Path::from("probe"), vec![1].into())
            .await
            .unwrap();

        assert!(prefix.join("probe").is_file());
        let probe_leftovers =
            std::fs::read_dir(prefix.join(".krabka-probe")).map_or(0, Iterator::count);
        assert!(
            probe_leftovers == 0,
            "the startup probe cleans up after itself"
        );
    }
}
