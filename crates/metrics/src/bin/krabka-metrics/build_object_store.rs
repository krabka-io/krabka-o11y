use krabka_blockstore::{MeteredObjectStore, ObjectStoreMetrics};
use object_store::prefix::PrefixStore;

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
    let (store, prefix) = object_store::parse_url_opts(&parsed, std::env::vars())?;
    Ok(MeteredObjectStore::wrap(
        Arc::new(PrefixStore::new(store, prefix)),
        metrics,
    ))
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
        let store = build_object_store(url.as_str(), ObjectStoreMetrics::unregistered()).unwrap();

        store
            .put(&Path::from("probe"), vec![1].into())
            .await
            .unwrap();

        assert!(prefix.join("probe").is_file());
    }
}
