use std::{error::Error, sync::Arc};

use object_store::{ObjectStore, path::Path, prefix::PrefixStore};

use crate::{
    ConditionalUpdateRequirement, MeteredObjectStore, ObjectStoreMetrics,
    verify_object_store_semantics,
};

/// Opens, meters, and verifies an object store URL.
pub async fn build_metered_object_store(
    url: &str,
    metrics: ObjectStoreMetrics,
) -> Result<Arc<dyn ObjectStore>, Box<dyn Error + Send + Sync>> {
    let parsed = url::Url::parse(url)?;
    let parsed_store = object_store::parse_url_opts(&parsed, std::env::vars())?;
    let store = MeteredObjectStore::wrap(
        Arc::new(PrefixStore::new(parsed_store.0, parsed_store.1)),
        metrics,
    );
    verify_object_store_semantics(
        store.as_ref(),
        &Path::from(""),
        ConditionalUpdateRequirement::for_object_store_url(url),
    )
    .await?;
    Ok(store)
}
