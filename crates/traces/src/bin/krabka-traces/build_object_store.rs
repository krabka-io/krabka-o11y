use super::{Arc, Cli, ConfiguredObjectStore, Parser, Url};

pub(crate) fn build_object_store(
    cli: &Cli,
) -> Result<ConfiguredObjectStore, Box<dyn std::error::Error + Send + Sync>> {
    let root = Url::parse(&cli.object_store_url)?;
    let (store, prefix) = object_store::parse_url_opts(&root, std::env::vars())?;
    let configured = ConfiguredObjectStore {
        store: Arc::from(store),
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
