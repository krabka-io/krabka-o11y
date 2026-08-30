use super::*;

pub(crate) fn build_object_store(
    url: &str,
) -> Result<ConfiguredObjectStore, Box<dyn std::error::Error + Send + Sync>> {
    let parsed = url::Url::parse(url)?;
    let (store, prefix) = object_store::parse_url_opts(&parsed, std::env::vars())?;
    Ok(ConfiguredObjectStore {
        store: std::sync::Arc::from(store),
        prefix,
    })
}
