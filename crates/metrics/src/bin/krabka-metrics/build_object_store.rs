use super::*;

pub(crate) fn build_object_store(url: &str) -> Result<Arc<dyn ObjectStore>, Box<dyn std::error::Error>> {
    let parsed = url::Url::parse(url)?;
    let (store, _prefix) = object_store::parse_url_opts(&parsed, std::env::vars())?;
    Ok(Arc::from(store))
}
