use super::{ChainedResolver, DebuginfodConfig, native_resolver_from_debuginfod_config};

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub fn native_resolver_from_debuginfod_urls(
    urls: Vec<String>,
) -> Result<ChainedResolver, crate::ProfilesError> {
    native_resolver_from_debuginfod_config(urls, DebuginfodConfig::default())
}
