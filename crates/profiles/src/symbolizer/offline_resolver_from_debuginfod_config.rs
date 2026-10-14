use super::{ChainedResolver, DebuginfodConfig, configured_debuginfod_resolvers};

/// Resolver used by the persistent symbolizer. Unlike the query fallback it
/// leaves misses unresolved so a later debuginfo upload can fill them.
///
/// # Errors
/// Returns an error when the debuginfod client configuration is invalid.
pub fn offline_resolver_from_debuginfod_config(
    urls: Vec<String>,
    config: DebuginfodConfig,
) -> Result<ChainedResolver, crate::ProfilesError> {
    Ok(ChainedResolver::new(configured_debuginfod_resolvers(
        urls, config, false,
    )?))
}
