use super::{ChainedResolver, DebuginfodConfig, configured_debuginfod_resolvers};

/// Build the native resolver chain with explicit debuginfod resource policy.
///
/// # Errors
///
/// Returns an error when a configured debuginfod URL is invalid or its HTTP
/// client cannot be built.
pub fn native_resolver_from_debuginfod_config(
    urls: Vec<String>,
    config: DebuginfodConfig,
) -> Result<ChainedResolver, crate::ProfilesError> {
    Ok(ChainedResolver::new(configured_debuginfod_resolvers(
        urls, config, true,
    )?))
}
