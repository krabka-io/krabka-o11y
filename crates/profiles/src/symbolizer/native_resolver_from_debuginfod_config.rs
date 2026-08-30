use super::{
    AddressFallbackResolver, Arc, ChainedResolver, DebuginfodConfig, DebuginfodResolver,
    FileSystemResolver, NativeResolver,
};

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
    let mut resolvers: Vec<Arc<dyn NativeResolver>> = vec![Arc::new(FileSystemResolver::default())];
    if !urls.is_empty() {
        let debuginfod =
            DebuginfodResolver::with_config(urls, config).map_err(crate::ProfilesError::Block)?;
        resolvers.push(Arc::new(debuginfod));
    }
    resolvers.push(Arc::new(AddressFallbackResolver));
    Ok(ChainedResolver::new(resolvers))
}
