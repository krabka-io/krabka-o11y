use super::{
    Arc, ChainedResolver, DebuginfodConfig, DebuginfodResolver, FileSystemResolver, NativeResolver,
};

/// Resolver used by the persistent symbolizer. Unlike the query fallback it
/// leaves misses unresolved so a later debuginfo upload can fill them.
pub fn offline_resolver_from_debuginfod_config(
    urls: Vec<String>,
    config: DebuginfodConfig,
) -> Result<ChainedResolver, crate::ProfilesError> {
    let mut resolvers: Vec<Arc<dyn NativeResolver>> = vec![Arc::new(FileSystemResolver::default())];
    if !urls.is_empty() {
        resolvers.push(Arc::new(
            DebuginfodResolver::with_config(urls, config).map_err(crate::ProfilesError::Block)?,
        ));
    }
    Ok(ChainedResolver::new(resolvers))
}
