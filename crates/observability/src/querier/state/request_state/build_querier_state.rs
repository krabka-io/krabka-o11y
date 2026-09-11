use super::{
    Arc, ObjectStore, OverridesProvider, QuerierState, ServiceConfig, ServiceConfigError,
    build_querier_state_with_object_store_prefix, limits_provider_for_config,
};

/// A querier state with its own limits provider, built from `config` alone.
///
/// A service builds one provider for the whole process and threads that one in
/// instead. This entry point is for a caller that has only a config, such as a
/// test or an embedder.
///
/// # Errors
/// Returns an error when the configured index cannot be read, when the object
/// store is missing for the configured index source, or when the runtime
/// overrides file cannot be read.
pub async fn build_querier_state(
    config: &ServiceConfig,
    object_store: Option<&dyn ObjectStore>,
) -> Result<QuerierState, ServiceConfigError> {
    let overrides = limits_provider_for_config(config)?;
    build_querier_state_with_overrides(config, object_store, overrides).await
}

/// A querier state that resolves every tenant through the provider the service
/// already built.
///
/// # Errors
/// Returns an error when the configured index cannot be read, or when the
/// object store is missing for the configured index source.
pub(crate) async fn build_querier_state_with_overrides(
    config: &ServiceConfig,
    object_store: Option<&dyn ObjectStore>,
    overrides: Arc<OverridesProvider>,
) -> Result<QuerierState, ServiceConfigError> {
    build_querier_state_with_object_store_prefix(config, object_store, None, overrides).await
}
