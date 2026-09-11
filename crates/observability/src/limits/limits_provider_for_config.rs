use super::{
    Arc, OverridesProvider, ServiceConfig, ServiceConfigError, load_logs_limits_overrides_config,
};

/// The one provider a logs service shares between its distributor and its
/// querier.
///
/// # Errors
/// [`ServiceConfigError::LimitsOverrides`] when
/// `--logs-limits-overrides-config` names a file that cannot be read or does
/// not parse.
pub(crate) fn limits_provider_for_config(
    config: &ServiceConfig,
) -> Result<Arc<OverridesProvider>, ServiceConfigError> {
    Ok(Arc::new(load_logs_limits_overrides_config(config)?))
}
