use super::{FsPath, OverridesError, OverridesProvider, ServiceConfig, limits_for_config};

/// Builds the one provider a logs service resolves every tenant through.
///
/// With no `--logs-limits-overrides-config` the provider holds only the process
/// defaults, so every tenant gets the same numbers. With one, the file's
/// `defaults` block and its per-tenant entries merge over those.
///
/// # Errors
///
/// [`OverridesError::Read`] when the file cannot be read, and
/// [`OverridesError::Yaml`] when it does not parse.
pub(crate) fn load_logs_limits_overrides_config(
    config: &ServiceConfig,
) -> Result<OverridesProvider, OverridesError> {
    let base = limits_for_config(config);
    let Some(path) = config.logs_limits_overrides_config.as_deref() else {
        return Ok(OverridesProvider::new(base));
    };
    let text = read_overrides_file(path)?;
    OverridesProvider::from_yaml_over(&text, &base)
}

fn read_overrides_file(path: &FsPath) -> Result<String, OverridesError> {
    std::fs::read_to_string(path).map_err(|error| OverridesError::Read {
        path: path.display().to_string(),
        reason: error.to_string(),
    })
}
