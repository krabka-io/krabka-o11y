use std::path::Path;

use super::{Limits, OverridesProvider};

/// Read the per-tenant limit overrides this process was pointed at.
///
/// `defaults` are the limits the command line built. A tenant the file does not
/// name gets them unchanged, and a tenant it does name gets them with only the
/// keys of its entry replaced. With no file, every tenant gets `defaults`, so a
/// caller always has a provider and never a `None` to fall back from.
///
/// # Errors
/// Returns an error when the file cannot be read, or when it is not the
/// expected YAML document.
pub(crate) fn load_traces_limits_overrides_config(
    path: Option<&Path>,
    api_path: Option<&Path>,
    defaults: Limits,
) -> Result<OverridesProvider, Box<dyn std::error::Error + Send + Sync>> {
    let provider = if let Some(path) = path {
        let text = std::fs::read_to_string(path)?;
        OverridesProvider::from_yaml_with_defaults(&text, defaults)?
    } else {
        OverridesProvider::new(defaults)
    };
    match api_path {
        Some(path) => provider
            .with_api_file(path.to_path_buf())
            .map_err(Into::into),
        None => Ok(provider),
    }
}
