use super::{Limits, OverridesProvider, Path};

/// The query limits for every tenant: the runtime overrides file when one is
/// named, and `Limits::default()` when none is. Mimir applies its default
/// limits without a runtime config, so a querier with no file still enforces
/// them.
pub(crate) fn load_runtime_overrides(
    path: Option<&Path>,
) -> Result<OverridesProvider, Box<dyn std::error::Error>> {
    let Some(path) = path else {
        return Ok(OverridesProvider::new(Limits::default()));
    };
    let yaml = std::fs::read_to_string(path)?;
    Ok(OverridesProvider::from_yaml(&yaml)?)
}
