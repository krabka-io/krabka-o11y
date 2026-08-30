use super::{OverridesProvider, Path};

pub(crate) fn load_runtime_overrides(
    path: Option<&Path>,
) -> Result<Option<OverridesProvider>, Box<dyn std::error::Error>> {
    let Some(path) = path else {
        return Ok(None);
    };
    let yaml = std::fs::read_to_string(path)?;
    Ok(Some(OverridesProvider::from_yaml(&yaml)?))
}
