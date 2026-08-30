use super::*;

pub(crate) fn load_profiles_limits_overrides_config(
    path: Option<&Path>,
) -> Result<OverridesProvider, Box<dyn std::error::Error>> {
    let Some(path) = path else {
        return Ok(OverridesProvider::new(Limits::default()));
    };
    let text = std::fs::read_to_string(path)?;
    Ok(OverridesProvider::from_yaml(&text)?)
}
