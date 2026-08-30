use super::*;

pub(crate) fn load_tenant_limits_config(
    path: Option<&Path>,
) -> Result<TenantLimitConfig, Box<dyn std::error::Error>> {
    let Some(path) = path else {
        return Ok(TenantLimitConfig::default());
    };
    let bytes = std::fs::read(path)?;
    Ok(serde_json::from_slice(&bytes)?)
}
