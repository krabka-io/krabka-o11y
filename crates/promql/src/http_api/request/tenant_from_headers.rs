use super::*;

pub(crate) fn tenant_from_headers(headers: &HeaderMap) -> Result<String, ApiError> {
    let tenant = headers
        .get("X-Scope-OrgID")
        .and_then(|value| value.to_str().ok())
        .filter(|tenant| !tenant.is_empty())
        .map(str::to_string)
        .ok_or_else(|| ApiError::bad_data("missing X-Scope-OrgID tenant header"))?;
    validate_tenant(&tenant).map_err(ApiError::bad_data)?;
    Ok(tenant)
}
