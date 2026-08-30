use super::*;

pub(crate) fn loki_ruler_tenant(headers: &HeaderMap) -> Result<String, HttpQueryError> {
    match headers.get("X-Scope-OrgID") {
        Some(value) => {
            let tenant = value.to_str().map_err(|_| HttpQueryError::InvalidTenant)?;
            if tenant.is_empty() {
                Err(HttpQueryError::InvalidTenant)
            } else {
                Ok(tenant.to_string())
            }
        }
        None => Ok("fake".to_string()),
    }
}
