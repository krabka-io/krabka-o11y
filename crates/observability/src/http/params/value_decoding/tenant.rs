use super::*;

pub(crate) fn tenant(headers: &HeaderMap) -> Result<&str, HttpQueryError> {
    headers
        .get("X-Scope-OrgID")
        .ok_or(HttpQueryError::MissingTenant)?
        .to_str()
        .map_err(|_| HttpQueryError::InvalidTenant)
        .and_then(|tenant| {
            if tenant.is_empty() {
                Err(HttpQueryError::InvalidTenant)
            } else {
                Ok(tenant)
            }
        })
}
