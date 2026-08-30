use super::{HeaderMap, PushError, validate_request_tenant};

// cargo-mutants: covered through HTTP push-path tenant validation tests.
#[cfg_attr(test, mutants::skip)]
pub(crate) fn tenant_from_headers(headers: &HeaderMap) -> Result<&str, PushError> {
    headers
        .get("X-Scope-OrgID")
        .ok_or(PushError::MissingTenant)?
        .to_str()
        .map_err(|_| PushError::MissingTenant)
        .and_then(validate_request_tenant)
}
