use super::{PushError, validate_tenant};

// cargo-mutants: shared tenant validation glue is covered by HTTP and gRPC callers.
#[cfg_attr(test, mutants::skip)]
pub(crate) fn validate_request_tenant(tenant: &str) -> Result<&str, PushError> {
    if tenant.is_empty() {
        Err(PushError::MissingTenant)
    } else {
        validate_tenant(tenant).map_err(PushError::InvalidTenant)?;
        Ok(tenant)
    }
}
