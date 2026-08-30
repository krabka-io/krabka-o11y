use super::{PushError, validate_request_tenant};

// cargo-mutants: covered through OTLP gRPC push-path tenant validation tests.
#[cfg_attr(test, mutants::skip)]
pub(crate) fn tenant_from_metadata(
    metadata: &tonic::metadata::MetadataMap,
) -> Result<&str, PushError> {
    metadata
        .get("x-scope-orgid")
        .ok_or(PushError::MissingTenant)?
        .to_str()
        .map_err(|_| PushError::MissingTenant)
        .and_then(validate_request_tenant)
}
