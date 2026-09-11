use super::{
    TENANT_HEADER, TenantId, TenantRequestError, TenantResolveError, resolve_single_tenant,
};

/// The one tenant an OTLP gRPC export names in its `x-scope-orgid` metadata.
///
/// Loki serves OTLP over HTTP only, so no upstream answer exists for this
/// door. A request without a tenant gets `UNAUTHENTICATED`, because Loki
/// answers the HTTP push with 401. A malformed tenant, or more than one, gets
/// `INVALID_ARGUMENT`, because Loki answers the HTTP push with 400. The
/// message is the text the HTTP push sends.
pub(crate) fn grpc_tenant(
    metadata: &tonic::metadata::MetadataMap,
) -> Result<TenantId, tonic::Status> {
    let value = metadata
        .get(TENANT_HEADER)
        .map(tonic::metadata::MetadataValue::as_encoded_bytes);
    resolve_single_tenant(value).map_err(|error| match error {
        TenantRequestError::Resolve(TenantResolveError::Missing) => {
            tonic::Status::unauthenticated(error.to_string())
        }
        TenantRequestError::Resolve(_) | TenantRequestError::MultipleOrgIds => {
            tonic::Status::invalid_argument(error.to_string())
        }
    })
}
