use super::{GrpcRequest, GrpcStatus, Principal, TracesError};

pub(crate) fn grpc_status_from_error(err: &TracesError) -> GrpcStatus {
    match err {
        TracesError::Limit(_) | TracesError::RateLimit(_) => {
            GrpcStatus::resource_exhausted(err.to_string())
        }
        TracesError::Invalid(_)
        | TracesError::Tenant(_)
        | TracesError::Decode(_)
        | TracesError::TooLarge { .. } => GrpcStatus::invalid_argument(err.to_string()),
        TracesError::TenantDenied(_) => GrpcStatus::permission_denied(err.to_string()),
        TracesError::UnsupportedContentType(_) => GrpcStatus::unimplemented(err.to_string()),
        TracesError::Wal(_)
        | TracesError::Produce(_)
        | TracesError::ProduceBatch { .. }
        | TracesError::Block(_) => GrpcStatus::internal(err.to_string()),
    }
}

/// The principal that `GrpcAuthenticationLayer` put on a gRPC request.
///
/// A request without one did not pass through the authentication layer. It
/// gets `Internal` and never reaches the WAL, as a missing `Extension` is a
/// 500 on the HTTP doors.
pub(crate) fn request_principal<T>(request: &GrpcRequest<T>) -> Result<&Principal, GrpcStatus> {
    request.extensions().get::<Principal>().ok_or_else(|| {
        GrpcStatus::internal(
            "the request carries no principal: serve the service through the authentication layer",
        )
    })
}
