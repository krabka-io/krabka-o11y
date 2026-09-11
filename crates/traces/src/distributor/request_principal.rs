use super::{GrpcRequest, GrpcStatus, Principal};

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
