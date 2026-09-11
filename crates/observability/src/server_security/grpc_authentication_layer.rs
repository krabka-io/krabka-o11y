use std::sync::Arc;

use tower::Layer;

use super::{GrpcAuthentication, ServerSecurity, authenticator::Authenticator};

/// A `tower::Layer` that authenticates gRPC requests as the HTTP listeners do.
///
/// Add it with `tonic::transport::Server::builder().layer(..)`, and serve the
/// server on [`grpc_incoming`](super::grpc_incoming) so that a verified client
/// certificate reaches the layer. A request authenticates with a bearer token
/// in its `authorization` metadata, a basic credential there, or the
/// connection's verified client certificate. A failed request gets the gRPC
/// status `Unauthenticated`. A request that passes gets its
/// [`Principal`](super::Principal) in the extensions, which tonic hands to
/// the service as `tonic::Request::extensions`.
///
/// The [`UnauthenticatedRoutes`](super::UnauthenticatedRoutes) list does not
/// apply to gRPC.
#[derive(Debug, Clone)]
pub struct GrpcAuthenticationLayer {
    authenticator: Option<Arc<Authenticator>>,
}

impl GrpcAuthenticationLayer {
    /// A layer that authenticates as `security` says.
    #[must_use]
    pub fn new(security: &ServerSecurity) -> Self {
        Self {
            authenticator: security.authenticator(),
        }
    }
}

impl<S> Layer<S> for GrpcAuthenticationLayer {
    type Service = GrpcAuthentication<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcAuthentication::new(inner, self.authenticator.clone())
    }
}
