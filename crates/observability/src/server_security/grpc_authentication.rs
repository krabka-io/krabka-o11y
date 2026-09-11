use std::{
    sync::Arc,
    task::{Context, Poll},
};

use axum::http::{Request, Response};
use futures_util::future::{Either, Ready, ready};
use tower::Service;

use super::{PeerAddr, Principal, authenticator::Authenticator};

/// The service that [`GrpcAuthenticationLayer`](super::GrpcAuthenticationLayer) puts around a gRPC service.
#[derive(Debug, Clone)]
pub struct GrpcAuthentication<S> {
    inner: S,
    authenticator: Option<Arc<Authenticator>>,
}

impl<S> GrpcAuthentication<S> {
    pub(super) fn new(inner: S, authenticator: Option<Arc<Authenticator>>) -> Self {
        Self {
            inner,
            authenticator,
        }
    }
}

impl<S, RequestBody, ResponseBody> Service<Request<RequestBody>> for GrpcAuthentication<S>
where
    S: Service<Request<RequestBody>, Response = Response<ResponseBody>>,
    ResponseBody: Default,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Either<S::Future, Ready<Result<Self::Response, Self::Error>>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: Request<RequestBody>) -> Self::Future {
        let principal = match &self.authenticator {
            None => Principal::Unauthenticated,
            Some(authenticator) => {
                let peer = request.extensions().get::<PeerAddr>();
                let Some(principal) = authenticator.authenticate(request.headers(), peer) else {
                    let status = tonic::Status::unauthenticated("unauthenticated");
                    return Either::Right(ready(Ok(status.into_http())));
                };
                principal
            }
        };
        request.extensions_mut().insert(principal);
        Either::Left(self.inner.call(request))
    }
}
