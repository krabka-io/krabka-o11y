use axum::{
    Extension,
    extract::FromRequestParts,
    http::{HeaderMap, request::Parts},
    response::{IntoResponse, Response},
};
use krabka_observability::server_security::Principal;

use super::{
    ApiError, CardinalityParams, DiscoveryParams, RequestAuth, parse_cardinality_params,
    parse_discovery_params,
};

/// The authenticated principal and the headers of one API request.
///
/// It extracts as `Extension<Principal>` followed by `HeaderMap`, and rejects
/// a request without a principal exactly as `Extension<Principal>` does.
pub(crate) struct RequestCaller {
    pub(crate) principal: Principal,
    pub(crate) headers: HeaderMap,
}

impl RequestCaller {
    /// Borrows the caller as the [`RequestAuth`] the handlers authorize with.
    pub(crate) fn auth(&self) -> RequestAuth<'_> {
        RequestAuth {
            headers: &self.headers,
            principal: &self.principal,
        }
    }
}

impl<S: Send + Sync> FromRequestParts<S> for RequestCaller {
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Extension(principal) = Extension::<Principal>::from_request_parts(parts, state)
            .await
            .map_err(IntoResponse::into_response)?;
        Ok(Self {
            principal,
            headers: parts.headers.clone(),
        })
    }
}

/// A parameter set an API handler parses from the raw query string.
pub(crate) trait RawQueryParams: Sized + Send {
    /// Parses the parameters from the raw query string, if the request has one.
    fn parse_raw_query(raw_query: Option<&str>) -> Result<Self, ApiError>;
}

impl RawQueryParams for CardinalityParams {
    fn parse_raw_query(raw_query: Option<&str>) -> Result<Self, ApiError> {
        parse_cardinality_params(raw_query)
    }
}

impl RawQueryParams for DiscoveryParams {
    fn parse_raw_query(raw_query: Option<&str>) -> Result<Self, ApiError> {
        parse_discovery_params(raw_query)
    }
}

/// Query-string parameters parsed by [`RawQueryParams`]; a parse error is the
/// rejection response.
pub(crate) struct ParsedQuery<Params>(pub(crate) Params);

impl<S: Send + Sync, Params: RawQueryParams> FromRequestParts<S> for ParsedQuery<Params> {
    type Rejection = Response;

    fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        std::future::ready(
            Params::parse_raw_query(parts.uri.query())
                .map(Self)
                .map_err(IntoResponse::into_response),
        )
    }
}
