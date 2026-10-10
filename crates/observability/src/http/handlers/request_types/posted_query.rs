use axum::extract::{FromRequest, FromRequestParts, Request};

use super::{
    Bytes, HeaderMap, HttpQueryError, Instant, IntoResponse, QuerierState, QueryKind,
    RequestSecurity, Response, SeriesParams, handle_query, parse_posted_series_params,
    post_query_params_body_first,
};

/// The extracted parts of a Loki query POST: the URL query string and the
/// form body that carries the remaining parameters.
pub(crate) struct PostedQueryRequest {
    pub security: RequestSecurity,
    pub headers: HeaderMap,
    pub raw_query: Option<String>,
    pub body: Bytes,
}

impl PostedQueryRequest {
    /// The request's parameters as one query string, the form body's taking
    /// precedence over the URL's.
    pub(crate) fn body_first_query(&self) -> Result<String, HttpQueryError> {
        post_query_params_body_first(self.raw_query.as_deref(), &self.body)
    }

    /// The request's parameters, read as a series or label-names request.
    pub(crate) fn series_params(&self) -> Result<SeriesParams, HttpQueryError> {
        parse_posted_series_params(self.raw_query.as_deref(), &self.body)
    }
}

/// Extracts the parts in the order a handler taking `RequestSecurity`,
/// `HeaderMap`, `RawQuery` and `Bytes` would, so a request without a
/// principal is refused before its body is read.
impl<S: Send + Sync> FromRequest<S> for PostedQueryRequest {
    type Rejection = Response;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        let (mut parts, body) = request.into_parts();
        let security = RequestSecurity::from_request_parts(&mut parts, state)
            .await
            .map_err(IntoResponse::into_response)?;
        let headers = parts.headers.clone();
        let raw_query = parts.uri.query().map(ToOwned::to_owned);
        let body = Bytes::from_request(Request::from_parts(parts, body), state)
            .await
            .map_err(IntoResponse::into_response)?;
        Ok(Self {
            security,
            headers,
            raw_query,
            body,
        })
    }
}

/// Runs a posted `query` or `query_range` request and records its outcome
/// under the route label of `kind`.
pub(crate) async fn handle_posted_query(
    state: QuerierState,
    request: PostedQueryRequest,
    kind: QueryKind,
) -> Response {
    let start = Instant::now();
    let route = kind.route_label();
    let resp = match request.body_first_query() {
        Ok(raw_query) => {
            // Boxed so the handler's own future stays small; the query
            // future is the large part of it.
            Box::pin(handle_query(
                state.clone(),
                request.security,
                request.headers,
                Some(&raw_query),
                kind,
            ))
            .await
        }
        Err(error) => error.into_response(),
    };
    state.record_query(route, resp.status(), start);
    resp
}
