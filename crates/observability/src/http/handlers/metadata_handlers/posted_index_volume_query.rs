use super::{
    Bytes, HeaderMap, IntoResponse, QuerierState, RequestSecurity, Response, StatusCode,
    VolumeKind, execute_index_volume_query, json_response, post_query_params_body_first,
};

/// The parts of a `POST` to `/index/volume` or `/index/volume_range`.
pub(crate) struct PostedIndexVolumeQuery<'a> {
    pub(crate) security: &'a RequestSecurity,
    pub(crate) headers: &'a HeaderMap,
    pub(crate) raw_query: Option<&'a str>,
    pub(crate) body: &'a Bytes,
    pub(crate) kind: VolumeKind,
}

/// Answers a `POST` volume request, whose form body takes precedence over its
/// URL query string.
pub(crate) async fn posted_index_volume_response(
    state: &QuerierState,
    request: PostedIndexVolumeQuery<'_>,
) -> Response {
    let raw_query = match post_query_params_body_first(request.raw_query, request.body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    match execute_index_volume_query(
        state,
        request.security,
        request.headers,
        Some(&raw_query),
        request.kind,
    )
    .await
    {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}
