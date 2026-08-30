use super::{DistributorState, HeaderMap, Span, StatusCode, Response, append_decoded_response, IntoResponse};

pub(crate) async fn append_decoded(
    state: &DistributorState,
    headers: &HeaderMap,
    spans: Vec<Span>,
    success: StatusCode,
) -> Response {
    append_decoded_response(state, headers, spans, success.into_response()).await
}
