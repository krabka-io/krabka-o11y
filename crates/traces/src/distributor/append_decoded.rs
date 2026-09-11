use super::{
    DistributorState, IntoResponse, Response, Span, StatusCode, TenantId, append_decoded_response,
};

pub(crate) async fn append_decoded(
    state: &DistributorState,
    tenant: &TenantId,
    spans: Vec<Span>,
    success: StatusCode,
) -> Response {
    append_decoded_response(state, tenant, spans, success.into_response()).await
}
