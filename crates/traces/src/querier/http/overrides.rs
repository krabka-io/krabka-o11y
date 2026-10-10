use super::{
    Bytes, Method, QuerierRequest, Response, SpanStore, overrides_api_response, request_tenant,
};

pub(crate) async fn overrides<S>(
    request: QuerierRequest<S>,
    method: Method,
    body: Bytes,
) -> Response
where
    S: SpanStore + 'static,
{
    let QuerierRequest {
        state,
        principal,
        headers,
        uri,
    } = request;
    let tenant = match request_tenant(&headers, &principal, &state.cfg.tenant_policy) {
        Ok(tenant) => tenant,
        Err(rejection) => return *rejection,
    };
    overrides_api_response(
        &state.cfg.overrides,
        tenant.as_str(),
        &method,
        &headers,
        uri.query(),
        &body,
    )
}
