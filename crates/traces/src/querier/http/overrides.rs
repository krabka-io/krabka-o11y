use super::{
    AppState, Bytes, Extension, HeaderMap, Method, Principal, Response, SpanStore, State, Uri,
    overrides_api_response, request_tenant,
};

pub(crate) async fn overrides<S>(
    State(state): State<AppState<S>>,
    Extension(principal): Extension<Principal>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response
where
    S: SpanStore + 'static,
{
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
