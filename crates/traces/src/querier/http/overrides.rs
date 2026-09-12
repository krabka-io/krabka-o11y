use super::{
    AppState, Extension, HeaderMap, IntoResponse, Json, Principal, Response, SpanStore, State,
    request_tenant,
};

pub(crate) async fn overrides<S>(
    State(state): State<AppState<S>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
) -> Response
where
    S: SpanStore + 'static,
{
    let tenant = match request_tenant(&headers, &principal, &state.cfg.tenant_policy) {
        Ok(tenant) => tenant,
        Err(rejection) => return *rejection,
    };
    Json(*state.cfg.overrides.for_tenant(tenant.as_str())).into_response()
}
