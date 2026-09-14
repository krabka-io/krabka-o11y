use super::{
    Arc, BlockCatalog, Bytes, Extension, HeaderMap, Method, Principal, QuerierBackend,
    QueryFrontend, Response, State, Uri, overrides_api_response, request_tenant,
};

pub(crate) async fn overrides<B, C>(
    State(qf): State<Arc<QueryFrontend<B, C>>>,
    Extension(principal): Extension<Principal>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    let tenant = match request_tenant(&headers, &principal, &qf.cfg.tenant_policy) {
        Ok(tenant) => tenant,
        Err(rejection) => return *rejection,
    };
    overrides_api_response(
        &qf.cfg.overrides,
        tenant.as_str(),
        &method,
        &headers,
        uri.query(),
        &body,
    )
}
