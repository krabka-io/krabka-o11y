use super::{
    Arc, BlockCatalog, Extension, HeaderMap, IntoResponse, Json, Principal, QuerierBackend,
    QueryFrontend, Response, State, request_tenant,
};

pub(crate) async fn overrides<B, C>(
    State(qf): State<Arc<QueryFrontend<B, C>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    let tenant = match request_tenant(&headers, &principal, &qf.cfg.tenant_policy) {
        Ok(tenant) => tenant,
        Err(rejection) => return *rejection,
    };
    Json(*qf.cfg.overrides.for_tenant(tenant.as_str())).into_response()
}
