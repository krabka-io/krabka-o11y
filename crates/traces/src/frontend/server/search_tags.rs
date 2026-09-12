use std::collections::BTreeSet;

use super::{
    Arc, BlockCatalog, Extension, HeaderMap, IntoResponse, Json, Principal, QuerierBackend,
    QueryFrontend, Response, State, StatusCode, Uri, backend_error_response, json,
    optional_time_bounds, request_tenant,
};

pub(crate) async fn search_tags<B, C>(
    State(qf): State<Arc<QueryFrontend<B, C>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    uri: Uri,
) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    let tenant = match request_tenant(&headers, &principal, &qf.cfg.tenant_policy) {
        Ok(tenant) => tenant,
        Err(rejection) => return *rejection,
    };
    let (start_ns, end_ns) = match optional_time_bounds(&uri) {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let (tags, _, _) = match qf.tag_names(&tenant, None, start_ns, end_ns).await {
        Ok(out) => out,
        Err(err) => return backend_error_response(&err),
    };
    let tag_names = tags
        .into_iter()
        .flat_map(|scope| scope.tags)
        .collect::<BTreeSet<_>>();
    Json(json!({ "tagNames": tag_names })).into_response()
}
