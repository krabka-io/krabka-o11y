use std::collections::BTreeSet;

use super::{
    ApiVersion, BlockCatalog, FrontendRequest, IntoResponse, Json, QuerierBackend, Response,
    RouteVariant, StatusCode, backend_error_response, json, query_time_range, scope_name,
    scope_param, status_text_response, with_warnings,
};

/// `/api/search/tags`, or for [`ApiVersion::V2`] the scoped
/// `/api/v2/search/tags`.
pub(crate) async fn search_tags<B, C, V>(request: FrontendRequest<B, C>) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
    V: RouteVariant<ApiVersion>,
{
    let bounds = match request.tenant_and_optional_bounds() {
        Ok(bounds) => bounds,
        Err(rejection) => return *rejection,
    };
    let FrontendRequest { qf, uri, .. } = request;
    // The v1 endpoint lists every scope's names as one flat set.
    let scope = match V::VARIANT {
        ApiVersion::V1 => None,
        ApiVersion::V2 => match scope_param(&uri) {
            Ok(scope) => scope,
            Err(err) => return status_text_response(StatusCode::BAD_REQUEST, err),
        },
    };
    let result = match qf
        .tag_names(&bounds.tenant, scope, query_time_range(&bounds))
        .await
    {
        Ok(out) => out,
        Err(err) => return backend_error_response(&err),
    };
    if matches!(V::VARIANT, ApiVersion::V1) {
        let tag_names = result
            .tags
            .into_iter()
            .flat_map(|scope| scope.tags)
            .collect::<BTreeSet<_>>();
        return Json(json!({ "tagNames": tag_names })).into_response();
    }
    let scopes: Vec<_> = result
        .tags
        .iter()
        .map(|st| json!({ "name": scope_name(st.scope), "tags": &st.tags }))
        .collect();
    with_warnings(
        json!({ "scopes": scopes, "metrics": { "inspectedBytes": "0" } }),
        &result.warnings,
    )
}
