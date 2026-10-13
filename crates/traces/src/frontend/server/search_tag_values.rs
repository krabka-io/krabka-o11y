use std::collections::BTreeSet;

use super::{
    ApiVersion, BlockCatalog, FrontendRequest, IntoResponse, Json, Path, QuerierBackend, Response,
    RouteVariant, backend_error_response, json, query_time_range, with_warnings,
};

/// `/api/search/tag/{tag}/values`, or for [`ApiVersion::V2`] the typed
/// `/api/v2/search/tag/{tag}/values`.
pub(crate) async fn search_tag_values<B, C, V>(
    request: FrontendRequest<B, C>,
    Path(tag): Path<String>,
) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
    V: RouteVariant<ApiVersion>,
{
    let bounds = match request.tenant_and_optional_bounds() {
        Ok(bounds) => bounds,
        Err(rejection) => return *rejection,
    };
    let qf = request.qf;
    let result = match qf
        .tag_values(&bounds.tenant, &tag, query_time_range(&bounds))
        .await
    {
        Ok(out) => out,
        Err(err) => return backend_error_response(&err),
    };
    if matches!(V::VARIANT, ApiVersion::V1) {
        let tag_values = result
            .values
            .into_iter()
            .map(|value| value.value)
            .collect::<BTreeSet<_>>();
        return Json(json!({ "tagValues": tag_values })).into_response();
    }
    let tag_values: Vec<_> = result
        .values
        .iter()
        .map(|v| json!({ "type": &v.type_, "value": &v.value }))
        .collect();
    with_warnings(
        json!({ "tagValues": tag_values, "metrics": { "inspectedBytes": "0" } }),
        &result.warnings,
    )
}
