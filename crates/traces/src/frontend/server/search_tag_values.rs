use std::collections::BTreeSet;

use super::{
    ApiVersion, BlockCatalog, FrontendRequest, IntoResponse, Json, Path, QuerierBackend, Response,
    RouteVariant, backend_error_response, json, optional_time_bounds, tenant_and_bounds,
    with_warnings,
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
    let (tenant, start_ns, end_ns) =
        match tenant_and_bounds(request.tenant_request(), optional_time_bounds) {
            Ok(request) => request,
            Err(rejection) => return *rejection,
        };
    let qf = request.qf;
    let (values, _metrics, warnings) = match qf
        .tag_values(
            &tenant,
            &tag,
            krabka_blockstore::TimeRange { start_ns, end_ns },
        )
        .await
    {
        Ok(out) => out,
        Err(err) => return backend_error_response(&err),
    };
    if matches!(V::VARIANT, ApiVersion::V1) {
        let tag_values = values
            .into_iter()
            .map(|value| value.value)
            .collect::<BTreeSet<_>>();
        return Json(json!({ "tagValues": tag_values })).into_response();
    }
    let tag_values: Vec<_> = values
        .iter()
        .map(|v| json!({ "type": &v.type_, "value": &v.value }))
        .collect();
    with_warnings(
        json!({ "tagValues": tag_values, "metrics": { "inspectedBytes": "0" } }),
        &warnings,
    )
}
