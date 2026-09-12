use std::convert::Infallible;

use futures::stream;

use super::{
    Arc, BlockCatalog, Body, Bytes, Extension, HeaderMap, IntoResponse, Principal, QuerierBackend,
    QueryFrontend, Response, State, StatusCode, Uri, bounded_count, request_tenant,
    required_time_bounds, search_query,
};

pub(crate) async fn search_stream<B, C>(
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
    let query = match search_query(&uri) {
        Ok(Some(query)) => query,
        Ok(None) => return (StatusCode::BAD_REQUEST, "missing query parameter q").into_response(),
        Err(error) => return (StatusCode::BAD_REQUEST, error).into_response(),
    };
    let (start_ns, end_ns) = match required_time_bounds(&uri) {
        Ok(bounds) => bounds,
        Err(error) => return (StatusCode::BAD_REQUEST, error).into_response(),
    };
    let limit = bounded_count(&uri, "limit", qf.default_limit());
    let spss = bounded_count(&uri, "spss", qf.default_spss());
    let receiver = match qf
        .search_stream(&tenant, &query, start_ns, end_ns, limit, spss)
        .await
    {
        Ok(receiver) => receiver,
        Err(error) => return (StatusCode::BAD_GATEWAY, error.to_string()).into_response(),
    };
    let body = stream::unfold(receiver, |mut receiver| async move {
        let item = receiver.recv().await?;
        let value = match item {
            Ok(response) => serde_json::to_vec(&response).unwrap_or_default(),
            Err(error) => serde_json::to_vec(&serde_json::json!({ "error": error.to_string() }))
                .unwrap_or_default(),
        };
        let mut line = value;
        line.push(b'\n');
        Some((Ok::<Bytes, Infallible>(Bytes::from(line)), receiver))
    });
    (
        [(("content-type"), "application/x-ndjson")],
        Body::from_stream(body),
    )
        .into_response()
}
