pub(crate) mod metadata_handlers;
pub(crate) mod query_execution;
pub(crate) mod request_types;

use axum::response::IntoResponse;

use crate::{
    HeaderMap, Instant, QuerierState, RequestSecurity, Response, execute_label_names_query,
    execute_series_query, parse_series_params,
};

pub(crate) enum MetadataQuery {
    Series,
    LabelNames,
}

pub(crate) async fn record_metadata_query(
    state: QuerierState,
    security: RequestSecurity,
    headers: HeaderMap,
    raw_query: Option<String>,
    metric: &'static str,
    query: MetadataQuery,
) -> Response {
    let start = Instant::now();
    let response = match parse_series_params(raw_query.as_deref()) {
        Ok(params) => {
            let result = match query {
                MetadataQuery::Series => {
                    execute_series_query(&state, &security, &headers, &params).await
                }
                MetadataQuery::LabelNames => {
                    execute_label_names_query(&state, &security, &headers, &params).await
                }
            };
            match result {
                Ok(response) => response,
                Err(error) => error.into_response(),
            }
        }
        Err(error) => error.into_response(),
    };
    state.record_query(metric, response.status(), start);
    response
}
