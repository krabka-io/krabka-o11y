use super::{
    HeaderMap, HttpQueryError, QuerierState, Response, SeriesParams, StatusCode, json,
    json_response, label_names_data,
};

pub(crate) async fn execute_api_prom_label_names_query(
    state: &QuerierState,
    headers: &HeaderMap,
    params: &SeriesParams,
) -> Result<Response, HttpQueryError> {
    let values = label_names_data(state, headers, params).await?;
    Ok(if values.is_empty() {
        json_response(StatusCode::OK, &json!({}))
    } else {
        json_response(
            StatusCode::OK,
            &json!({
                "values": values,
            }),
        )
    })
}
