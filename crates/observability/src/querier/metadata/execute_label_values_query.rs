use super::{
    HeaderMap, HttpQueryError, QuerierState, RequestSecurity, Response, SeriesParams,
    label_values_data, loki_sparse_success, loki_success, series_data,
};

/// One `/label/{name}/values` request.
#[derive(Clone, Copy)]
pub(crate) struct LabelValuesRequest<'a> {
    pub(crate) security: &'a RequestSecurity,
    pub(crate) headers: &'a HeaderMap,
    /// The label whose values are listed.
    pub(crate) label_name: &'a str,
    /// The series selector and time range the values come from.
    pub(crate) series_params: &'a SeriesParams,
}

pub(crate) async fn execute_label_values_query(
    state: &QuerierState,
    request: LabelValuesRequest<'_>,
) -> Result<Response, HttpQueryError> {
    let LabelValuesRequest {
        security,
        headers,
        label_name,
        series_params,
    } = request;
    let data = label_values_data(
        series_data(state, security, headers, series_params).await?,
        label_name,
    );
    Ok(if data.is_empty() {
        loki_sparse_success()
    } else {
        loki_success(data)
    })
}
