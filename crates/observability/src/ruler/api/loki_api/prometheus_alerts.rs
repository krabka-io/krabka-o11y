use super::{
    HeaderMap, IntoResponse, PrometheusRulerRequest, QuerierState, RawQuery, RequestSecurity,
    Response, State, json, prometheus_alerts_response, prometheus_ruler_success,
};

pub(crate) async fn prometheus_alerts(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let PrometheusRulerRequest {
        tenant,
        filters: _,
        evaluation_time,
        namespaces,
    } = match super::prometheus_ruler_request(&state, &security, &headers, raw_query.as_deref())
        .await
    {
        Ok(request) => request,
        Err(response) => return *response,
    };
    let alerts = match namespaces {
        Some(namespaces) => {
            match prometheus_alerts_response(&state, &tenant, &namespaces, evaluation_time).await {
                Ok(alerts) => alerts,
                Err(error) => return error.into_response(),
            }
        }
        None => Vec::new(),
    };
    prometheus_ruler_success(&json!({ "alerts": alerts }))
}
