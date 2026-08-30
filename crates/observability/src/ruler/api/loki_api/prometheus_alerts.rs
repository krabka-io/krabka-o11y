use super::{
    HeaderMap, IntoResponse, PrometheusRulesFilters, QuerierState, RawQuery, Response, State,
    StatusCode, current_unix_time_ns, json, json_response, loki_ruler_tenant,
    prometheus_alerts_response,
};

pub(crate) async fn prometheus_alerts(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let tenant = match loki_ruler_tenant(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let filters = match PrometheusRulesFilters::parse(raw_query.as_deref()) {
        Ok(filters) => filters,
        Err(error) => return error.into_response(),
    };
    let evaluation_time = filters.evaluation_time.unwrap_or_else(current_unix_time_ns);
    let namespaces = state
        .rules
        .tenants
        .lock()
        .expect("Loki rule store lock poisoned")
        .get(&tenant)
        .cloned();
    let alerts = match namespaces {
        Some(namespaces) => {
            match prometheus_alerts_response(&state, &tenant, &namespaces, evaluation_time).await {
                Ok(alerts) => alerts,
                Err(error) => return error.into_response(),
            }
        }
        None => Vec::new(),
    };
    json_response(
        StatusCode::OK,
        &json!({
            "status": "success",
            "data": {
                "alerts": alerts
            },
            "errorType": "",
            "error": "",
        }),
    )
}
