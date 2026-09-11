use super::{
    HeaderMap, IntoResponse, PrometheusRulesFilters, QuerierState, RawQuery, RequestSecurity,
    Response, State, StatusCode, TenantErrorSurface, authorized_ruler_tenant, current_unix_time_ns,
    json, json_response, prometheus_rule_groups_response,
};

pub(crate) async fn prometheus_rules(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let tenant = match authorized_ruler_tenant(
        &state,
        &security,
        &headers,
        TenantErrorSurface::PrometheusRuler,
    )
    .await
    {
        Ok(tenant) => tenant,
        Err(response) => return response,
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
        .get(tenant.as_str())
        .cloned();
    let page = match namespaces {
        Some(namespaces) => {
            match prometheus_rule_groups_response(
                &state,
                &tenant,
                &namespaces,
                &filters,
                evaluation_time,
            )
            .await
            {
                Ok(page) => page,
                Err(error) => return error.into_response(),
            }
        }
        None => match filters.page_groups(Vec::new()) {
            Ok(page) => page,
            Err(error) => return error.into_response(),
        },
    };
    let mut data = json!({
        "groups": page.groups
    });
    if let Some(token) = page.next_token {
        data["groupNextToken"] = json!(token);
    }
    json_response(
        StatusCode::OK,
        &json!({
            "status": "success",
            "data": data,
            "errorType": "",
            "error": "",
        }),
    )
}
