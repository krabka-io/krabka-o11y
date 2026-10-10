use super::{
    HeaderMap, IntoResponse, PrometheusRulerInputs, PrometheusRulerRequest, QuerierState, RawQuery,
    RequestSecurity, Response, State, StatusCode, json, json_response,
    prometheus_rule_groups_response,
};

pub(crate) async fn prometheus_rules(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let PrometheusRulerRequest {
        tenant,
        filters,
        evaluation_time,
        namespaces,
    } = match (PrometheusRulerInputs {
        state: &state,
        security: &security,
        headers: &headers,
        raw_query: raw_query.as_deref(),
    })
    .resolve()
    .await
    {
        Ok(request) => request,
        Err(response) => return *response,
    };
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
