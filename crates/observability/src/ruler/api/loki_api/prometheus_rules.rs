use super::{
    HeaderMap, IntoResponse, PrometheusRulerRequest, QuerierState, RawQuery, RequestSecurity,
    Response, State, json, prometheus_rule_groups_response, prometheus_ruler_success,
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
    } = match super::prometheus_ruler_request(&state, &security, &headers, raw_query.as_deref())
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
    prometheus_ruler_success(&data)
}
