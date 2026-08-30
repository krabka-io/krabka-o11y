use super::{
    HttpQueryError, QuerierState, QueryKind, QueryParams, Value, execute_http_query_for_tenant,
    loki_yaml_mapping, prometheus_alerts_from_query_result, yaml_string_field,
};

pub(crate) async fn prometheus_alerts_for_rule(
    state: &QuerierState,
    tenant: &str,
    rule: &serde_yaml::Value,
    evaluation_time: i64,
) -> Result<Vec<Value>, HttpQueryError> {
    let Some(fields) = loki_yaml_mapping(rule) else {
        return Ok(Vec::new());
    };
    let Some(alert_name) = yaml_string_field(fields, "alert") else {
        return Ok(Vec::new());
    };
    let Some(query) = yaml_string_field(fields, "expr") else {
        return Ok(Vec::new());
    };
    let params = QueryParams {
        query: query.to_string(),
        time: Some(evaluation_time),
        start: None,
        end: None,
        since: None,
        step: None,
        interval: None,
        limit: None,
        direction: None,
        delay_for: None,
    };
    let result = execute_http_query_for_tenant(state, tenant, &params, QueryKind::Instant).await?;
    Ok(prometheus_alerts_from_query_result(
        &state.alert_states,
        tenant,
        alert_name,
        fields,
        query,
        evaluation_time,
        &result,
    ))
}
