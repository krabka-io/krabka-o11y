use super::{
    MetricStore, PrometheusApiState, PromqlError, RuleRenderOptions, RuleTypeFilter, TenantId,
    TimeExt, Value, json, prometheus_alerts_for_rule_json, rfc3339_time_string, yaml_duration,
    yaml_mapping_json, yaml_optional_string, yaml_string, zero_evaluation_time,
};

pub(crate) async fn prometheus_rule_json<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &TenantId,
    namespace: &str,
    group: &str,
    rule_index: usize,
    rule: &serde_yaml::Value,
    options: RuleRenderOptions,
) -> Result<Option<Value>, PromqlError> {
    let status = state.ruler_rule_status(tenant, namespace, group, rule_index);
    let health = status.as_ref().map_or("ok", |status| {
        if status.last_error.is_empty() {
            "ok"
        } else {
            "err"
        }
    });
    let last_error = status
        .as_ref()
        .map_or_else(String::new, |status| status.last_error.clone());
    let last_evaluation = status.as_ref().map_or_else(
        || zero_evaluation_time().to_string(),
        |status| rfc3339_time_string(status.last_evaluation_ms),
    );
    let evaluation_time = status
        .as_ref()
        .map_or(0.0, |status| status.evaluation_time_seconds);
    if let Some(name) = yaml_optional_string(rule, "record") {
        if options.type_filter == RuleTypeFilter::Alert {
            return Ok(None);
        }
        return Ok(Some(json!({
            "evaluationTime": evaluation_time,
            "health": health,
            "lastError": last_error,
            "lastEvaluation": last_evaluation,
            "name": name,
            "query": yaml_string(rule, "expr"),
            "type": "recording",
        })));
    }
    let Some(name) = yaml_optional_string(rule, "alert") else {
        return Ok(None);
    };
    if options.type_filter == RuleTypeFilter::Record {
        return Ok(None);
    }
    let eval_time_ms = state.ruler_evaluation_time_ms();
    let last_evaluation = status.as_ref().map_or_else(
        || rfc3339_time_string(eval_time_ms),
        |status| rfc3339_time_string(status.last_evaluation_ms),
    );
    let alert_eval = prometheus_alerts_for_rule_json(state, tenant, rule, eval_time_ms).await;
    let (health, last_error, alerts) = match alert_eval {
        Ok(alerts) => (health, last_error, alerts),
        Err(_) if status.is_some() => (health, last_error, Vec::new()),
        Err(error) => ("err", error.to_string(), Vec::new()),
    };
    let mut rule_json = json!({
        "annotations": yaml_mapping_json(rule, "annotations"),
        "duration": yaml_duration(rule, "for").secs_i64(),
        "evaluationTime": evaluation_time,
        "health": health,
        "lastError": last_error,
        "lastEvaluation": last_evaluation,
        "labels": yaml_mapping_json(rule, "labels"),
        "name": name,
        "query": yaml_string(rule, "expr"),
        "type": "alerting",
    });
    if !options.exclude_alerts {
        rule_json["alerts"] = json!(alerts);
    }
    Ok(Some(rule_json))
}
