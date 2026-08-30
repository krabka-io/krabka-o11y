use super::{json, MetricStore, PrometheusApiState, RuleRenderOptions, Value, PromqlError, yaml_optional_string, RuleTypeFilter, zero_evaluation_time, yaml_string, prometheus_alerts_for_rule_json, yaml_mapping_json, TimeExt, yaml_duration, rfc3339_time_string};

pub(crate) async fn prometheus_rule_json<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    rule: &serde_yaml::Value,
    options: RuleRenderOptions,
) -> Result<Option<Value>, PromqlError> {
    if let Some(name) = yaml_optional_string(rule, "record") {
        if options.type_filter == RuleTypeFilter::Alert {
            return Ok(None);
        }
        return Ok(Some(json!({
            "evaluationTime": 0.0,
            "health": "ok",
            "lastError": "",
            "lastEvaluation": zero_evaluation_time(),
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
    let alert_eval = prometheus_alerts_for_rule_json(state, tenant, rule, eval_time_ms).await;
    let (health, last_error, alerts) = match alert_eval {
        Ok(alerts) => ("ok", String::new(), alerts),
        Err(error) => ("err", error.to_string(), Vec::new()),
    };
    let mut rule_json = json!({
        "annotations": yaml_mapping_json(rule, "annotations"),
        "duration": yaml_duration(rule, "for").secs_i64(),
        "evaluationTime": 0.0,
        "health": health,
        "lastError": last_error,
        "lastEvaluation": rfc3339_time_string(eval_time_ms),
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
