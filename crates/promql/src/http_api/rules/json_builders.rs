use super::*;
use crate::ruler::{alert_template_variables, expand_alert_label_map_async};

pub(crate) fn labels_map_json(labels: &crate::PromqlLabels) -> Value {
    let mut values = super::Map::new();
    for pair in labels.iter() {
        values.insert(
            pair.name.clone(),
            Value::String(pair.value.as_str().to_owned()),
        );
    }
    Value::Object(values)
}

struct EvaluatedAlert {
    key: AlertStateKey,
    labels: crate::PromqlLabels,
    value: f64,
    annotations: BTreeMap<String, String>,
}

pub(crate) async fn prometheus_alerts_for_rule_json<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &TenantId,
    rule: &serde_yaml::Value,
    eval_time_ms: i64,
) -> Result<Vec<Value>, PromqlError> {
    let Some(name) = yaml_optional_string(rule, "alert") else {
        return Ok(Vec::new());
    };
    let query = yaml_string(rule, "expr");
    let result = state
        .engine
        .query_instant(tenant, &query, eval_time_ms)
        .await?;
    let QueryResult::InstantVector(samples) = result else {
        return Ok(Vec::new());
    };
    let hold = yaml_duration(rule, "for");
    let rule_id = format!("{name}\n{query}");
    let string_map = |key| {
        let json = yaml_mapping_json(rule, key);
        let object = json.as_object().unwrap();
        let mut result = BTreeMap::new();
        for name in object.keys() {
            let value = object[name].as_str().unwrap().to_owned();
            result.insert(name.clone(), value);
        }
        result
    };
    let rule_labels = string_map("labels");
    let annotations = string_map("annotations");
    let external_labels = krabka_blockstore::Labels::new();
    let mut evaluated = Vec::new();
    let mut active_keys = BTreeSet::new();
    for sample in samples {
        // Alert.Value is smpl.F upstream, zero when a sample contains only H.
        let value = match &sample.value {
            SampleValue::Float(value) => *value,
            SampleValue::Histogram(_) => 0.0,
        };
        let original_labels = sample.labels;
        let mut labels = original_labels.clone();
        labels.remove("__name__");
        let variables = alert_template_variables(
            crate::ruler::template_sample_value(sample.value),
            &original_labels,
            &external_labels,
            "",
        );
        let expanded_labels = expand_alert_label_map_async(
            &state.engine,
            tenant,
            &rule_labels,
            &variables,
            eval_time_ms,
        )
        .await;
        for name in expanded_labels.keys() {
            labels.insert(name.clone(), expanded_labels[name].clone());
        }
        labels.insert("alertname", name.as_str());
        let key = AlertStateKey {
            tenant: tenant.as_str().to_owned(),
            rule_id: rule_id.clone(),
            labels: labels.clone(),
        };
        if active_keys.contains(&key) {
            return Err(PromqlError::Exec(
                "vector contains metrics with the same labelset after applying alert labels".into(),
            ));
        }
        let expanded_annotations = expand_alert_label_map_async(
            &state.engine,
            tenant,
            &annotations,
            &variables,
            eval_time_ms,
        )
        .await;
        active_keys.insert(key.clone());
        let mut annotation_values = BTreeMap::new();
        for annotation_name in expanded_annotations.keys() {
            annotation_values.insert(
                annotation_name.clone(),
                expanded_annotations[annotation_name].as_str().to_owned(),
            );
        }
        evaluated.push(EvaluatedAlert {
            key,
            labels,
            value,
            annotations: annotation_values,
        });
    }

    let mut alert_states = state
        .ruler_alerts
        .write()
        .map_err(|_| PromqlError::Exec("ruler alert state lock poisoned".into()))?;
    alert_states.retain(|key, _| {
        (key.tenant != tenant.as_str() || key.rule_id != rule_id) || active_keys.contains(key)
    });

    let mut alerts = Vec::new();
    for evaluated_alert in evaluated {
        let active_at_ms = *alert_states
            .entry(evaluated_alert.key)
            .or_insert(eval_time_ms);
        let active = Time::from_millis(eval_time_ms.saturating_sub(active_at_ms));
        let alert_state = if hold == Time::ZERO || active >= hold {
            "firing"
        } else {
            "pending"
        };
        let annotations: crate::PromqlLabels = evaluated_alert.annotations.into();
        alerts.push(json!({
            "activeAt": rfc3339_time_string(active_at_ms),
            "annotations": labels_map_json(&annotations),
            "duration": hold.secs_i64(),
            "labels": labels_map_json(&evaluated_alert.labels),
            "name": name,
            "query": query,
            "state": alert_state,
            "value": sample_string(evaluated_alert.value),
        }));
    }
    Ok(alerts)
}

pub(crate) struct PrometheusRuleGroupJson {
    pub(crate) namespace: String,
    pub(crate) stored_group_name: String,
    pub(crate) value: Value,
}

pub(crate) async fn prometheus_rule_groups_json<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &TenantId,
    rules: BTreeMap<String, BTreeMap<String, serde_yaml::Value>>,
    options: RuleRenderOptions,
    rule_names: &BTreeSet<String>,
    rule_groups: &BTreeSet<String>,
    files: &BTreeSet<String>,
) -> Result<Vec<PrometheusRuleGroupJson>, PromqlError> {
    let mut groups = Vec::new();
    for namespace_entry in rules {
        let namespace = namespace_entry.0;
        let namespace_groups = namespace_entry.1;
        if !files.is_empty() && !files.contains(&namespace) {
            continue;
        }
        for group_entry in namespace_groups {
            let stored_group_name = group_entry.0;
            let group = group_entry.1;
            if !rule_groups.is_empty() && !rule_groups.contains(&stored_group_name) {
                continue;
            }
            let rules = prometheus_rules_json(
                state,
                tenant,
                &namespace,
                &stored_group_name,
                &group,
                options,
                rule_names,
            )
            .await?;
            if rules.is_empty() {
                continue;
            }
            let group_name = yaml_string(&group, "name");
            let status = state.ruler_group_status(tenant, &namespace, &stored_group_name);
            let last_evaluation = state
                .ruler_group_last_eval_ms(tenant.as_str(), &namespace, &stored_group_name)
                .map_or_else(|| zero_evaluation_time().to_string(), rfc3339_time_string);
            let value = json!({
                "name": group_name,
                "file": namespace,
                "interval": yaml_duration(&group, "interval").secs_i64(),
                "lastEvaluation": last_evaluation,
                "evaluationTime": status.as_ref().map_or(0.0, |status| status.evaluation_time_seconds),
                "sourceTenants": [],
                "rules": rules,
            });
            groups.push(PrometheusRuleGroupJson {
                namespace: namespace.clone(),
                stored_group_name,
                value,
            });
        }
    }
    Ok(groups)
}

struct AlertEvaluationView {
    health: &'static str,
    last_error: String,
    alerts: Vec<Value>,
}

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
            "labels": yaml_mapping_json(rule, "labels"),
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
    let evaluation = match alert_eval {
        Ok(alerts) => AlertEvaluationView {
            health,
            last_error,
            alerts,
        },
        Err(_) if status.is_some() => AlertEvaluationView {
            health,
            last_error,
            alerts: Vec::new(),
        },
        Err(error) => AlertEvaluationView {
            health: "err",
            last_error: error.to_string(),
            alerts: Vec::new(),
        },
    };
    let mut rule_json = json!({
        "annotations": yaml_mapping_json(rule, "annotations"),
        "duration": yaml_duration(rule, "for").secs_i64(),
        "keepFiringFor": yaml_duration(rule, "keep_firing_for").secs_i64(),
        "evaluationTime": evaluation_time,
        "health": evaluation.health,
        "lastError": evaluation.last_error,
        "lastEvaluation": last_evaluation,
        "labels": yaml_mapping_json(rule, "labels"),
        "name": name,
        "query": yaml_string(rule, "expr"),
        "type": "alerting",
    });
    if !options.exclude_alerts {
        rule_json["alerts"] = json!(evaluation.alerts);
    }
    Ok(Some(rule_json))
}

pub(crate) async fn prometheus_rules_json<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &TenantId,
    namespace: &str,
    group_name: &str,
    group: &serde_yaml::Value,
    options: RuleRenderOptions,
    rule_names: &BTreeSet<String>,
) -> Result<Vec<Value>, PromqlError> {
    let Some(rules) = group.get("rules").and_then(serde_yaml::Value::as_sequence) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for indexed_rule in rules.iter().enumerate() {
        let rule_index = indexed_rule.0;
        let rule = indexed_rule.1;
        if !rule_names.is_empty()
            && !rule
                .get("record")
                .or_else(|| rule.get("alert"))
                .and_then(serde_yaml::Value::as_str)
                .is_some_and(|name| rule_names.contains(name))
        {
            continue;
        }
        if let Some(rule_json) = prometheus_rule_json(
            state, tenant, namespace, group_name, rule_index, rule, options,
        )
        .await?
        {
            out.push(rule_json);
        }
    }
    Ok(out)
}
