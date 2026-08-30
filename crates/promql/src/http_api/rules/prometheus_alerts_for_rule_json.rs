use super::*;

pub(crate) async fn prometheus_alerts_for_rule_json<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
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
    let mut evaluated = Vec::new();
    let mut active_keys = BTreeSet::new();
    for sample in samples {
        let SampleValue::Float(value) = sample.value else {
            continue;
        };
        let labels = alert_labels_map(&sample.labels, rule, &name);
        let key = AlertStateKey {
            tenant: tenant.to_string(),
            rule_id: rule_id.clone(),
            labels: labels.clone(),
        };
        active_keys.insert(key.clone());
        evaluated.push((key, labels, value));
    }

    let mut alert_states = state
        .ruler_alerts
        .write()
        .map_err(|_| PromqlError::Exec("ruler alert state lock poisoned".into()))?;
    alert_states.retain(|key, _| {
        (key.tenant != tenant || key.rule_id != rule_id) || active_keys.contains(key)
    });

    let mut alerts = Vec::new();
    for (key, labels, value) in evaluated {
        let active_at_ms = *alert_states.entry(key).or_insert(eval_time_ms);
        let active = Time::from_millis(eval_time_ms.saturating_sub(active_at_ms));
        let alert_state = if hold == Time::ZERO || active >= hold {
            "firing"
        } else {
            "pending"
        };
        let template_labels = labels_from_map(&labels);
        let annotations = expand_alert_mapping_json(
            &yaml_mapping_json(rule, "annotations"),
            value,
            &template_labels,
        );
        let expanded_labels = labels
            .into_iter()
            .map(|(name, label_value)| {
                let expanded = expand_alert_template(&label_value, value, &template_labels);
                (name, expanded)
            })
            .collect::<BTreeMap<_, _>>();
        alerts.push(json!({
            "activeAt": rfc3339_time_string(active_at_ms),
            "annotations": annotations,
            "duration": hold.secs_i64(),
            "labels": labels_map_json(expanded_labels),
            "name": name,
            "query": query,
            "state": alert_state,
            "value": sample_string(value),
        }));
    }
    Ok(alerts)
}
