use super::{
    AlertStateKey, BTreeMap, BTreeSet, MetricStore, PrometheusApiState, PromqlError, QueryResult,
    SampleValue, TenantId, Time, TimeExt, Value, json, labels_map_json, rfc3339_time_string,
    sample_string, yaml_duration, yaml_mapping_json, yaml_optional_string, yaml_string,
};
use crate::ruler::{alert_template_variables, expand_alert_label_map_async};

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
