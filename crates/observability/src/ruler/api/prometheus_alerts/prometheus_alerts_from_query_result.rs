use super::{
    BTreeMap, BTreeSet, PrometheusAlertKey, PrometheusAlertRuntimeState,
    PrometheusRetainedAlertParams, SharedPrometheusAlertStates, Value,
    expand_prometheus_alert_template, json, prometheus_active_at, prometheus_alert_template_map,
    retained_prometheus_alerts, yaml_duration_ns_field, yaml_string_labels_field,
};

struct PrometheusAlertSample {
    labels: BTreeMap<String, String>,
    value: String,
}

pub(crate) fn prometheus_alerts_from_query_result(
    alert_states: &SharedPrometheusAlertStates,
    tenant: &str,
    alert_name: &str,
    fields: &serde_yaml::Mapping,
    query: &str,
    evaluation_time: i64,
    result: &Value,
) -> Vec<Value> {
    let hold_duration_ns = yaml_duration_ns_field(fields, "for").unwrap_or(0);
    let keep_firing_for_ns = yaml_duration_ns_field(fields, "keep_firing_for").unwrap_or(0);
    let annotation_templates = yaml_string_labels_field(fields, "annotations");
    let rule_label_templates = yaml_string_labels_field(fields, "labels");
    let samples = result
        .pointer("/data/result")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|sample| {
            let value = sample
                .get("value")
                .and_then(Value::as_array)
                .and_then(|value| value.get(1))
                .and_then(Value::as_str)?;
            let mut labels = BTreeMap::new();
            if let Some(metric) = sample.get("metric").and_then(Value::as_object) {
                for entry in metric {
                    if let Some(value) = entry.1.as_str() {
                        labels.insert(entry.0.clone(), value.to_string());
                    }
                }
            }
            for entry in &rule_label_templates {
                labels.insert(
                    entry.0.clone(),
                    expand_prometheus_alert_template(entry.1, &labels, value),
                );
            }
            labels.insert("alertname".to_string(), alert_name.to_string());
            Some(PrometheusAlertSample {
                labels,
                value: value.to_string(),
            })
        })
        .collect::<Vec<_>>();

    let mut states = alert_states
        .alerts
        .lock()
        .expect("Prometheus alert state lock poisoned");
    let mut active_keys = BTreeSet::new();
    let mut alerts = samples
        .into_iter()
        .map(|sample| {
            let key = PrometheusAlertKey {
                tenant: tenant.to_string(),
                alert_name: alert_name.to_string(),
                query: query.to_string(),
                labels: sample.labels.clone(),
            };
            let alert = states
                .entry(key.clone())
                .or_insert_with(|| PrometheusAlertRuntimeState {
                    active_at: evaluation_time,
                    last_active_at: evaluation_time,
                    value: sample.value.clone(),
            });
            alert.last_active_at = evaluation_time;
            alert.value.clone_from(&sample.value);
            let state = if evaluation_time.saturating_sub(alert.active_at) >= hold_duration_ns {
                "firing"
            } else {
                "pending"
            };
            active_keys.insert(key);
            json!({
                "activeAt": prometheus_active_at(alert.active_at),
                "annotations": prometheus_alert_template_map(&annotation_templates, &sample.labels, &sample.value),
                "labels": sample.labels,
                "state": state,
                "value": sample.value,
            })
        })
        .collect::<Vec<_>>();

    let retained = retained_prometheus_alerts(
        &states,
        &PrometheusRetainedAlertParams {
            tenant,
            alert_name,
            query,
            evaluation_time,
            hold_duration_ns,
            keep_firing_for_ns,
            active_keys: &active_keys,
            annotation_templates: &annotation_templates,
        },
    );
    alerts.extend(retained.alerts);

    states.retain(|key, _| {
        key.tenant != tenant
            || key.alert_name != alert_name
            || key.query != query
            || active_keys.contains(key)
            || retained.keys.contains(key)
    });
    alerts
}
