use super::{
    expand_prometheus_alert_template, prometheus_alert_template_map, yaml_duration_ns_field,
    yaml_string_labels_field,
};
use crate::{
    BTreeMap, BTreeSet, Labels, OffsetDateTime, PrometheusAlertKey, PrometheusAlertRuntimeState,
    Rfc3339, SharedPrometheusAlertStates, Value, json,
};

pub(crate) fn prometheus_active_at(timestamp_ns: i64) -> String {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(timestamp_ns))
        .ok()
        .and_then(|timestamp| timestamp.format(&Rfc3339).ok())
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string())
}

pub(crate) fn prometheus_alert_key_matches_rule(
    key: &PrometheusAlertKey,
    params: &PrometheusRetainedAlertParams<'_>,
) -> bool {
    key.tenant == params.tenant
        && key.alert_name == params.alert_name
        && key.query == params.query
        && !params.active_keys.contains(key)
}

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

pub(crate) struct PrometheusRetainedAlertParams<'a> {
    pub(crate) tenant: &'a str,
    pub(crate) alert_name: &'a str,
    pub(crate) query: &'a str,
    pub(crate) evaluation_time: i64,
    pub(crate) hold_duration_ns: i64,
    pub(crate) keep_firing_for_ns: i64,
    pub(crate) active_keys: &'a BTreeSet<PrometheusAlertKey>,
    pub(crate) annotation_templates: &'a Labels,
}

pub(crate) struct RetainedPrometheusAlerts {
    pub(crate) alerts: Vec<Value>,
    pub(crate) keys: BTreeSet<PrometheusAlertKey>,
}

pub(crate) fn retained_prometheus_alerts(
    states: &BTreeMap<PrometheusAlertKey, PrometheusAlertRuntimeState>,
    params: &PrometheusRetainedAlertParams<'_>,
) -> RetainedPrometheusAlerts {
    let mut retained_alerts = Vec::new();
    let mut retained_keys = BTreeSet::new();
    for entry in states {
        let key = entry.0;
        let alert = entry.1;
        if !prometheus_alert_key_matches_rule(key, params) {
            continue;
        }
        let was_firing =
            alert.last_active_at.saturating_sub(alert.active_at) >= params.hold_duration_ns;
        let within_keep_firing = params.evaluation_time.saturating_sub(alert.last_active_at)
            <= params.keep_firing_for_ns;
        if was_firing && within_keep_firing {
            retained_keys.insert(key.clone());
            retained_alerts.push(json!({
                "activeAt": prometheus_active_at(alert.active_at),
                "annotations": prometheus_alert_template_map(
                    params.annotation_templates,
                    &key.labels,
                    &alert.value,
                ),
                "labels": key.labels,
                "state": "firing",
                "value": alert.value,
            }));
        }
    }
    RetainedPrometheusAlerts {
        alerts: retained_alerts,
        keys: retained_keys,
    }
}
