use super::*;

pub(crate) fn retained_prometheus_alerts(
    states: &BTreeMap<PrometheusAlertKey, PrometheusAlertRuntimeState>,
    params: &PrometheusRetainedAlertParams<'_>,
) -> (Vec<Value>, BTreeSet<PrometheusAlertKey>) {
    let mut retained_alerts = Vec::new();
    let mut retained_keys = BTreeSet::new();
    for (key, alert) in states {
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
    (retained_alerts, retained_keys)
}
