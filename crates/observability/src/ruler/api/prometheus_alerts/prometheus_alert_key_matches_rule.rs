use super::*;

pub(crate) fn prometheus_alert_key_matches_rule(
    key: &PrometheusAlertKey,
    params: &PrometheusRetainedAlertParams<'_>,
) -> bool {
    key.tenant == params.tenant
        && key.alert_name == params.alert_name
        && key.query == params.query
        && !params.active_keys.contains(key)
}
