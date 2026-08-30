use super::unix_ms_to_rfc3339;

pub(crate) fn alertmanager_payload(
    alerts: Vec<krabka_promql::AlertmanagerAlert>,
) -> serde_json::Value {
    serde_json::Value::Array(
        alerts
            .into_iter()
            .map(|alert| {
                serde_json::json!({
                    "labels": alert.labels,
                    "annotations": alert.annotations,
                    "startsAt": unix_ms_to_rfc3339(alert.starts_at_ms),
                    "endsAt": alert.ends_at_ms.map(unix_ms_to_rfc3339),
                    "generatorURL": alert.generator_url,
                })
            })
            .collect(),
    )
}
