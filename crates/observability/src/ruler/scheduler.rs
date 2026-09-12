use std::collections::{BTreeMap, HashMap};

use prost::Message as _;
use tokio::task::JoinHandle;

use crate::ruler::api::prometheus_rules::{
    loki_yaml_mapping, prometheus_alerts_for_rule, prometheus_rule_group_interval_seconds,
    serde_yaml_key, yaml_string_field,
};
use crate::{
    CancellationToken, LokiStreamEncoding, QuerierState, QueryKind, QueryParams, TenantId,
    current_unix_time_ns, execute_http_query_for_tenant,
};

pub(crate) fn spawn_logs_ruler(state: QuerierState, shutdown: CancellationToken) -> JoinHandle<()> {
    tokio::spawn(async move {
        let alertmanager = std::env::var("KRABKA_OBSERVABILITY_ALERTMANAGER_URL").ok();
        let remote_write = std::env::var("KRABKA_OBSERVABILITY_RULER_REMOTE_WRITE_URL").ok();
        let client = reqwest::Client::new();
        let mut last = HashMap::<(String, String, String), i64>::new();
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                () = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
            }
            let now = current_unix_time_ns();
            let rules = state
                .rules
                .tenants
                .lock()
                .expect("Loki rule store lock poisoned")
                .clone();
            for (tenant, namespaces) in rules {
                let Ok(tenant_id) = TenantId::new(&tenant) else {
                    continue;
                };
                for (namespace, groups) in namespaces {
                    for (group_name, group) in groups {
                        let interval = prometheus_rule_group_interval_seconds(&group)
                            .max(1)
                            .saturating_mul(1_000_000_000);
                        let key = (tenant.clone(), namespace.clone(), group_name);
                        if !evaluation_due(last.get(&key).copied(), now, interval) {
                            continue;
                        }
                        last.insert(key, now);
                        let Some(rules) = loki_yaml_mapping(&group)
                            .and_then(|fields| fields.get(serde_yaml_key("rules")))
                            .and_then(serde_yaml::Value::as_sequence)
                        else {
                            continue;
                        };
                        for rule in rules {
                            let Some(fields) = loki_yaml_mapping(rule) else {
                                continue;
                            };
                            if yaml_string_field(fields, "alert").is_some() {
                                if let Ok(alerts) =
                                    prometheus_alerts_for_rule(&state, &tenant_id, rule, now).await
                                    && let Some(url) = &alertmanager
                                {
                                    let firing = alerts
                                        .into_iter()
                                        .filter(|alert| alert["state"] == "firing")
                                        .map(|alert| {
                                            serde_json::json!({
                                                "labels": alert["labels"],
                                                "annotations": alert["annotations"],
                                                "startsAt": alert["activeAt"],
                                            })
                                        })
                                        .collect::<Vec<_>>();
                                    if !firing.is_empty()
                                        && let Err(error) = client
                                            .post(format!(
                                                "{}/api/v2/alerts",
                                                url.trim_end_matches('/')
                                            ))
                                            .json(&firing)
                                            .send()
                                            .await
                                    {
                                        tracing::warn!(%error, "logs ruler Alertmanager dispatch failed");
                                    }
                                }
                            } else if let Some(url) = &remote_write
                                && let Err(error) = evaluate_recording_rule(
                                    &state, &tenant_id, rule, now, url, &client,
                                )
                                .await
                            {
                                tracing::warn!(%error, "logs recording rule remote write failed");
                            }
                        }
                    }
                }
            }
        }
    })
}

fn evaluation_due(last: Option<i64>, now: i64, interval: i64) -> bool {
    last.is_none_or(|last| now.saturating_sub(last) >= interval)
}

#[cfg(test)]
mod tests {
    use super::evaluation_due;

    #[test]
    fn rule_group_becomes_due_at_its_interval_boundary() {
        assert!(evaluation_due(None, 10, 5));
        assert!(!evaluation_due(Some(10), 14, 5));
        assert!(evaluation_due(Some(10), 15, 5));
    }
}

async fn evaluate_recording_rule(
    state: &QuerierState,
    tenant: &TenantId,
    rule: &serde_yaml::Value,
    now: i64,
    url: &str,
    client: &reqwest::Client,
) -> Result<(), String> {
    let fields = loki_yaml_mapping(rule).ok_or("recording rule is not an object")?;
    let record = yaml_string_field(fields, "record").ok_or("recording rule has no record name")?;
    let expression = yaml_string_field(fields, "expr").ok_or("recording rule has no expression")?;
    let result = execute_http_query_for_tenant(
        state,
        tenant,
        &QueryParams {
            query: expression.to_string(),
            time: Some(now),
            start: None,
            end: None,
            since: None,
            step: None,
            interval: None,
            limit: None,
            direction: None,
            delay_for: None,
        },
        QueryKind::Instant,
        LokiStreamEncoding::Folded,
    )
    .await
    .map_err(|error| error.to_string())?;
    let mut timeseries = Vec::new();
    for series in result
        .pointer("/data/result")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(sample) = series.get("value").and_then(serde_json::Value::as_array) else {
            continue;
        };
        let Some(value) = sample
            .get(1)
            .and_then(serde_json::Value::as_str)
            .and_then(|value| value.parse().ok())
        else {
            continue;
        };
        let mut labels = BTreeMap::from([("__name__".to_string(), record.to_string())]);
        if let Some(metric) = series.get("metric").and_then(serde_json::Value::as_object) {
            labels.extend(metric.iter().filter_map(|(name, value)| {
                value
                    .as_str()
                    .map(|value| (name.clone(), value.to_string()))
            }));
        }
        if let Some(configured) = fields
            .get(serde_yaml_key("labels"))
            .and_then(serde_yaml::Value::as_mapping)
        {
            labels.extend(configured.iter().filter_map(|(name, value)| {
                Some((name.as_str()?.to_string(), value.as_str()?.to_string()))
            }));
        }
        timeseries.push(TimeSeries {
            labels: labels
                .into_iter()
                .map(|(name, value)| Label { name, value })
                .collect(),
            samples: vec![Sample {
                value,
                timestamp: now / 1_000_000,
            }],
        });
    }
    if timeseries.is_empty() {
        return Ok(());
    }
    let compressed = snap::raw::Encoder::new()
        .compress_vec(&WriteRequest { timeseries }.encode_to_vec())
        .map_err(|error| error.to_string())?;
    client
        .post(url)
        .header("content-type", "application/x-protobuf")
        .header("content-encoding", "snappy")
        .header("x-prometheus-remote-write-version", "0.1.0")
        .header(krabka_blockstore::TENANT_HEADER, tenant.as_str())
        .body(compressed)
        .send()
        .await
        .map_err(|error| error.to_string())?
        .error_for_status()
        .map_err(|error| error.to_string())?;
    Ok(())
}

#[derive(Clone, PartialEq, prost::Message)]
struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    timeseries: Vec<TimeSeries>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    samples: Vec<Sample>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct Label {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(string, tag = "2")]
    value: String,
}

#[derive(Clone, Copy, PartialEq, prost::Message)]
struct Sample {
    #[prost(double, tag = "1")]
    value: f64,
    #[prost(int64, tag = "2")]
    timestamp: i64,
}
