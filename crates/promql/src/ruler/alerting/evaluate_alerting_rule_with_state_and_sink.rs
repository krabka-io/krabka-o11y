use super::{
    AlertStateKey, AlertmanagerAlert, AlertmanagerSink, BTreeMap, MetricStore, PromqlEngine,
    PromqlError, QueryResult, RecordingRuleWalSink, RulerAlertState, RulerAlertStateRecord,
    RulerStateSink, SamplePayload, SampleValue, TenantId, TimeExt, WalRecord,
    expand_alert_label_map, labels_to_map, yaml_duration, yaml_optional_string,
    yaml_required_string, yaml_string_map,
};

pub(crate) async fn evaluate_alerting_rule_with_state_and_sink<S, W, A, R>(
    engine: &PromqlEngine<S>,
    sinks: (&W, &A, &R),
    state: &mut RulerAlertState,
    tenant: &TenantId,
    rule: &serde_yaml::Value,
    eval_time_ms: i64,
) -> Result<usize, PromqlError>
where
    S: MetricStore,
    W: RecordingRuleWalSink,
    A: AlertmanagerSink,
    R: RulerStateSink,
{
    let (wal_sink, sink, state_sink) = sinks;
    let Some(alert_name) = yaml_optional_string(rule, "alert") else {
        return Ok(0);
    };
    let expr = yaml_required_string(rule, "expr")?;
    let result = engine.query_instant(tenant, &expr, eval_time_ms).await?;
    let QueryResult::InstantVector(samples) = result else {
        return Ok(0);
    };

    let rule_labels = yaml_string_map(rule, "labels");
    let annotations = yaml_string_map(rule, "annotations");
    let external_labels = sink.template_external_labels();
    let external_url = sink.template_external_url(&alert_name);
    let hold_for = yaml_duration(rule, "for")?;
    let keep_firing_for = yaml_duration(rule, "keep_firing_for")?;
    let rule_id = format!("{alert_name}\n{expr}");
    let mut active_keys = Vec::new();
    let mut active_records = Vec::new();
    let mut alerts = Vec::new();
    let mut synthetic_records = Vec::new();
    for sample in samples {
        let SampleValue::Float(value) = sample.value else {
            continue;
        };
        let mut labels = labels_to_map(&sample.labels);
        labels.insert("alertname".to_string(), alert_name.clone());
        labels.extend(rule_labels.clone());
        // Expand `$value`/`$labels` in alert label values against the sample's
        // series labels, matching Prometheus alert templating.
        let labels = expand_alert_label_map(
            &labels,
            value,
            &sample.labels,
            &external_labels,
            &external_url,
        );
        let key = AlertStateKey {
            tenant: tenant.to_string(),
            rule_id: rule_id.clone(),
            labels: labels.clone(),
        };
        let was_active = state.active_since_ms.contains_key(&key);
        let was_firing = state.keep_firing_until_ms.contains_key(&key);
        active_keys.push(key.clone());
        let starts_at_ms = *state
            .active_since_ms
            .entry(key.clone())
            .or_insert(eval_time_ms);
        if eval_time_ms.saturating_sub(starts_at_ms) < hold_for.millis_i64() {
            // Still pending: not firing yet, so it cannot be kept firing.
            state.keep_firing_until_ms.remove(&key);
            active_records.push(RulerAlertStateRecord {
                tenant: tenant.to_string(),
                rule_id: rule_id.clone(),
                labels: labels.clone(),
                active_since_ms: Some(starts_at_ms),
                keep_firing_until_ms: None,
            });
            synthetic_records.extend(active_alert_records(
                tenant,
                &labels,
                "pending",
                starts_at_ms,
                eval_time_ms,
            ));
            continue;
        }
        // Firing: (re)arm the keep-firing deadline so that if the series stops
        // matching on a later tick the alert keeps firing for `keep_firing_for`.
        let keep_firing_until_ms = eval_time_ms.saturating_add(keep_firing_for.millis_i64());
        state.keep_firing_until_ms.insert(key, keep_firing_until_ms);
        active_records.push(RulerAlertStateRecord {
            tenant: tenant.to_string(),
            rule_id: rule_id.clone(),
            labels: labels.clone(),
            active_since_ms: Some(starts_at_ms),
            keep_firing_until_ms: Some(keep_firing_until_ms),
        });
        if was_active && !was_firing {
            synthetic_records.push(alerts_record(
                tenant,
                &labels,
                "pending",
                stale_nan(),
                eval_time_ms,
            ));
        }
        synthetic_records.extend(active_alert_records(
            tenant,
            &labels,
            "firing",
            starts_at_ms,
            eval_time_ms,
        ));
        let annotations = expand_alert_label_map(
            &annotations,
            value,
            &sample.labels,
            &external_labels,
            &external_url,
        );
        alerts.push(AlertmanagerAlert {
            labels,
            annotations,
            starts_at_ms,
            ends_at_ms: None,
            generator_url: external_url.clone(),
        });
    }

    // Reconcile alert instances whose series stopped matching this tick.
    //
    // Prometheus only notifies for instances that previously *fired*: a pending
    // instance that disappears is dropped silently. A previously-firing instance
    // is either kept firing (within its `keep_firing_for` window) or resolved
    // with `EndsAt = eval_time`.
    let cleared_keys = state
        .active_since_ms
        .keys()
        .filter(|key| {
            key.tenant == tenant.as_str() && key.rule_id == rule_id && !active_keys.contains(key)
        })
        .cloned()
        .collect::<Vec<_>>();
    let mut cleared_records = Vec::new();
    let mut kept_firing_keys = Vec::new();
    for key in cleared_keys {
        match state.keep_firing_until_ms.get(&key).copied() {
            // Within the keep-firing window: the alert stays firing. Retain its
            // active state and re-emit it as still-firing (no `EndsAt`).
            Some(until_ms) if until_ms > eval_time_ms => {
                let starts_at_ms = state
                    .active_since_ms
                    .get(&key)
                    .copied()
                    .unwrap_or(eval_time_ms);
                kept_firing_keys.push(key.clone());
                active_records.push(RulerAlertStateRecord {
                    tenant: key.tenant.clone(),
                    rule_id: key.rule_id.clone(),
                    labels: key.labels.clone(),
                    active_since_ms: Some(starts_at_ms),
                    keep_firing_until_ms: Some(until_ms),
                });
                synthetic_records.extend(active_alert_records(
                    tenant,
                    &key.labels,
                    "firing",
                    starts_at_ms,
                    eval_time_ms,
                ));
                alerts.push(AlertmanagerAlert {
                    labels: key.labels.clone(),
                    annotations: BTreeMap::new(),
                    starts_at_ms,
                    ends_at_ms: None,
                    generator_url: external_url.clone(),
                });
            }
            // Had fired and the keep-firing window has elapsed (or was zero):
            // emit a resolved alert and tombstone the instance.
            Some(_) => {
                let starts_at_ms = state
                    .active_since_ms
                    .get(&key)
                    .copied()
                    .unwrap_or(eval_time_ms);
                state.keep_firing_until_ms.remove(&key);
                cleared_records.push(RulerAlertStateRecord {
                    tenant: key.tenant.clone(),
                    rule_id: key.rule_id.clone(),
                    labels: key.labels.clone(),
                    active_since_ms: None,
                    keep_firing_until_ms: None,
                });
                synthetic_records.extend(stale_alert_records(
                    tenant,
                    &key.labels,
                    "firing",
                    eval_time_ms,
                ));
                alerts.push(AlertmanagerAlert {
                    labels: key.labels.clone(),
                    annotations: BTreeMap::new(),
                    starts_at_ms,
                    ends_at_ms: Some(eval_time_ms),
                    generator_url: external_url.clone(),
                });
            }
            // Only ever pending: drop silently, no notification.
            None => {
                cleared_records.push(RulerAlertStateRecord {
                    tenant: key.tenant.clone(),
                    rule_id: key.rule_id.clone(),
                    labels: key.labels.clone(),
                    active_since_ms: None,
                    keep_firing_until_ms: None,
                });
                synthetic_records.extend(stale_alert_records(
                    tenant,
                    &key.labels,
                    "pending",
                    eval_time_ms,
                ));
            }
        }
    }
    for record in active_records.into_iter().chain(cleared_records) {
        state_sink.persist_ruler_alert_state(record).await?;
    }
    for record in synthetic_records {
        wal_sink.append_recording_rule_record(record).await?;
    }
    // Retain the active instances plus any instance still inside its keep-firing
    // window; tombstone everything else for this rule.
    state.active_since_ms.retain(|key, _| {
        key.tenant != tenant.as_str()
            || key.rule_id != rule_id
            || active_keys.contains(key)
            || kept_firing_keys.contains(key)
    });
    let count = alerts.len();
    if count > 0 {
        sink.dispatch_alerts(alerts).await?;
    }
    Ok(count)
}

pub(crate) async fn evaluate_and_persist_alerting_rule_with_state_and_wal<S, W, A, R>(
    engine: &PromqlEngine<S>,
    sinks: (&W, &A, &R),
    state: &mut RulerAlertState,
    tenant: &TenantId,
    rule: &serde_yaml::Value,
    eval_time_ms: i64,
) -> Result<usize, PromqlError>
where
    S: MetricStore,
    W: RecordingRuleWalSink,
    A: AlertmanagerSink,
    R: RulerStateSink,
{
    evaluate_alerting_rule_with_state_and_sink(engine, sinks, state, tenant, rule, eval_time_ms)
        .await
}

fn active_alert_records(
    tenant: &TenantId,
    labels: &BTreeMap<String, String>,
    alert_state: &str,
    starts_at_ms: i64,
    eval_time_ms: i64,
) -> [WalRecord; 2] {
    [
        alerts_record(tenant, labels, alert_state, 1.0, eval_time_ms),
        alert_for_state_record(tenant, labels, starts_at_ms as f64 / 1000.0, eval_time_ms),
    ]
}

fn stale_alert_records(
    tenant: &TenantId,
    labels: &BTreeMap<String, String>,
    alert_state: &str,
    eval_time_ms: i64,
) -> [WalRecord; 2] {
    let stale = stale_nan();
    [
        alerts_record(tenant, labels, alert_state, stale, eval_time_ms),
        alert_for_state_record(tenant, labels, stale, eval_time_ms),
    ]
}

fn alerts_record(
    tenant: &TenantId,
    labels: &BTreeMap<String, String>,
    alert_state: &str,
    value: f64,
    eval_time_ms: i64,
) -> WalRecord {
    let mut labels = labels.clone();
    labels.insert("__name__".into(), "ALERTS".into());
    labels.insert("alertstate".into(), alert_state.into());
    wal_record(tenant, labels, value, eval_time_ms)
}

fn alert_for_state_record(
    tenant: &TenantId,
    labels: &BTreeMap<String, String>,
    value: f64,
    eval_time_ms: i64,
) -> WalRecord {
    let mut labels = labels.clone();
    labels.insert("__name__".into(), "ALERTS_FOR_STATE".into());
    labels.remove("alertstate");
    wal_record(tenant, labels, value, eval_time_ms)
}

fn wal_record(
    tenant: &TenantId,
    labels: BTreeMap<String, String>,
    value: f64,
    eval_time_ms: i64,
) -> WalRecord {
    WalRecord {
        tenant: tenant.to_string(),
        labels: labels.into_iter().collect(),
        payload: SamplePayload::Float {
            timestamp_ms: eval_time_ms,
            value,
            start_timestamp_ms: None,
        },
        exemplars: Vec::new(),
    }
}

fn stale_nan() -> f64 {
    f64::from_bits(0x7ff0_0000_0000_0002)
}
