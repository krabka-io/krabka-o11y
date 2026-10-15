use super::*;

/// Evaluates one recording rule and materializes the result as metrics WAL
/// records.
///
/// This function rewrites `__name__` to `record_name` on each output series and
/// merges the rule-level `labels` on top, so the rule labels win. This matches
/// Prometheus. If two output samples collapse to the same label set after that
/// rewrite, the rule fails and writes no duplicate WAL records. Prometheus
/// rejects this case as "vector contains metrics with the same labelset after
/// applying rule labels".
///
/// # Errors
///
/// Returns an error when metric input is malformed. Returns an error when a
/// limit is exceeded. Returns an error when the backing WAL, the block store, or
/// a remote endpoint fails.
pub async fn evaluate_recording_rule<S: MetricStore>(
    engine: &PromqlEngine<S>,
    tenant: &TenantId,
    record_name: &str,
    expr: &str,
    rule_labels: &BTreeMap<String, String>,
    eval_time_ms: i64,
) -> Result<Vec<WalRecord>, PromqlError> {
    let result = engine.query_instant(tenant, expr, eval_time_ms).await?;
    let QueryResult::InstantVector(samples) = result else {
        return Err(PromqlError::Exec(
            "recording rule expression must evaluate to an instant vector".into(),
        ));
    };

    let mut seen_fingerprints = std::collections::BTreeSet::new();
    let mut records = Vec::with_capacity(samples.len());
    for sample in samples {
        let labels = recording_labels(sample.labels, record_name, rule_labels);
        if !seen_fingerprints.insert(labels.fingerprint()) {
            return Err(PromqlError::Exec(
                "vector contains metrics with the same labelset after applying rule labels".into(),
            ));
        }
        let payload = match sample.value {
            SampleValue::Float(value) => SamplePayload::Float {
                timestamp_ms: sample.ts_ms,
                value,
                start_timestamp_ms: None,
            },
            SampleValue::Histogram(hist) => SamplePayload::Hist {
                timestamp_ms: sample.ts_ms,
                hist,
            },
        };
        records.push(WalRecord {
            tenant: tenant.to_string(),
            labels: labels
                .iter()
                .map(|pair| {
                    krabka_metrics::MetricLabelPair::new(pair.name.clone(), pair.value.clone())
                })
                .collect(),
            payload,
            exemplars: Vec::new(),
        });
    }
    Ok(records)
}

/// Evaluates one recording rule and appends its materialized samples to the WAL
/// sink.
///
/// # Errors
///
/// Returns an error when metric input is malformed. Returns an error when a
/// limit is exceeded. Returns an error when the backing WAL, the block store, or
/// a remote endpoint fails.
pub async fn evaluate_and_append_recording_rule<S, W>(
    engine: &PromqlEngine<S>,
    sink: &W,
    tenant: &TenantId,
    record_name: &str,
    expr: &str,
    rule_labels: &BTreeMap<String, String>,
    eval_time_ms: i64,
) -> Result<usize, PromqlError>
where
    S: MetricStore,
    W: RecordingRuleWalSink,
{
    let records =
        evaluate_recording_rule(engine, tenant, record_name, expr, rule_labels, eval_time_ms)
            .await?;
    let count = records.len();
    for record in records {
        sink.append_recording_rule_record(record).await?;
    }
    Ok(count)
}

/// Evaluates all recording rules in one rule group and appends their outputs.
///
/// # Errors
///
/// Returns an error when metric input is malformed. Returns an error when a
/// limit is exceeded. Returns an error when the backing WAL, the block store, or
/// a remote endpoint fails.
pub async fn evaluate_and_append_recording_rule_group<S, W>(
    engine: &PromqlEngine<S>,
    sink: &W,
    tenant: &TenantId,
    group: &serde_yaml::Value,
    eval_time_ms: i64,
) -> Result<usize, PromqlError>
where
    S: MetricStore,
    W: RecordingRuleWalSink,
{
    let Some(rules) = group.get("rules").and_then(serde_yaml::Value::as_sequence) else {
        return Err(PromqlError::Exec(
            "recording rule group must contain rules".into(),
        ));
    };

    let mut appended = 0;
    for rule in rules {
        let Some(record_name) = yaml_optional_string(rule, "record") else {
            continue;
        };
        let expr = yaml_required_string(rule, "expr")?;
        let rule_labels = yaml_string_map(rule, "labels");
        appended += evaluate_and_append_recording_rule(
            engine,
            sink,
            tenant,
            &record_name,
            &expr,
            &rule_labels,
            eval_time_ms,
        )
        .await?;
    }
    Ok(appended)
}
