use super::{
    BTreeMap, MetricStore, PromqlEngine, PromqlError, QueryResult, SamplePayload, SampleValue,
    WalRecord, recording_labels,
};

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
    tenant: &str,
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
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
            payload,
            exemplars: Vec::new(),
        });
    }
    Ok(records)
}
