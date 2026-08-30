use super::{
    BTreeMap, MetricStore, PromqlEngine, PromqlError, RecordingRuleWalSink, evaluate_recording_rule,
};

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
    tenant: &str,
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
