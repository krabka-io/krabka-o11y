use super::*;

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
    tenant: &str,
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
