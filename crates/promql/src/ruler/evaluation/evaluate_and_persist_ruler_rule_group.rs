use super::{
    AlertmanagerSink, MetricStore, PromqlEngine, PromqlError, RecordingRuleWalSink,
    RulerAlertState, RulerEvaluationReport, RulerGroupEvaluation, RulerGroupEvaluationStatus,
    RulerRuleEvaluationStatus, RulerStateSink, TenantId, evaluate_and_append_recording_rule,
    evaluate_and_persist_alerting_rule_with_state_and_wal, yaml_optional_string,
    yaml_required_string, yaml_string_map,
};

/// Evaluates one mixed ruler rule group and persists alert state records.
///
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn evaluate_and_persist_ruler_rule_group<S, W, A, R>(
    engine: &PromqlEngine<S>,
    sinks: (&W, &A, &R),
    alert_state: &mut RulerAlertState,
    tenant: &TenantId,
    group: &serde_yaml::Value,
    eval_time_ms: i64,
) -> Result<RulerGroupEvaluation, PromqlError>
where
    S: MetricStore,
    W: RecordingRuleWalSink,
    A: AlertmanagerSink,
    R: RulerStateSink,
{
    Ok(evaluate_and_persist_ruler_rule_group_with_report(
        engine,
        sinks,
        alert_state,
        tenant,
        ("", "", group),
        eval_time_ms,
    )
    .await?
    .evaluation)
}

pub(super) async fn evaluate_and_persist_ruler_rule_group_with_report<S, W, A, R>(
    engine: &PromqlEngine<S>,
    sinks: (&W, &A, &R),
    alert_state: &mut RulerAlertState,
    tenant: &TenantId,
    group: (&str, &str, &serde_yaml::Value),
    eval_time_ms: i64,
) -> Result<RulerEvaluationReport, PromqlError>
where
    S: MetricStore,
    W: RecordingRuleWalSink,
    A: AlertmanagerSink,
    R: RulerStateSink,
{
    let (namespace, group_name, group) = group;
    let Some(rules) = group.get("rules").and_then(serde_yaml::Value::as_sequence) else {
        return Err(PromqlError::Exec(
            "ruler rule group must contain rules".into(),
        ));
    };
    let (wal_sink, alert_sink, state_sink) = sinks;
    let group_started = std::time::Instant::now();
    let mut report = RulerEvaluationReport::default();
    let mut group_errors = Vec::new();

    for (rule_index, rule) in rules.iter().enumerate() {
        let rule_started = std::time::Instant::now();
        let result = if let Some(record_name) = yaml_optional_string(rule, "record") {
            match yaml_required_string(rule, "expr") {
                Ok(expr) => evaluate_and_append_recording_rule(
                    engine,
                    wal_sink,
                    tenant,
                    &record_name,
                    &expr,
                    &yaml_string_map(rule, "labels"),
                    eval_time_ms,
                )
                .await
                .map(|count| (count, 0)),
                Err(error) => Err(error),
            }
        } else if yaml_optional_string(rule, "alert").is_some() {
            evaluate_and_persist_alerting_rule_with_state_and_wal(
                engine,
                (wal_sink, alert_sink, state_sink),
                alert_state,
                tenant,
                rule,
                eval_time_ms,
            )
            .await
            .map(|count| (0, count))
        } else {
            continue;
        };

        let last_error = match result {
            Ok((recording_records, alerts_dispatched)) => {
                report.evaluation.recording_records += recording_records;
                report.evaluation.alerts_dispatched += alerts_dispatched;
                String::new()
            }
            Err(error) => {
                let error = error.to_string();
                group_errors.push(error.clone());
                error
            }
        };
        report.rules.push(RulerRuleEvaluationStatus {
            tenant: tenant.to_string(),
            namespace: namespace.to_string(),
            group: group_name.to_string(),
            rule_index,
            last_error,
            last_evaluation_ms: eval_time_ms,
            evaluation_time_seconds: rule_started.elapsed().as_secs_f64(),
        });
    }

    report.evaluation.last_eval_ms = eval_time_ms;
    report.groups.push(RulerGroupEvaluationStatus {
        tenant: tenant.to_string(),
        namespace: namespace.to_string(),
        group: group_name.to_string(),
        last_error: group_errors.join("; "),
        last_evaluation_ms: eval_time_ms,
        evaluation_time_seconds: group_started.elapsed().as_secs_f64(),
    });
    Ok(report)
}
