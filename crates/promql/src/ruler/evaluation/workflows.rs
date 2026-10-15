use super::*;

/// Evaluates one mixed ruler rule group: recording outputs, then alert dispatch.
///
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn evaluate_ruler_rule_group<S, W, A>(
    engine: &PromqlEngine<S>,
    wal_sink: &W,
    alert_sink: &A,
    alert_state: &mut RulerAlertState,
    tenant: &TenantId,
    group: &serde_yaml::Value,
    eval_time_ms: i64,
) -> Result<RulerGroupEvaluation, PromqlError>
where
    S: MetricStore,
    W: RecordingRuleWalSink,
    A: AlertmanagerSink,
{
    let recording_records =
        evaluate_and_append_recording_rule_group(engine, wal_sink, tenant, group, eval_time_ms)
            .await?;
    let alerts_dispatched = evaluate_and_dispatch_alerting_rule_group(
        engine,
        alert_sink,
        alert_state,
        tenant,
        group,
        eval_time_ms,
    )
    .await?;
    Ok(RulerGroupEvaluation {
        recording_records,
        alerts_dispatched,
        last_eval_ms: eval_time_ms,
    })
}

/// Evaluates all ruler rule groups for one tenant.
///
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn evaluate_ruler_rule_set<S, W, A>(
    engine: &PromqlEngine<S>,
    wal_sink: &W,
    alert_sink: &A,
    alert_state: &mut RulerAlertState,
    tenant: &TenantId,
    rules: &BTreeMap<String, BTreeMap<String, serde_yaml::Value>>,
    eval_time_ms: i64,
) -> Result<RulerGroupEvaluation, PromqlError>
where
    S: MetricStore,
    W: RecordingRuleWalSink,
    A: AlertmanagerSink,
{
    let mut total = RulerGroupEvaluation::default();
    for namespace_groups in rules.values() {
        for group in namespace_groups.values() {
            let evaluation = evaluate_ruler_rule_group(
                engine,
                wal_sink,
                alert_sink,
                alert_state,
                tenant,
                group,
                eval_time_ms,
            )
            .await?;
            total.recording_records += evaluation.recording_records;
            total.alerts_dispatched += evaluation.alerts_dispatched;
            total.last_eval_ms = evaluation.last_eval_ms;
        }
    }
    Ok(total)
}

struct RuleEvaluationCounts {
    recording_records: usize,
    alerts_dispatched: usize,
}

/// Evaluates one mixed ruler rule group and persists alert state records.
///
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn evaluate_and_persist_ruler_rule_group<S, W, A, R>(
    engine: &PromqlEngine<S>,
    sinks: RulerSinks<'_, W, A, R>,
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
        RulerGroupRef {
            namespace: "",
            group_name: "",
            group,
        },
        eval_time_ms,
    )
    .await?
    .evaluation)
}

pub(super) async fn evaluate_and_persist_ruler_rule_group_with_report<S, W, A, R>(
    engine: &PromqlEngine<S>,
    sinks: RulerSinks<'_, W, A, R>,
    alert_state: &mut RulerAlertState,
    tenant: &TenantId,
    group: RulerGroupRef<'_>,
    eval_time_ms: i64,
) -> Result<RulerEvaluationReport, PromqlError>
where
    S: MetricStore,
    W: RecordingRuleWalSink,
    A: AlertmanagerSink,
    R: RulerStateSink,
{
    let namespace = group.namespace;
    let group_name = group.group_name;
    let group = group.group;
    let Some(rules) = group.get("rules").and_then(serde_yaml::Value::as_sequence) else {
        return Err(PromqlError::Exec(
            "ruler rule group must contain rules".into(),
        ));
    };
    let wal_sink = sinks.wal;
    let alert_sink = sinks.alertmanager;
    let state_sink = sinks.ruler_state;
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
                .map(|count| RuleEvaluationCounts {
                    recording_records: count,
                    alerts_dispatched: 0,
                }),
                Err(error) => Err(error),
            }
        } else if yaml_optional_string(rule, "alert").is_some() {
            evaluate_alerting_rule_with_state_and_sink(
                engine,
                RulerSinks::new(wal_sink, alert_sink, state_sink),
                alert_state,
                tenant,
                rule,
                eval_time_ms,
            )
            .await
            .map(|count| RuleEvaluationCounts {
                recording_records: 0,
                alerts_dispatched: count,
            })
        } else {
            continue;
        };

        let last_error = match result {
            Ok(counts) => {
                report.evaluation.recording_records += counts.recording_records;
                report.evaluation.alerts_dispatched += counts.alerts_dispatched;
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

pub type RulerRuleSet = BTreeMap<String, BTreeMap<String, serde_yaml::Value>>;

pub struct RulerRuleSetEvaluationRequest<'a, S: MetricStore, W, A, R> {
    pub engine: &'a PromqlEngine<S>,
    pub sinks: RulerSinks<'a, W, A, R>,
    pub alert_state: &'a mut RulerAlertState,
    pub tenant: &'a TenantId,
    pub rules: &'a RulerRuleSet,
    pub eval_time_ms: i64,
}

impl<'a, S: MetricStore, W, A, R> RulerRuleSetEvaluationRequest<'a, S, W, A, R> {
    pub fn new(
        engine: &'a PromqlEngine<S>,
        sinks: RulerSinks<'a, W, A, R>,
        alert_state: &'a mut RulerAlertState,
        tenant: &'a TenantId,
        rules: &'a RulerRuleSet,
        eval_time_ms: i64,
    ) -> Self {
        Self {
            engine,
            sinks,
            alert_state,
            tenant,
            rules,
            eval_time_ms,
        }
    }
}

/// Evaluates all ruler rule groups for one tenant and persists compactable group state.
///
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn evaluate_and_persist_ruler_rule_set<S, W, A, R>(
    request: RulerRuleSetEvaluationRequest<'_, S, W, A, R>,
) -> Result<RulerGroupEvaluation, PromqlError>
where
    S: MetricStore,
    W: RecordingRuleWalSink,
    A: AlertmanagerSink,
    R: RulerStateSink,
{
    Ok(evaluate_and_persist_ruler_rule_set_with_report(request)
        .await?
        .evaluation)
}

/// Evaluates every rule independently and returns detailed health information.
///
/// A bad rule or failed group-state write is recorded in the report and does
/// not prevent later rules or groups from running.
///
/// # Errors
/// Returns an error when metric input is malformed or a limit is exceeded.
pub async fn evaluate_and_persist_ruler_rule_set_with_report<S, W, A, R>(
    request: RulerRuleSetEvaluationRequest<'_, S, W, A, R>,
) -> Result<RulerEvaluationReport, PromqlError>
where
    S: MetricStore,
    W: RecordingRuleWalSink,
    A: AlertmanagerSink,
    R: RulerStateSink,
{
    let RulerRuleSetEvaluationRequest {
        engine,
        sinks,
        alert_state,
        tenant,
        rules,
        eval_time_ms,
    } = request;
    let wal_sink = sinks.wal;
    let alert_sink = sinks.alertmanager;
    let state_sink = sinks.ruler_state;
    let mut total = RulerEvaluationReport::default();
    for namespace_entry in rules {
        let namespace = namespace_entry.0;
        let namespace_groups = namespace_entry.1;
        for group_entry in namespace_groups {
            let group_name = group_entry.0;
            let group = group_entry.1;
            let group_report = evaluate_and_persist_ruler_rule_group_with_report(
                engine,
                RulerSinks::new(wal_sink, alert_sink, state_sink),
                alert_state,
                tenant,
                RulerGroupRef {
                    namespace,
                    group_name,
                    group,
                },
                eval_time_ms,
            )
            .await;
            let mut group_report = match group_report {
                Ok(report) => report,
                Err(error) => {
                    let error = error.to_string();
                    RulerEvaluationReport {
                        evaluation: RulerGroupEvaluation {
                            last_eval_ms: eval_time_ms,
                            ..RulerGroupEvaluation::default()
                        },
                        groups: vec![super::RulerGroupEvaluationStatus {
                            tenant: tenant.to_string(),
                            namespace: namespace.clone(),
                            group: group_name.clone(),
                            last_error: error,
                            last_evaluation_ms: eval_time_ms,
                            evaluation_time_seconds: 0.0,
                        }],
                        rules: Vec::new(),
                    }
                }
            };
            if let Err(error) = state_sink
                .persist_ruler_group_state(RulerGroupStateRecord {
                    tenant: tenant.to_string(),
                    namespace: namespace.clone(),
                    group: group_name.clone(),
                    last_eval_ms: group_report.evaluation.last_eval_ms,
                })
                .await
            {
                let error = error.to_string();
                if let Some(group) = group_report.groups.last_mut() {
                    if !group.last_error.is_empty() {
                        group.last_error.push_str("; ");
                    }
                    group.last_error.push_str(&error);
                }
            }
            total.evaluation.recording_records += group_report.evaluation.recording_records;
            total.evaluation.alerts_dispatched += group_report.evaluation.alerts_dispatched;
            total.evaluation.last_eval_ms = group_report.evaluation.last_eval_ms;
            total.rules.extend(group_report.rules);
            total.groups.extend(group_report.groups);
        }
    }
    Ok(total)
}

/// Evaluates this shard's due ruler rule groups for one tenant, persists
/// state, and retains per-rule and per-group status.
///
/// The report's `evaluation` field is the group evaluation alone.
///
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn evaluate_and_persist_ruler_rule_set_for_shard_due_for_eval_with_report<S, W, A, R>(
    engine: &PromqlEngine<S>,
    sinks: RulerSinks<'_, W, A, R>,
    alert_state: &mut RulerAlertState,
    tenant: &TenantId,
    rules: &BTreeMap<String, BTreeMap<String, serde_yaml::Value>>,
    schedule: RulerEvaluationSchedule<'_>,
) -> Result<RulerEvaluationReport, PromqlError>
where
    S: MetricStore,
    W: RecordingRuleWalSink,
    A: AlertmanagerSink,
    R: RulerStateSink,
{
    let group_state = schedule.group_state;
    let shard = schedule.shard;
    let eval_time_ms = schedule.eval_time_ms;
    let scheduled = filter_ruler_rule_set_for_shard_due_for_eval(
        tenant.as_str(),
        rules,
        group_state,
        shard,
        eval_time_ms,
    );
    let evaluation =
        evaluate_and_persist_ruler_rule_set_with_report(super::RulerRuleSetEvaluationRequest::new(
            engine,
            sinks,
            alert_state,
            tenant,
            &scheduled,
            eval_time_ms,
        ))
        .await?;
    for namespace_entry in &scheduled {
        let namespace = namespace_entry.0;
        let namespace_groups = namespace_entry.1;
        for group_name in namespace_groups.keys() {
            group_state.apply_record(RulerGroupStateRecord {
                tenant: tenant.to_string(),
                namespace: namespace.clone(),
                group: group_name.clone(),
                last_eval_ms: eval_time_ms,
            });
        }
    }
    Ok(evaluation)
}
