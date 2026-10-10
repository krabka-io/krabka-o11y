use super::*;

/// A tenant's rule groups, keyed by namespace and then group name.
pub(crate) type RuleSet = BTreeMap<String, BTreeMap<String, serde_yaml::Value>>;

/// A 30s group that records `job:up:current` from `up` and alerts
/// `InstanceUp` once `up > 0` has held for 5m.
pub(crate) fn mixed_rule_group() -> serde_yaml::Value {
    serde_yaml::from_str(
        r"
name: mixed
interval: 30s
rules:
  - record: job:up:current
    expr: up
  - alert: InstanceUp
    expr: up > 0
    for: 5m
",
    )
    .expect("rule group yaml")
}

/// An engine over `tenant-a`'s `up{job="api"}`, which is 1 at each of
/// `sample_times_ms`.
pub(crate) fn up_api_engine(sample_times_ms: &[i64]) -> PromqlEngine<InMemoryMetricStore> {
    let mut store = InMemoryMetricStore::new();
    for &ts_ms in sample_times_ms {
        store.push_float("tenant-a", labels("up", "api"), ts_ms, 1.0);
    }
    PromqlEngine::new(Arc::new(store), EngineOpts::default())
}

/// One `up{job}` sample of `tenant-a`.
#[derive(Clone, Copy)]
pub(crate) struct UpSample<'a> {
    pub(crate) job: &'a str,
    pub(crate) ts_ms: i64,
    pub(crate) value: f64,
}

/// An engine over `tenant-a`'s `up` series holding `samples`.
pub(crate) fn up_samples_engine(samples: &[UpSample<'_>]) -> PromqlEngine<InMemoryMetricStore> {
    let mut store = InMemoryMetricStore::new();
    for sample in samples {
        store.push_float(
            "tenant-a",
            labels("up", sample.job),
            sample.ts_ms,
            sample.value,
        );
    }
    PromqlEngine::new(Arc::new(store), EngineOpts::default())
}

/// The firing `InstanceUp{job="api"}` alert active since `starts_at_ms`.
pub(crate) fn instance_up_alert(starts_at_ms: i64) -> super::super::AlertmanagerAlert {
    super::super::AlertmanagerAlert {
        labels: BTreeMap::from([
            ("alertname".to_string(), "InstanceUp".to_string()),
            ("job".to_string(), "api".to_string()),
        ]),
        annotations: BTreeMap::new(),
        starts_at_ms,
        ends_at_ms: None,
        generator_url: String::new(),
    }
}

/// The `job:up:current{job="api"}` = 1 record a recording rule appends at
/// `timestamp_ms`.
pub(crate) fn job_up_current_record(timestamp_ms: i64) -> WalRecord {
    WalRecord {
        tenant: "tenant-a".to_string(),
        labels: vec![
            ("__name__".to_string(), "job:up:current".into()),
            ("job".to_string(), "api".into()),
        ],
        payload: SamplePayload::Float {
            timestamp_ms,
            value: 1.0,
            start_timestamp_ms: None,
        },
        exemplars: Vec::new(),
    }
}

/// A rule set of the `team-a/recording` group, which records
/// `job:up:current`, and the `team-b/alerting` group read from `alerting_yaml`.
pub(crate) fn namespaced_rule_set(alerting_yaml: &str) -> RuleSet {
    let recording_group: serde_yaml::Value = serde_yaml::from_str(
        r"
name: recording
rules:
  - record: job:up:current
    expr: up
",
    )
    .expect("recording group yaml");
    let alerting_group: serde_yaml::Value =
        serde_yaml::from_str(alerting_yaml).expect("alerting group yaml");
    BTreeMap::from([
        (
            "team-a".to_string(),
            BTreeMap::from([("recording".to_string(), recording_group)]),
        ),
        (
            "team-b".to_string(),
            BTreeMap::from([("alerting".to_string(), alerting_group)]),
        ),
    ])
}

/// One rule group that holds only its name and evaluation interval.
pub(crate) struct IntervalGroup {
    pub(crate) namespace: &'static str,
    pub(crate) group_name: &'static str,
    pub(crate) interval: &'static str,
}

/// A rule set of rule-less `groups`.
pub(crate) fn interval_rule_set(groups: &[IntervalGroup]) -> RuleSet {
    let mut rules = RuleSet::new();
    for group in groups {
        let yaml = serde_yaml::to_value(BTreeMap::from([
            ("name", group.group_name),
            ("interval", group.interval),
        ]))
        .expect("group yaml");
        rules
            .entry(group.namespace.to_string())
            .or_default()
            .insert(group.group_name.to_string(), yaml);
    }
    rules
}

/// When a `tenant-a` rule group last evaluated.
pub(crate) struct GroupLastEval {
    pub(crate) namespace: &'static str,
    pub(crate) group: &'static str,
    pub(crate) last_eval_ms: i64,
}

/// The group state that `last_evals` replay into.
pub(crate) fn group_state(last_evals: &[GroupLastEval]) -> super::super::RulerGroupState {
    let mut state = super::super::RulerGroupState::default();
    state.apply_records(
        last_evals
            .iter()
            .map(|last_eval| super::super::RulerGroupStateRecord {
                tenant: "tenant-a".to_string(),
                namespace: last_eval.namespace.to_string(),
                group: last_eval.group.to_string(),
                last_eval_ms: last_eval.last_eval_ms,
            }),
    );
    state
}

/// The group state of the sharded-scheduling tests: `team-a/not-yet` last
/// evaluated at 120s, `team-b/due` at 60s and `team-c/also-due` at 90s.
pub(crate) fn staggered_group_state() -> super::super::RulerGroupState {
    group_state(&[
        GroupLastEval {
            namespace: "team-a",
            group: "not-yet",
            last_eval_ms: 120_000,
        },
        GroupLastEval {
            namespace: "team-b",
            group: "due",
            last_eval_ms: 60_000,
        },
        GroupLastEval {
            namespace: "team-c",
            group: "also-due",
            last_eval_ms: 90_000,
        },
    ])
}

/// The sinks an `up > 0` recording-and-alerting evaluation wrote to.
pub(crate) struct UpRuleSinks<'a> {
    pub(crate) wal_sink: &'a RecordingSink,
    pub(crate) alert_sink: &'a RecordingAlertmanagerSink,
}

/// Checks the evaluation at 6m that follows a pending one at 1m: one more
/// recording, and the `InstanceUp` alert that has been pending since 1m firing.
pub(crate) fn assert_up_rules_fired_at_six_minutes(
    firing: &super::super::RulerGroupEvaluation,
    sinks: &UpRuleSinks<'_>,
) {
    assert2::assert!(
        *firing
            == super::super::RulerGroupEvaluation {
                recording_records: 1,
                alerts_dispatched: 1,
                last_eval_ms: 360_000,
            }
    );
    assert2::assert!(
        sinks.wal_sink.records()
            == vec![
                job_up_current_record(60_000),
                job_up_current_record(360_000),
            ]
    );
    assert2::assert!(sinks.alert_sink.alerts() == vec![instance_up_alert(60_000)]);
}
