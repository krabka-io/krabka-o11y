use super::*;

mod alerting_rule_dispatch_expands_value_and_labels_templates {
    use super::*;

    #[tokio::test]
    pub(crate) async fn alerting_rule_dispatch_expands_value_and_labels_templates() {
        let rule: serde_yaml::Value = serde_yaml::from_str(
            r#"
alert: InstanceUp
expr: up > 0
labels:
  detail: "v={{ $value }}"
annotations:
  summary: "{{ $labels.job }} {{ $externalLabels.cluster }} {{ $externalURL }} value {{ $value }}"
  passthrough: "{{ humanize $value }}"
  related: '{{ query "errors" | first | value }} {{ query "errors" | first | label "job" }}'
"#,
        )
        .expect("alerting rule yaml");
        let mut store = InMemoryMetricStore::new();
        store.push_float("tenant-a", labels("up", "api"), 60_000, 1.0);
        store.push_float("tenant-a", labels("errors", "worker"), 60_000, 7.0);
        let store = Arc::new(store);
        let engine = PromqlEngine::new(store, EngineOpts::default());
        let mut external_labels = Labels::new();
        external_labels.insert("cluster", "prod");
        let sink = RecordingAlertmanagerSink::with_template_context(
            external_labels,
            "https://prom.example/graph?alert=InstanceUp",
        );

        let dispatched = super::super::super::evaluate_and_dispatch_alerting_rule(
            &engine,
            &sink,
            &tenant_id("tenant-a"),
            &rule,
            60_000,
        )
        .await
        .expect("alert dispatch");

        assert2::assert!(dispatched == 1);
        // The shared Go-template runtime expands variables and Prometheus helpers
        // in alert label values and annotations.
        assert2::assert!(
            sink.alerts()
                == vec![super::super::super::AlertmanagerAlert {
                    labels: BTreeMap::from([
                        ("alertname".to_string(), "InstanceUp".to_string()),
                        ("detail".to_string(), "v=1".to_string()),
                        ("job".to_string(), "api".to_string()),
                    ]),
                    annotations: BTreeMap::from([
                        ("passthrough".to_string(), "1".to_string()),
                        ("related".to_string(), "7 worker".to_string()),
                        (
                            "summary".to_string(),
                            "api prod https://prom.example/graph?alert=InstanceUp value 1"
                                .to_string(),
                        ),
                    ]),
                    starts_at_ms: 60_000,
                    ends_at_ms: None,
                    generator_url: "https://prom.example/graph?alert=InstanceUp".to_string(),
                }]
        );
    }
}

mod alerting_rule_dispatch_sends_firing_alerts_to_alertmanager_sink {
    use super::*;

    #[tokio::test]
    pub(crate) async fn alerting_rule_dispatch_sends_firing_alerts_to_alertmanager_sink() {
        let rule: serde_yaml::Value = serde_yaml::from_str(
            r"
alert: InstanceUp
expr: up > 0
labels:
  severity: page
annotations:
  summary: instance is up
",
        )
        .expect("alerting rule yaml");
        let engine = up_samples_engine(&[
            UpSample {
                job: "api",
                ts_ms: 60_000,
                value: 1.0,
            },
            UpSample {
                job: "web",
                ts_ms: 60_000,
                value: 0.0,
            },
        ]);
        let sink = RecordingAlertmanagerSink::default();

        let dispatched = super::super::super::evaluate_and_dispatch_alerting_rule(
            &engine,
            &sink,
            &tenant_id("tenant-a"),
            &rule,
            60_000,
        )
        .await
        .expect("alert dispatch");

        assert2::assert!(dispatched == 1);
        assert2::assert!(
            sink.alerts()
                == vec![super::super::super::AlertmanagerAlert {
                    labels: BTreeMap::from([
                        ("alertname".to_string(), "InstanceUp".to_string()),
                        ("job".to_string(), "api".to_string()),
                        ("severity".to_string(), "page".to_string()),
                    ]),
                    annotations: BTreeMap::from([(
                        "summary".to_string(),
                        "instance is up".to_string()
                    )]),
                    starts_at_ms: 60_000,
                    ends_at_ms: None,
                    generator_url: String::new(),
                }]
        );
    }
}

mod alerting_rule_dispatch_waits_for_for_duration_before_sending {
    use super::*;

    #[tokio::test]
    pub(crate) async fn alerting_rule_dispatch_waits_for_for_duration_before_sending() {
        let rule: serde_yaml::Value = serde_yaml::from_str(
            r"
alert: InstanceUp
expr: up > 0
for: 5m
",
        )
        .expect("alerting rule yaml");
        let engine = up_api_engine(&[60_000, 360_000]);
        let sink = RecordingAlertmanagerSink::default();
        let mut state = super::super::super::RulerAlertState::default();

        let pending = super::super::super::evaluate_and_dispatch_alerting_rule_with_state(
            &engine,
            &sink,
            &mut state,
            &tenant_id("tenant-a"),
            &rule,
            60_000,
        )
        .await
        .expect("pending alert evaluation");
        assert2::assert!(pending == 0);
        assert2::assert!(sink.alerts().is_empty());

        let firing = super::super::super::evaluate_and_dispatch_alerting_rule_with_state(
            &engine,
            &sink,
            &mut state,
            &tenant_id("tenant-a"),
            &rule,
            360_000,
        )
        .await
        .expect("firing alert evaluation");
        assert2::assert!(firing == 1);
        assert2::assert!(sink.alerts() == vec![instance_up_alert(60_000)]);
    }
}

mod alerting_rule_group_dispatch_runs_alerts_and_skips_recording_rules {
    use super::*;

    #[tokio::test]
    pub(crate) async fn alerting_rule_group_dispatch_runs_alerts_and_skips_recording_rules() {
        let group = mixed_rule_group();
        let engine = up_api_engine(&[60_000, 360_000]);
        let sink = RecordingAlertmanagerSink::default();
        let mut state = super::super::super::RulerAlertState::default();

        let pending = super::super::super::evaluate_and_dispatch_alerting_rule_group(
            &engine,
            &sink,
            &mut state,
            &tenant_id("tenant-a"),
            &group,
            60_000,
        )
        .await
        .expect("pending group alert evaluation");
        assert2::assert!(pending == 0);
        assert2::assert!(sink.alerts().is_empty());

        let firing = super::super::super::evaluate_and_dispatch_alerting_rule_group(
            &engine,
            &sink,
            &mut state,
            &tenant_id("tenant-a"),
            &group,
            360_000,
        )
        .await
        .expect("firing group alert evaluation");
        assert2::assert!(firing == 1);
        assert2::assert!(sink.alerts() == vec![instance_up_alert(60_000)]);
    }
}

mod alerting_rule_state_persistence_records_active_and_cleared_alerts {
    use super::*;

    #[tokio::test]
    pub(crate) async fn alerting_rule_state_persistence_records_active_and_cleared_alerts() {
        let rule = up_alert_rule(true);
        let engine = up_transition_engine();
        let alert_sink = RecordingAlertmanagerSink::default();
        let state_sink = RecordingRulerStateSink::default();
        let mut state = super::super::super::RulerAlertState::default();

        let pending = super::super::super::evaluate_and_persist_alerting_rule_with_state(
            &engine,
            &alert_sink,
            &state_sink,
            &mut state,
            &tenant_id("tenant-a"),
            &rule,
            60_000,
        )
        .await
        .expect("pending alert state persistence");
        let cleared = super::super::super::evaluate_and_persist_alerting_rule_with_state(
            &engine,
            &alert_sink,
            &state_sink,
            &mut state,
            &tenant_id("tenant-a"),
            &rule,
            120_000,
        )
        .await
        .expect("cleared alert state persistence");

        let mut alert_labels = BTreeMap::new();
        alert_labels.insert("alertname".to_string(), "InstanceUp".to_string());
        alert_labels.insert("job".to_string(), "api".to_string());
        check!(pending == 0);
        check!(cleared == 0);
        check!(
            state_sink.alert_records()
                == vec![
                    super::super::super::RulerAlertStateRecord {
                        tenant: "tenant-a".to_string(),
                        rule_id: "InstanceUp\nup > 0".to_string(),
                        labels: alert_labels.clone().into(),
                        active_since_ms: Some(60_000),
                        keep_firing_until_ms: None,
                    },
                    super::super::super::RulerAlertStateRecord {
                        tenant: "tenant-a".to_string(),
                        rule_id: "InstanceUp\nup > 0".to_string(),
                        labels: alert_labels.into(),
                        active_since_ms: None,
                        keep_firing_until_ms: None,
                    },
                ]
        );
    }
}

mod alerting_rule_with_compound_for_does_not_fire_immediately {
    use super::*;

    #[tokio::test]
    pub(crate) async fn alerting_rule_with_compound_for_does_not_fire_immediately() {
        // "1h30m" must parse to 90m; the alert may not fire until the series has
        // been active that long. The old single-unit parser coerced this to `0`
        // and fired on the first evaluation.
        let rule: serde_yaml::Value = serde_yaml::from_str(
            r"
alert: InstanceUp
expr: up > 0
for: 1h30m
",
        )
        .expect("alerting rule yaml");
        let mut store = InMemoryMetricStore::new();
        store.push_float("tenant-a", labels("up", "api"), 60_000, 1.0);
        store.push_float("tenant-a", labels("up", "api"), 60_000 + 90 * 60_000, 1.0);
        let store = Arc::new(store);
        let engine = PromqlEngine::new(store, EngineOpts::default());
        let sink = RecordingAlertmanagerSink::default();
        let mut state = super::super::super::RulerAlertState::default();

        // First evaluation: the alert becomes active now (active-since = this
        // eval time). With `for: 1h30m` it must NOT fire immediately — proving
        // the compound duration parsed as 90m rather than collapsing to 0.
        let pending = super::super::super::evaluate_and_dispatch_alerting_rule_with_state(
            &engine,
            &sink,
            &mut state,
            &tenant_id("tenant-a"),
            &rule,
            60_000,
        )
        .await
        .expect("pending evaluation");
        assert2::assert!(pending == 0);
        assert2::assert!(sink.alerts().is_empty());

        // 90 minutes later the `for: 1h30m` window is satisfied and it fires.
        let firing = super::super::super::evaluate_and_dispatch_alerting_rule_with_state(
            &engine,
            &sink,
            &mut state,
            &tenant_id("tenant-a"),
            &rule,
            60_000 + 90 * 60_000,
        )
        .await
        .expect("firing evaluation");
        assert2::assert!(firing == 1);
    }
}

mod alerting_rule_with_negative_for_is_a_hard_error {
    use super::*;

    #[tokio::test]
    pub(crate) async fn alerting_rule_with_negative_for_is_a_hard_error() {
        let rule: serde_yaml::Value = serde_yaml::from_str(
            r"
alert: InstanceUp
expr: up > 0
for: -5m
",
        )
        .expect("alerting rule yaml");
        let engine = up_api_engine(&[60_000]);
        let sink = RecordingAlertmanagerSink::default();
        let mut state = super::super::super::RulerAlertState::default();

        let result = super::super::super::evaluate_and_dispatch_alerting_rule_with_state(
            &engine,
            &sink,
            &mut state,
            &tenant_id("tenant-a"),
            &rule,
            60_000,
        )
        .await;
        assert2::assert!(result.is_err());
    }
}

mod firing_alert_emits_resolved_when_series_stops_matching {
    use super::*;

    #[tokio::test]
    pub(crate) async fn firing_alert_emits_resolved_when_series_stops_matching() {
        let rule = up_alert_rule(false);
        let engine = up_transition_engine();
        let mut state = super::super::super::RulerAlertState::default();

        // First tick: fires immediately (no `for`).
        let firing_sink = RecordingAlertmanagerSink::default();
        let firing = super::super::super::evaluate_and_dispatch_alerting_rule_with_state(
            &engine,
            &firing_sink,
            &mut state,
            &tenant_id("tenant-a"),
            &rule,
            60_000,
        )
        .await
        .expect("firing evaluation");
        assert2::assert!(firing == 1);
        assert2::assert!(firing_sink.alerts()[0].ends_at_ms == None);

        // Second tick: series drops; a resolved alert with EndsAt is emitted.
        let resolved_sink = RecordingAlertmanagerSink::default();
        let resolved = super::super::super::evaluate_and_dispatch_alerting_rule_with_state(
            &engine,
            &resolved_sink,
            &mut state,
            &tenant_id("tenant-a"),
            &rule,
            120_000,
        )
        .await
        .expect("resolved evaluation");
        assert2::assert!(resolved == 1);
        assert2::assert!(
            resolved_sink.alerts()
                == vec![super::super::super::AlertmanagerAlert {
                    labels: BTreeMap::from([
                        ("alertname".to_string(), "InstanceUp".to_string()),
                        ("job".to_string(), "api".to_string()),
                    ]),
                    annotations: BTreeMap::new(),
                    starts_at_ms: 60_000,
                    ends_at_ms: Some(120_000),
                    generator_url: String::new(),
                }]
        );
    }
}

mod keep_firing_for_holds_alert_firing_then_resolves_after_window {
    use super::*;

    #[tokio::test]
    pub(crate) async fn keep_firing_for_holds_alert_firing_then_resolves_after_window() {
        let rule: serde_yaml::Value = serde_yaml::from_str(
            r"
alert: InstanceUp
expr: up > 0
keep_firing_for: 5m
",
        )
        .expect("alerting rule yaml");
        let mut store = InMemoryMetricStore::new();
        store.push_float("tenant-a", labels("up", "api"), 0, 1.0);
        store.push_float("tenant-a", labels("up", "api"), 120_000, 0.0);
        store.push_float("tenant-a", labels("up", "api"), 600_000, 0.0);
        let store = Arc::new(store);
        let engine = PromqlEngine::new(store, EngineOpts::default());
        let mut state = super::super::super::RulerAlertState::default();
        let state_sink = RecordingRulerStateSink::default();

        // t=0: fires; keep-firing deadline armed at 0 + 5m = 300_000.
        let sink0 = RecordingAlertmanagerSink::default();
        let fired = super::super::super::evaluate_and_persist_alerting_rule_with_state(
            &engine,
            &sink0,
            &state_sink,
            &mut state,
            &tenant_id("tenant-a"),
            &rule,
            0,
        )
        .await
        .expect("initial firing");
        assert2::assert!(fired == 1);
        assert2::assert!(sink0.alerts()[0].ends_at_ms == None);
        let records = state_sink.alert_records();
        assert2::assert!(records.len() == 1);
        assert2::assert!(records[0].keep_firing_until_ms == Some(300_000));

        // Simulate a process restart by rebuilding only from the durable record.
        state = super::super::super::RulerAlertState::default();
        state.apply_record(records[0].clone());

        // t=120s after restart: series gone but within keep_firing_for; still
        // firing, no EndsAt.
        let sink1 = RecordingAlertmanagerSink::default();
        let kept = super::super::super::evaluate_and_dispatch_alerting_rule_with_state(
            &engine,
            &sink1,
            &mut state,
            &tenant_id("tenant-a"),
            &rule,
            120_000,
        )
        .await
        .expect("kept firing");
        let kept_alerts = sink1.alerts();
        assert2::assert!(kept == 1);
        assert2::assert!(kept_alerts[0].ends_at_ms == None);

        // t=600s: keep-firing window (deadline 300s) elapsed; resolves with EndsAt.
        let sink2 = RecordingAlertmanagerSink::default();
        let resolved = super::super::super::evaluate_and_dispatch_alerting_rule_with_state(
            &engine,
            &sink2,
            &mut state,
            &tenant_id("tenant-a"),
            &rule,
            600_000,
        )
        .await
        .expect("resolved after window");
        let resolved_alerts = sink2.alerts();
        assert2::assert!(resolved == 1);
        assert2::assert!(resolved_alerts[0].ends_at_ms == Some(600_000));
    }
}

mod ruler_alert_state_replays_compacted_records_before_evaluation {
    use super::*;

    #[tokio::test]
    pub(crate) async fn ruler_alert_state_replays_compacted_records_before_evaluation() {
        let rule: serde_yaml::Value = serde_yaml::from_str(
            r"
alert: InstanceUp
expr: up > 0
for: 5m
",
        )
        .expect("alerting rule yaml");
        let mut alert_labels = BTreeMap::new();
        alert_labels.insert("alertname".to_string(), "InstanceUp".to_string());
        alert_labels.insert("job".to_string(), "api".to_string());
        let mut state = super::super::super::RulerAlertState::default();
        state.apply_record(super::super::super::RulerAlertStateRecord {
            tenant: "tenant-a".to_string(),
            rule_id: "InstanceUp\nup > 0".to_string(),
            labels: alert_labels.clone().into(),
            active_since_ms: Some(60_000),
            keep_firing_until_ms: None,
        });

        let engine = up_api_engine(&[360_000]);
        let sink = RecordingAlertmanagerSink::default();

        let firing = super::super::super::evaluate_and_dispatch_alerting_rule_with_state(
            &engine,
            &sink,
            &mut state,
            &tenant_id("tenant-a"),
            &rule,
            360_000,
        )
        .await
        .expect("replayed alert state evaluation");

        assert2::assert!(firing == 1);
        assert2::assert!(sink.alerts()[0].starts_at_ms == 60_000);

        state.apply_record(super::super::super::RulerAlertStateRecord {
            tenant: "tenant-a".to_string(),
            rule_id: "InstanceUp\nup > 0".to_string(),
            labels: alert_labels.into(),
            active_since_ms: None,
            keep_firing_until_ms: None,
        });
        let sink = RecordingAlertmanagerSink::default();
        let pending = super::super::super::evaluate_and_dispatch_alerting_rule_with_state(
            &engine,
            &sink,
            &mut state,
            &tenant_id("tenant-a"),
            &rule,
            360_000,
        )
        .await
        .expect("tombstoned alert state evaluation");

        assert2::assert!(pending == 0);
        assert2::assert!(sink.alerts().is_empty());
    }
}
