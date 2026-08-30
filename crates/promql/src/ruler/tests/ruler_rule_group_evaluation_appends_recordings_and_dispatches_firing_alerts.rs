use super::*;

#[tokio::test]
pub(crate) async fn ruler_rule_group_evaluation_appends_recordings_and_dispatches_firing_alerts() {
    let group: serde_yaml::Value = serde_yaml::from_str(
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
    .expect("rule group yaml");
    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels("up", "api"), 60_000, 1.0);
    store.push_float("tenant-a", labels("up", "api"), 360_000, 1.0);
    let store = Arc::new(store);
    let engine = PromqlEngine::new(store, EngineOpts::default());
    let wal_sink = RecordingSink::default();
    let alert_sink = RecordingAlertmanagerSink::default();
    let mut state = super::super::RulerAlertState::default();

    let pending = super::super::evaluate_ruler_rule_group(
        &engine,
        &wal_sink,
        &alert_sink,
        &mut state,
        "tenant-a",
        &group,
        60_000,
    )
    .await
    .expect("pending group evaluation");
    assert2::assert!(
        pending
            == super::super::RulerGroupEvaluation {
                recording_records: 1,
                alerts_dispatched: 0,
                last_eval_ms: 60_000,
            }
    );

    let firing = super::super::evaluate_ruler_rule_group(
        &engine,
        &wal_sink,
        &alert_sink,
        &mut state,
        "tenant-a",
        &group,
        360_000,
    )
    .await
    .expect("firing group evaluation");

    assert2::assert!(
        firing
            == super::super::RulerGroupEvaluation {
                recording_records: 1,
                alerts_dispatched: 1,
                last_eval_ms: 360_000,
            }
    );
    assert2::assert!(
        wal_sink.records()
            == vec![
                WalRecord {
                    tenant: "tenant-a".to_string(),
                    labels: vec![
                        ("__name__".to_string(), "job:up:current".to_string()),
                        ("job".to_string(), "api".to_string()),
                    ],
                    payload: SamplePayload::Float {
                        timestamp_ms: 60_000,
                        value: 1.0,
                        start_timestamp_ms: None,
                    },
                    exemplars: Vec::new(),
                },
                WalRecord {
                    tenant: "tenant-a".to_string(),
                    labels: vec![
                        ("__name__".to_string(), "job:up:current".to_string()),
                        ("job".to_string(), "api".to_string()),
                    ],
                    payload: SamplePayload::Float {
                        timestamp_ms: 360_000,
                        value: 1.0,
                        start_timestamp_ms: None,
                    },
                    exemplars: Vec::new(),
                },
            ]
    );
    assert2::assert!(
        alert_sink.alerts()
            == vec![super::super::AlertmanagerAlert {
                labels: BTreeMap::from([
                    ("__name__".to_string(), "up".to_string()),
                    ("alertname".to_string(), "InstanceUp".to_string()),
                    ("job".to_string(), "api".to_string()),
                ]),
                annotations: BTreeMap::new(),
                starts_at_ms: 60_000,
                ends_at_ms: None,
                generator_url: String::new(),
            }]
    );
}
