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
    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels("up", "api"), 60_000, 1.0);
    store.push_float("tenant-a", labels("up", "web"), 60_000, 0.0);
    let store = Arc::new(store);
    let engine = PromqlEngine::new(store, EngineOpts::default());
    let sink = RecordingAlertmanagerSink::default();

    let dispatched =
        super::super::evaluate_and_dispatch_alerting_rule(&engine, &sink, "tenant-a", &rule, 60_000)
            .await
            .expect("alert dispatch");

    assert2::assert!(dispatched == 1);
    assert2::assert!(
        sink.alerts()
            == vec![super::super::AlertmanagerAlert {
                labels: BTreeMap::from([
                    ("__name__".to_string(), "up".to_string()),
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
