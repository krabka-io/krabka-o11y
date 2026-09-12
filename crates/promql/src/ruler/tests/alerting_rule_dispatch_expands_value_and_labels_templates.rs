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

    let dispatched = super::super::evaluate_and_dispatch_alerting_rule(
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
            == vec![super::super::AlertmanagerAlert {
                labels: BTreeMap::from([
                    ("__name__".to_string(), "up".to_string()),
                    ("alertname".to_string(), "InstanceUp".to_string()),
                    ("detail".to_string(), "v=1".to_string()),
                    ("job".to_string(), "api".to_string()),
                ]),
                annotations: BTreeMap::from([
                    ("passthrough".to_string(), "1".to_string()),
                    ("related".to_string(), "7 worker".to_string()),
                    (
                        "summary".to_string(),
                        "api prod https://prom.example/graph?alert=InstanceUp value 1".to_string(),
                    ),
                ]),
                starts_at_ms: 60_000,
                ends_at_ms: None,
                generator_url: "https://prom.example/graph?alert=InstanceUp".to_string(),
            }]
    );
}
