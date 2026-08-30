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
  summary: "{{ $labels.job }} value {{ $value }}"
  passthrough: "{{ humanize $value }}"
"#,
    )
    .expect("alerting rule yaml");
    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels("up", "api"), 60_000, 1.0);
    let store = Arc::new(store);
    let engine = PromqlEngine::new(store, EngineOpts::default());
    let sink = RecordingAlertmanagerSink::default();

    let dispatched = super::super::evaluate_and_dispatch_alerting_rule(
        &engine, &sink, "tenant-a", &rule, 60_000,
    )
    .await
    .expect("alert dispatch");

    assert2::assert!(dispatched == 1);
    // `$value` is formatted via format_sample_value and `$labels.job` resolved
    // (in alert label values too); unknown actions like `humanize` are left
    // untouched.
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
                    (
                        "passthrough".to_string(),
                        "{{ humanize $value }}".to_string()
                    ),
                    ("summary".to_string(), "api value 1".to_string()),
                ]),
                starts_at_ms: 60_000,
                ends_at_ms: None,
                generator_url: String::new(),
            }]
    );
}
