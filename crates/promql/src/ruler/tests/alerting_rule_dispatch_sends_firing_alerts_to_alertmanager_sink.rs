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
    assert2::assert!(
        sink.alerts()
            == vec![super::super::AlertmanagerAlert {
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
