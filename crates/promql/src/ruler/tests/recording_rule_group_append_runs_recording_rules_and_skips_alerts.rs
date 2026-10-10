use super::*;

#[tokio::test]
pub(crate) async fn recording_rule_group_append_runs_recording_rules_and_skips_alerts() {
    let group: serde_yaml::Value = serde_yaml::from_str(
        r"
name: availability
interval: 30s
rules:
  - record: job:up:current
    expr: up
  - alert: InstanceDown
    expr: up == 0
",
    )
    .expect("rule group yaml");
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
    let sink = RecordingSink::default();

    let appended = super::super::evaluate_and_append_recording_rule_group(
        &engine,
        &sink,
        &tenant_id("tenant-a"),
        &group,
        60_000,
    )
    .await
    .expect("recording rule group append");

    let records = sink.records();
    check!(appended == 2);
    check!(records.len() == 2);
    check!(
        records.iter().all(|record| record.labels[0]
            == ("__name__".to_string(), "job:up:current".to_string().into()))
    );
}
