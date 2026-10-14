use super::*;

struct ExpectedLabel {
    name: &'static str,
    value: &'static str,
}

#[tokio::test]
pub(crate) async fn recording_rule_merges_rule_level_labels_into_every_series() {
    let engine = two_job_up_engine();
    let mut rule_labels = BTreeMap::new();
    rule_labels.insert("env".to_string(), "prod".to_string());
    rule_labels.insert("team".to_string(), "sre".to_string());

    let records = super::super::evaluate_recording_rule(
        &engine,
        &tenant_id("tenant-a"),
        "job:up:current",
        "up",
        &rule_labels,
        60_000,
    )
    .await
    .expect("recording rule evaluation");

    assert2::assert!(records.len() == 2);
    for record in &records {
        for label in [
            ExpectedLabel {
                name: "env",
                value: "prod",
            },
            ExpectedLabel {
                name: "team",
                value: "sre",
            },
            ExpectedLabel {
                name: "__name__",
                value: "job:up:current",
            },
        ] {
            assert2::assert!(
                record
                    .labels
                    .contains(&krabka_metrics::MetricLabelPair::new(
                        label.name,
                        label.value.to_string().into()
                    ))
            );
        }
    }
}
