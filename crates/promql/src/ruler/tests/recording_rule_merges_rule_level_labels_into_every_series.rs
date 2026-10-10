use super::*;

#[tokio::test]
pub(crate) async fn recording_rule_merges_rule_level_labels_into_every_series() {
    let engine = up_samples_engine(&[
        UpSample {
            job: "api",
            ts_ms: 60_000,
            value: 1.0,
        },
        UpSample {
            job: "web",
            ts_ms: 60_000,
            value: 1.0,
        },
    ]);
    let rule_labels = BTreeMap::from([
        ("env".to_string(), "prod".to_string()),
        ("team".to_string(), "sre".to_string()),
    ]);

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
        for (name, value) in [
            ("env", "prod"),
            ("team", "sre"),
            ("__name__", "job:up:current"),
        ] {
            assert2::assert!(
                record
                    .labels
                    .contains(&(name.to_string(), value.to_string().into()))
            );
        }
    }
}
