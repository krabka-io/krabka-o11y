use super::*;

#[tokio::test]
pub(crate) async fn recording_rule_fails_on_labelset_collision_after_rule_labels() {
    // Two series differ only by `job`; a rule label overwriting `job` to a
    // constant collapses them to the same labelset, which Prometheus rejects.
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
    let rule_labels = BTreeMap::from([("job".to_string(), "merged".to_string())]);

    let result = super::super::evaluate_recording_rule(
        &engine,
        &tenant_id("tenant-a"),
        "job:up:current",
        "up",
        &rule_labels,
        60_000,
    )
    .await;

    assert2::assert!(let Err(super::super::PromqlError::Exec(_)) = &result);
    if let Err(super::super::PromqlError::Exec(message)) = result {
        assert2::assert!(message.contains("same labelset after applying rule labels"));
    }
}
