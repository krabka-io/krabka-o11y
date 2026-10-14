use super::*;

#[tokio::test]
pub(crate) async fn recording_rule_fails_on_labelset_collision_after_rule_labels() {
    // Two series differ only by `job`; a rule label overwriting `job` to a
    // constant collapses them to the same labelset, which Prometheus rejects.
    let engine = two_job_up_engine();
    let mut rule_labels = BTreeMap::new();
    rule_labels.insert("job".to_string(), "merged".to_string());

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
