use super::*;

#[tokio::test]
pub(crate) async fn recording_rule_append_writes_materialized_records_to_sink() {
    let engine = up_api_engine(&[60_000]);
    let sink = RecordingSink::default();

    let appended = super::super::evaluate_and_append_recording_rule(
        &engine,
        &sink,
        &tenant_id("tenant-a"),
        "job:up:current",
        "up",
        &BTreeMap::new(),
        60_000,
    )
    .await
    .expect("recording rule append");

    assert2::assert!(appended == 1);
    assert2::assert!(sink.records() == vec![job_up_current_record(60_000)]);
}
