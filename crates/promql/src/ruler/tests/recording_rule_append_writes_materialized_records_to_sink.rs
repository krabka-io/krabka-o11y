use super::*;

#[tokio::test]
pub(crate) async fn recording_rule_append_writes_materialized_records_to_sink() {
    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels("up", "api"), 60_000, 1.0);
    let store = Arc::new(store);
    let engine = PromqlEngine::new(store, EngineOpts::default());
    let sink = RecordingSink::default();

    let appended = super::super::evaluate_and_append_recording_rule(
        &engine,
        &sink,
        "tenant-a",
        "job:up:current",
        "up",
        &BTreeMap::new(),
        60_000,
    )
    .await
    .expect("recording rule append");

    assert2::assert!(appended == 1);
    assert2::assert!(
        sink.records()
            == vec![WalRecord {
                tenant: "tenant-a".to_string(),
                labels: vec![
                    ("__name__".to_string(), "job:up:current".to_string()),
                    ("job".to_string(), "api".to_string()),
                ],
                payload: SamplePayload::Float {
                    timestamp_ms: 60_000,
                    value: 1.0,
                    start_timestamp_ms: None,
                },
                exemplars: Vec::new(),
            }]
    );
}
