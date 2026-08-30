use super::*;

#[tokio::test]
pub(crate) async fn cloned_wal_head_sees_records_replayed_through_original_handle() {
    let head = WalHead::new();
    let query_handle = head.clone();
    head.apply_wal_record(&WalRecord {
        tenant: "tenant-a".to_string(),
        labels: vec![
            ("__name__".to_string(), "up".to_string()),
            ("job".to_string(), "api".to_string()),
        ],
        payload: SamplePayload::Float {
            timestamp_ms: 10_000,
            value: 1.0,
            start_timestamp_ms: None,
        },
        exemplars: Vec::new(),
    });

    let engine = PromqlEngine::new(std::sync::Arc::new(query_handle), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "up", 10_000)
        .await
        .expect("query");
    let QueryResult::InstantVector(vector) = result else {
        panic!("expected vector");
    };

    assert2::assert!(vector[0].value == SampleValue::Float(1.0));
}
