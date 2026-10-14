use super::*;

#[tokio::test]
pub(crate) async fn cloned_wal_head_sees_records_replayed_through_original_handle() {
    let head = WalHead::new();
    let query_handle = head.clone();
    head.apply_wal_record(&WalRecord {
        tenant: "tenant-a".to_string(),
        labels: vec![
            krabka_metrics::MetricLabelPair::new("__name__", "up".to_string().into()),
            krabka_metrics::MetricLabelPair::new("job", "api".to_string().into()),
        ],
        payload: SamplePayload::Float {
            timestamp_ms: 10_000,
            value: 1.0,
            start_timestamp_ms: None,
        },
        exemplars: Vec::new(),
    });

    assert_up_query_value(query_handle).await;
}
