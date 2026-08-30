use super::*;

#[tokio::test]
pub(crate) async fn replay_wal_records_populates_queryable_head() {
    let mut store = InMemoryMetricStore::new();
    let series_labels = vec![
        ("__name__".to_string(), "up".to_string()),
        ("job".to_string(), "api".to_string()),
    ];
    store.apply_wal_record(&WalRecord {
        tenant: "tenant-a".to_string(),
        labels: series_labels.clone(),
        payload: SamplePayload::Float {
            timestamp_ms: 10_000,
            value: 1.0,
            start_timestamp_ms: None,
        },
        exemplars: Vec::new(),
    });
    store.apply_wal_record(&WalRecord {
        tenant: "tenant-a".to_string(),
        labels: vec![
            (
                "__name__".to_string(),
                "request_duration_seconds".to_string(),
            ),
            ("job".to_string(), "api".to_string()),
        ],
        payload: SamplePayload::Hist {
            timestamp_ms: 10_000,
            hist: native_histogram(),
        },
        exemplars: Vec::new(),
    });
    store.apply_wal_record(&WalRecord {
        tenant: "tenant-a".to_string(),
        labels: series_labels.clone(),
        payload: SamplePayload::Metadata {
            metric_family_name: "up".to_string(),
            metric_type: "gauge".to_string(),
            help: "Target health.".to_string(),
            unit: String::new(),
        },
        exemplars: Vec::new(),
    });
    store.apply_wal_record(&WalRecord {
        tenant: "tenant-a".to_string(),
        labels: series_labels.clone(),
        payload: SamplePayload::Exemplars,
        exemplars: vec![WalExemplar {
            labels: vec![("trace_id".to_string(), "abc".to_string())],
            value: 1.0,
            timestamp_ms: 10_000,
        }],
    });

    let engine = PromqlEngine::new(std::sync::Arc::new(store.clone()), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "up", 10_000)
        .await
        .expect("query");
    let QueryResult::InstantVector(vector) = result else {
        panic!("expected vector");
    };
    check!(vector[0].value == SampleValue::Float(1.0));
    check!(store.metadata("tenant-a", Some("up")).await.unwrap()[0].help == "Target health.");
    check!(
        store.exemplars("tenant-a", &[], 0, 10_000).await.unwrap()[0].labels
            == lbls(&[("trace_id", "abc")])
    );
    check!(
        store
            .scan("tenant-a", &[], 0, 10_000)
            .await
            .unwrap()
            .histogram_table
            .is_some()
    );
}
