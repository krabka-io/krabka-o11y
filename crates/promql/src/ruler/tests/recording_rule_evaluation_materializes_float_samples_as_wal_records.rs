use super::*;

fn assert_recorded_job_sample(records: &[WalRecord], job: &str, value: f64) {
    check!(records.iter().any(|record| record.labels
        == vec![
            krabka_metrics::MetricLabelPair::new(
                "__name__",
                "job:http_requests:sum".to_string().into(),
            ),
            krabka_metrics::MetricLabelPair::new("job", job.to_string().into()),
        ]
        && matches!(
            record.payload,
            SamplePayload::Float {
                timestamp_ms: 60_000,
                value: actual,
                start_timestamp_ms: None,
            } if actual == value
        )));
}

#[tokio::test]
pub(crate) async fn recording_rule_evaluation_materializes_float_samples_as_wal_records() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels("http_requests_total", "api"),
        60_000,
        7.0,
    );
    store.push_float(
        "tenant-a",
        labels("http_requests_total", "web"),
        60_000,
        11.0,
    );
    let store = Arc::new(store);
    let engine = PromqlEngine::new(store, EngineOpts::default());

    let records = super::super::evaluate_recording_rule(
        &engine,
        &tenant_id("tenant-a"),
        "job:http_requests:sum",
        "sum by (job) (http_requests_total)",
        &BTreeMap::new(),
        60_000,
    )
    .await
    .expect("recording rule evaluation");

    check!(records.len() == 2);
    check!(records.iter().all(|record| record.tenant == "tenant-a"));
    assert_recorded_job_sample(&records, "api", 7.0);
    assert_recorded_job_sample(&records, "web", 11.0);
}
