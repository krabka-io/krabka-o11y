use super::*;

#[tokio::test]
pub(crate) async fn exemplars_answer_around_a_deleted_block_and_warn() {
    let fixture = MemoryBlockStoreFixture::build();
    let object_store = &fixture.object_store;
    let writer_store = fixture.writer();

    let deleted_series = labels(&[
        crate::test_support::TestLabelPair::new("__name__", "http_requests_total"),
        crate::test_support::TestLabelPair::new("job", "api"),
        crate::test_support::TestLabelPair::new("pod", "one"),
    ]);
    let kept_series = labels(&[
        crate::test_support::TestLabelPair::new("__name__", "http_requests_total"),
        crate::test_support::TestLabelPair::new("job", "api"),
        crate::test_support::TestLabelPair::new("pod", "two"),
    ]);
    let deleted = sidecar_manifest(
        &writer_store,
        MetricBlockKind::Exemplars,
        "metrics/exemplars/0001.parquet",
        exemplar_schema(),
        exemplar_batch(ExemplarRow {
            fingerprint: deleted_series.fingerprint(),
            timestamp_ms: 10_100,
            value: 1.0,
            trace_id: Some("aaa".to_string()),
            span_id: Some("bbb".to_string()),
            labels: vec![krabka_blockstore::LabelPair::new(
                "kind",
                "slow".to_string(),
            )],
        }),
        &deleted_series,
    )
    .await;
    let kept = sidecar_manifest(
        &writer_store,
        MetricBlockKind::Exemplars,
        "metrics/exemplars/0002.parquet",
        exemplar_schema(),
        exemplar_batch(ExemplarRow {
            fingerprint: kept_series.fingerprint(),
            timestamp_ms: 10_500,
            value: 7.0,
            trace_id: Some("ccc".to_string()),
            span_id: Some("ddd".to_string()),
            labels: vec![krabka_blockstore::LabelPair::new(
                "kind",
                "slow".to_string(),
            )],
        }),
        &kept_series,
    )
    .await;
    object_store
        .delete(&ObjectPath::from("metrics/exemplars/0001.parquet"))
        .await
        .unwrap();

    let store =
        MetricBlockStore::from_compaction_manifests(fixture.reader(), None, &[deleted, kept]);
    let scan = api_exemplar_scan(&store).await;

    check!(scan.exemplars.len() == 1);
    check!(scan.exemplars[0].series_labels == kept_series);
    check!(scan.exemplars[0].ts_ms == 10_500);
    check!(scan.warnings.len() == 1);
    check!(scan.warnings[0].contains("metrics/exemplars/0001.parquet"));
    check!(scan.warnings[0].contains("missing"));
}
