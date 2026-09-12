use super::*;

#[tokio::test]
pub(crate) async fn exemplars_answer_around_a_deleted_block_and_warn() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = url::Url::parse("memory:///").unwrap();
    let writer_store = BlockStore::new(object_store.clone(), base.clone());

    let deleted_series = labels(&[
        ("__name__", "http_requests_total"),
        ("job", "api"),
        ("pod", "one"),
    ]);
    let kept_series = labels(&[
        ("__name__", "http_requests_total"),
        ("job", "api"),
        ("pod", "two"),
    ]);
    let deleted = sidecar_manifest(
        &writer_store,
        MetricBlockKind::Exemplars,
        "metrics/exemplars/0001.parquet",
        exemplar_schema(),
        exemplar_batch(
            deleted_series.fingerprint(),
            10_100,
            1.0,
            "aaa",
            "bbb",
            "kind",
            "slow",
        ),
        &deleted_series,
    )
    .await;
    let kept = sidecar_manifest(
        &writer_store,
        MetricBlockKind::Exemplars,
        "metrics/exemplars/0002.parquet",
        exemplar_schema(),
        exemplar_batch(
            kept_series.fingerprint(),
            10_500,
            7.0,
            "ccc",
            "ddd",
            "kind",
            "slow",
        ),
        &kept_series,
    )
    .await;
    object_store
        .delete(&ObjectPath::from("metrics/exemplars/0001.parquet"))
        .await
        .unwrap();

    let store = MetricBlockStore::from_compaction_manifests(
        BlockStore::new(object_store, base),
        None,
        &[deleted, kept],
    );
    let scan = store
        .exemplars(
            "tenant-a",
            &[krabka_blockstore::LabelMatcher {
                name: "job".to_string(),
                op: krabka_blockstore::MatchOp::Eq,
                value: "api".to_string(),
            }],
            10_000,
            11_000,
        )
        .await
        .unwrap();

    check!(scan.exemplars.len() == 1);
    check!(scan.exemplars[0].series_labels == kept_series);
    check!(scan.exemplars[0].ts_ms == 10_500);
    check!(scan.warnings.len() == 1);
    check!(scan.warnings[0].contains("metrics/exemplars/0001.parquet"));
    check!(scan.warnings[0].contains("missing"));
}
