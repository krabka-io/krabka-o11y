use super::*;

#[tokio::test]
pub(crate) async fn a_query_answers_around_a_deleted_block_and_warns() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = url::Url::parse("memory:///").unwrap();
    let mut block_store = BlockStore::new(object_store.clone(), base);

    let deleted_series = labels(&[("__name__", "up"), ("job", "api")]);
    let kept_series = labels(&[("__name__", "up"), ("job", "db")]);
    write_float_block(
        &mut block_store,
        "metrics/float/0001.parquet",
        &deleted_series,
        1_000,
        1.0,
    )
    .await;
    write_float_block(
        &mut block_store,
        "metrics/float/0002.parquet",
        &kept_series,
        1_000,
        2.0,
    )
    .await;
    object_store
        .delete(&ObjectPath::from("metrics/float/0001.parquet"))
        .await
        .unwrap();

    let store = MetricBlockStore::new(block_store);
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let (result, annotations) = engine
        .query_instant_with_annotations(&tenant_id("tenant-a"), "up", 1_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected instant vector");
    };
    assert2::assert!(
        samples
            == vec![InstantSample {
                labels: kept_series,
                ts_ms: 1_000,
                value: SampleValue::Float(2.0),
            }]
    );
    check!(annotations.warnings.len() == 1);
    check!(annotations.warnings[0].contains("metrics/float/0001.parquet"));
    check!(annotations.warnings[0].contains("missing"));
    check!(annotations.infos.is_empty());
}
