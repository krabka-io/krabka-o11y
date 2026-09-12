use super::*;

#[tokio::test]
pub(crate) async fn a_query_over_present_blocks_raises_no_warning() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = url::Url::parse("memory:///").unwrap();
    let mut block_store = BlockStore::new(object_store, base);

    let first_series = labels(&[("__name__", "up"), ("job", "api")]);
    let second_series = labels(&[("__name__", "up"), ("job", "db")]);
    write_float_block(
        &mut block_store,
        "metrics/float/0001.parquet",
        &first_series,
        1_000,
        1.0,
    )
    .await;
    write_float_block(
        &mut block_store,
        "metrics/float/0002.parquet",
        &second_series,
        1_000,
        2.0,
    )
    .await;

    let store = MetricBlockStore::new(block_store);
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let (result, annotations) = engine
        .query_instant_with_annotations(&tenant_id("tenant-a"), "up", 1_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected instant vector");
    };
    check!(samples.len() == 2);
    check!(annotations == Annotations::new());
}
