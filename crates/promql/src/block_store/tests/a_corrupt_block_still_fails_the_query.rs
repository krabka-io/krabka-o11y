use super::*;

#[tokio::test]
pub(crate) async fn a_corrupt_block_still_fails_the_query() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = url::Url::parse("memory:///").unwrap();
    let mut block_store = BlockStore::new(object_store.clone(), base);

    let corrupt_series = labels(&[("__name__", "up"), ("job", "api")]);
    let kept_series = labels(&[("__name__", "up"), ("job", "db")]);
    write_float_block(
        &mut block_store,
        "metrics/float/0001.parquet",
        &corrupt_series,
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
    // The object is there and the bytes do not decode. That is damaged data,
    // not deleted data, so the query must fail rather than answer around it.
    object_store
        .put(
            &ObjectPath::from("metrics/float/0001.parquet"),
            PutPayload::from_static(b"this is not a parquet block"),
        )
        .await
        .unwrap();

    let store = MetricBlockStore::new(block_store);
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let error = engine
        .query_instant(&tenant_id("tenant-a"), "up", 1_000)
        .await
        .expect_err("a corrupt block fails the query");

    check!(matches!(error, PromqlError::Store(_)));
    check!(error.to_string().contains("metrics/float/0001.parquet"));
    check!(error.to_string().contains("corrupt"));
}
