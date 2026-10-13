use super::{deleted_api_block_store::api_db_block_store, *};

#[tokio::test]
pub(crate) async fn a_corrupt_block_still_fails_the_query() {
    let fixture = api_db_block_store().await;
    // The object is there and the bytes do not decode. That is damaged data,
    // not deleted data, so the query must fail rather than answer around it.
    fixture
        .object_store
        .put(
            &ObjectPath::from("metrics/float/0001.parquet"),
            PutPayload::from_static(b"this is not a parquet block"),
        )
        .await
        .unwrap();

    let store = MetricBlockStore::new(fixture.block_store);
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let error = engine
        .query_instant(&tenant_id("tenant-a"), "up", 1_000)
        .await
        .expect_err("a corrupt block fails the query");

    check!(matches!(error, PromqlError::Store(_)));
    check!(error.to_string().contains("metrics/float/0001.parquet"));
    check!(error.to_string().contains("corrupt"));
}
