use super::*;

#[derive(bon::Builder)]
pub(crate) struct DeletedApiBlockStoreFixture {
    pub(crate) block_store: BlockStore,
    pub(crate) kept_series: Labels,
}

/// A block store of two float blocks at 1s, `up{job="api"}` = 1 in
/// `metrics/float/0001.parquet` and `up{job="db"}` = 2 in
/// `metrics/float/0002.parquet`, whose first block object has been deleted.
/// Returns the store and the surviving `job="db"` series labels.
pub(crate) async fn deleted_api_block_store() -> DeletedApiBlockStoreFixture {
    let fixture = MemoryBlockStoreFixture::build();
    let object_store = &fixture.object_store;
    let mut block_store = fixture.writer();

    let deleted_series = labels(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]);
    let kept_series = labels(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "db"),
    ]);
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
    DeletedApiBlockStoreFixture::builder()
        .block_store(block_store)
        .kept_series(kept_series)
        .build()
}
