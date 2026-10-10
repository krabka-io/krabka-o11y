use super::*;

/// A block store of two float blocks at 1s, `up{job="api"}` = 1 in
/// `metrics/float/0001.parquet` and `up{job="db"}` = 2 in
/// `metrics/float/0002.parquet`, whose first block object has been deleted.
/// Returns the store and the surviving `job="db"` series labels.
pub(crate) async fn deleted_api_block_store() -> (BlockStore, Labels) {
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
    (block_store, kept_series)
}
