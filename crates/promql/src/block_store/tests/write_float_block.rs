use super::*;

pub(crate) async fn write_float_block(
    block_store: &mut BlockStore,
    object_key: &str,
    series_labels: &Labels,
    ts_ms: i64,
    value: f64,
) {
    let fp = series_labels.fingerprint();
    let batch = encode_float_samples(&[(fp, ts_ms, value)]).unwrap();
    let block_meta = block_store
        .writer()
        .write_block("tenant-a", object_key, float_sample_schema(), &[batch])
        .await
        .unwrap();
    block_store
        .index_mut()
        .add_series("tenant-a", fp, series_labels);
    block_store.index_mut().add_block(&block_meta);
}
