use krabka_blockstore::BlockStore;
use krabka_metrics::{encode_float_samples, float_sample_schema};
use object_store::{ObjectStore, memory::InMemory};

use super::*;
use crate::block_store::MetricBlockStore;

#[tokio::test]
pub(crate) async fn created_timestamp_survives_cold_and_hot_merge() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut blocks = BlockStore::new(object_store, url::Url::parse("memory:///").unwrap());
    let series_labels = labels(&[("__name__", "http_requests_total"), ("job", "api")]);
    let fp = series_labels.fingerprint();
    let batch = encode_float_samples(&[(fp, 100_000, 5.0, Some(50_000))]).unwrap();
    let block_meta = blocks
        .writer()
        .write_block(
            "tenant-a",
            "metrics/float/created.parquet",
            float_sample_schema(),
            &[batch],
        )
        .await
        .unwrap();
    blocks
        .index_mut()
        .add_series("tenant-a", fp, &series_labels);
    blocks.index_mut().add_block(&block_meta);

    let mut hot = InMemoryMetricStore::new();
    hot.push_float_with_start_timestamp("tenant-a", series_labels, 200_000, 8.0, Some(50_000));
    let engine = PromqlEngine::new(
        Arc::new(MergedMetricStore::new(MetricBlockStore::new(blocks), hot)),
        EngineOpts::default(),
    );

    let result = engine
        .query_instant(
            &tenant_id("tenant-a"),
            "increase(http_requests_total[5m])",
            300_000,
        )
        .await
        .unwrap();
    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert2::assert!(samples.len() == 1);
    let SampleValue::Float(value) = &samples[0].value else {
        panic!("expected float");
    };
    assert2::assert!((*value - 10.0).abs() < 1e-9);
}
