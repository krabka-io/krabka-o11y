use krabka_metrics::{
    FloatRow, ObjectStoreCompactionIndexSink, TenantCompactionRows, list_compaction_manifests,
    write_compacted_tenant_blocks,
};

use super::*;

const QUERY: &str = "count(count_over_time(soak_metric[60s]))";
const QUERY_TIME_MS: i64 = 100_000;
const SERIES_PER_BLOCK: u64 = 3;

/// Writes `blocks` float-only metric blocks the way the compactor does and
/// returns their manifests.
async fn write_float_only_blocks(
    object_store: &Arc<dyn ObjectStore>,
    blocks: i64,
) -> Vec<CompactionIndexManifest> {
    let writer = krabka_blockstore::BlockWriter::new(Arc::clone(object_store));
    let sink = ObjectStoreCompactionIndexSink::new(Arc::clone(object_store));
    for block in 0..blocks {
        let mut series_labels = std::collections::BTreeMap::new();
        let mut float_rows = Vec::new();
        for series in 0..SERIES_PER_BLOCK {
            let labels = labels(&[("__name__", "soak_metric"), ("series", &series.to_string())]);
            let fingerprint = labels.fingerprint();
            series_labels.insert(fingerprint, labels);
            float_rows.push(FloatRow {
                fingerprint,
                timestamp_ms: QUERY_TIME_MS - 1_000 * (block + 1),
                value: 1.0,
                start_timestamp_ms: None,
            });
        }
        let rows = TenantCompactionRows {
            tenant: "tenant-a".to_string(),
            series_labels,
            float_rows,
            histogram_rows: Vec::new(),
            exemplar_rows: Vec::new(),
            metadata_rows: Vec::new(),
            clock_rows: Vec::new(),
        };
        write_compacted_tenant_blocks(&writer, &sink, &rows, block, block)
            .await
            .unwrap();
    }
    list_compaction_manifests(object_store).await.unwrap()
}

/// The requests a cold query and then a warm query made over `blocks` blocks.
async fn query_requests(blocks: i64) -> (RequestCounts, RequestCounts) {
    let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let manifests = write_float_only_blocks(&backing, blocks).await;
    let (counting, counts) = CountingObjectStore::wrap(backing);
    let base = url::Url::parse("memory:///").unwrap();
    let store = MetricBlockStore::from_compaction_manifests(
        BlockStore::new(Arc::clone(&counting), base.clone()),
        Some(BlockStore::new(counting, base)),
        &manifests,
    );
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let expected = QueryResult::InstantVector(vec![InstantSample {
        labels: Labels::new(),
        ts_ms: QUERY_TIME_MS,
        value: SampleValue::Float(3.0),
        drop_name: false,
    }]);

    let mut per_query = Vec::new();
    for _ in 0..2 {
        let before = *counts.lock().unwrap();
        let result = engine
            .query_instant(&tenant_id("tenant-a"), QUERY, QUERY_TIME_MS)
            .await
            .unwrap();
        assert2::assert!(result == expected);
        per_query.push(counts.lock().unwrap().since(before));
    }
    (per_query[0], per_query[1])
}

#[tokio::test]
pub(crate) async fn an_instant_query_reads_each_float_block_a_fixed_number_of_times() {
    // Per block, each query makes one `head`: it finds a block that deletion
    // removed and validates a cached footer. The cold query then reads the
    // footer and its page index as one range and the column chunks as a
    // second. The warm query has the footer cached and reads only the column
    // chunks. Nothing lists, and nothing reads a whole object.
    for blocks in [1_usize, 10] {
        let (cold, warm) = query_requests(i64::try_from(blocks).unwrap()).await;
        check!(
            (blocks, cold, warm)
                == (
                    blocks,
                    RequestCounts::per_block(blocks, 1, 2),
                    RequestCounts::per_block(blocks, 1, 1),
                )
        );
    }
}
