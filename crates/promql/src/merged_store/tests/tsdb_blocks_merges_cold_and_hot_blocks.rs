use super::*;

#[tokio::test]
pub(crate) async fn tsdb_blocks_merges_cold_and_hot_blocks() {
    let mut cold = InMemoryMetricStore::new();
    let mut hot = InMemoryMetricStore::new();
    cold.push_tsdb_block("tenant-a", "cold-b", 30_000, 40_000, 3, 1);
    hot.push_tsdb_block("tenant-a", "hot-a", 10_000, 20_000, 2, 1);
    cold.push_tsdb_block("tenant-a", "cold-a", 10_000, 15_000, 1, 1);

    let store = MergedMetricStore::new(cold, hot);
    let blocks = store.tsdb_blocks("tenant-a").await.unwrap();
    let ids = blocks
        .iter()
        .map(|block| block.id.as_str())
        .collect::<Vec<_>>();

    assert2::assert!(ids == vec!["cold-a", "hot-a", "cold-b"]);
}
