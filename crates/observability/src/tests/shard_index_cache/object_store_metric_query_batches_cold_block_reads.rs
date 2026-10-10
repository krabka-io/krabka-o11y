use super::*;

/// Metric queries fetch their cold blocks the way stream queries do.
///
/// A `get_delay` of 25 ms per object is what makes the difference visible: one
/// round trip per block, serially, would never put two in flight at once, and
/// `max_active_gets` would stay at one however many blocks the query planned.
#[tokio::test]
pub(crate) async fn object_store_metric_query_batches_cold_block_reads() {
    let FourColdApiBlocks {
        store,
        prefix,
        label_index,
        block_index,
    } = FourColdApiBlocks::write().await;
    let tenant = "tenant-a";

    let query = parse_metric_query(r#"count_over_time({app="api"} |= "error" [40ns])"#).unwrap();
    let plan = plan_stream_query(
        tenant,
        TimeRange::new(0, 39).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();
    check!(plan.blocks.len() == 4);

    let response = execute_metric_query_range_from_object_store_with_hot_tail_frontier_and_deletes(
        ColdBlockScan {
            store: Arc::new(store.clone()),
            prefix: &prefix,
            block_fetch_concurrency: NonZeroUsize::new(4).unwrap(),
        },
        &plan,
        &query,
        &label_index,
        (TimeRange::new(39, 39).unwrap(), 1),
        QueryHotTail::<crate::WalLogRecord> {
            records: &[],
            frontier: &CompactionFrontier::new(i64::MAX),
            delete_filters: &[],
        },
    )
    .await
    .unwrap();

    check!(
        response
            .pointer("/data/result/0/values/0/1")
            .and_then(serde_json::Value::as_str)
            == Some("4")
    );
    check!(
        store.max_active_gets() > 1,
        "expected cold block reads to overlap, max_active_gets={}",
        store.max_active_gets()
    );
}
