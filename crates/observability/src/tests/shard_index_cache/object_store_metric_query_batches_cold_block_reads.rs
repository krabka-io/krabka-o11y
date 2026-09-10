use super::*;

/// Metric queries fetch their cold blocks the way stream queries do.
///
/// A `get_delay` of 25 ms per object is what makes the difference visible: one
/// round trip per block, serially, would never put two in flight at once, and
/// `max_active_gets` would stay at one however many blocks the query planned.
#[tokio::test]
pub(crate) async fn object_store_metric_query_batches_cold_block_reads() {
    let store = RecordingObjectStore::new().with_get_delay(Duration::from_millis(25));
    let prefix = ObjectPath::from("observability/logs");
    let tenant = "tenant-a";
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series(tenant, krabka_blockstore::labels([("app", "api")]));
    let mut block_index = BlockIndex::default();

    for block_id in 0_i64..4 {
        let start_ns = block_id * 10;
        let end_ns = start_ns + 9;
        let block = write_log_block_to_object_store(
            &store,
            &prefix,
            &BlockKey::new(
                tenant,
                0,
                start_ns,
                end_ns,
                TimeRange::new(start_ns, end_ns).unwrap(),
            ),
            vec![LogRow::new(
                api,
                end_ns,
                format!("api error {block_id}"),
                BTreeMap::new(),
            )],
        )
        .await
        .unwrap();
        block_index.insert(block);
    }

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
        QueryHotTail {
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
