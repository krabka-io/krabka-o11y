use super::*;

#[tokio::test]
pub(crate) async fn object_store_stream_query_batches_cold_block_reads() {
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

    let plan = plan_stream_query(
        tenant,
        TimeRange::new(0, 39).unwrap(),
        parse_query(r#"{app="api"} |= "error""#).unwrap(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let scan = execute_stream_query_from_object_store_with_hot_tail_frontier_and_scan_options(
        Arc::new(store.clone()),
        &prefix,
        &plan,
        &label_index,
        QueryHotTail {
            records: &[],
            frontier: &CompactionFrontier::new(i64::MAX),
            delete_filters: &[],
        },
        StreamScanOptions::from_stream_options(LokiDirection::Forward, Some(100), None, None),
    )
    .await
    .unwrap();

    assert_eq!(scan.scanned_blocks.len(), 4);
    assert!(
        store.max_active_gets() > 1,
        "expected cold block reads to overlap, max_active_gets={}",
        store.max_active_gets()
    );
}
