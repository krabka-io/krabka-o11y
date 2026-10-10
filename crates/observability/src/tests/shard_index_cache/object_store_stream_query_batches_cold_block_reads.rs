use serde_json::{Value, json};

use super::*;
use crate::{WalLogRecord, apply_loki_stream_options};

#[tokio::test]
pub(crate) async fn object_store_stream_query_batches_cold_block_reads() {
    let FourColdApiBlocks {
        store,
        prefix,
        label_index,
        block_index,
    } = FourColdApiBlocks::write().await;
    let tenant = "tenant-a";

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
        QueryHotTail::<crate::WalLogRecord> {
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

type ColdLimitRow = (&'static str, i64, i64, i64, &'static str);
type HotLimitRow = (&'static str, i64, &'static str);

async fn check_limit_scan(
    cold: &[ColdLimitRow],
    hot: &[HotLimitRow],
    options: (LokiDirection, Option<usize>, Option<i64>, Option<i64>),
    query: &str,
    expected: &[HotLimitRow],
    expected_scanned: usize,
) {
    let store = RecordingObjectStore::new().with_get_delay(Duration::from_millis(25));
    let prefix = ObjectPath::from("observability/logs");
    let tenant = "tenant-a";
    let mut labels = LabelIndex::default();
    let mut blocks = BlockIndex::default();
    for (offset, &(app, start, end, timestamp, line)) in cold.iter().enumerate() {
        let fingerprint = labels.insert_series(tenant, krabka_blockstore::labels([("app", app)]));
        let offset = i64::try_from(offset).unwrap();
        let block = write_log_block_to_object_store(
            &store,
            &prefix,
            &BlockKey::new(
                tenant,
                0,
                offset,
                offset,
                TimeRange::new(start, end).unwrap(),
            ),
            vec![LogRow::new(fingerprint, timestamp, line, BTreeMap::new())],
        )
        .await
        .unwrap();
        blocks.insert(block);
    }
    let hot = hot
        .iter()
        .map(|&(app, timestamp_ns, line)| WalLogRecord {
            tenant: tenant.into(),
            labels: krabka_blockstore::labels([("app", app)]),
            timestamp_ns,
            line: line.into(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .collect::<Vec<_>>();
    let plan = plan_stream_query(
        tenant,
        TimeRange::new(0, 400).unwrap(),
        parse_query(query).unwrap(),
        &labels,
        &blocks,
    )
    .unwrap();
    let (direction, limit, interval, end) = options;
    let scan = execute_stream_query_from_object_store_with_hot_tail_frontier_and_scan_options(
        Arc::new(store.clone()),
        &prefix,
        &plan,
        &labels,
        QueryHotTail {
            records: &hot,
            frontier: &CompactionFrontier::new(i64::MIN),
            delete_filters: &[],
        },
        StreamScanOptions::from_stream_options(direction, limit, interval, end)
            .with_block_fetch_concurrency(NonZeroUsize::new(2).unwrap()),
    )
    .await
    .unwrap();
    let actual = apply_loki_stream_options(scan.value, direction, limit, interval, end);
    // The response ledger is independently assigned, including every label,
    // line, stream and entry order. It does not use the scan or limit helpers.
    let mut entries: BTreeMap<&str, Vec<Value>> = BTreeMap::new();
    for &(app, timestamp, line) in expected {
        entries
            .entry(app)
            .or_default()
            .push(json!([timestamp.to_string(), line]));
    }
    let result = entries
        .into_iter()
        .map(|(app, values)| json!({"stream":{"app":app},"values":values}))
        .collect::<Vec<_>>();
    check!(actual == json!({"status":"success","data":{"resultType":"streams","result":result}}));
    check!(scan.scanned_blocks.len() == expected_scanned);
    if expected_scanned > 1 && limit.is_some_and(|limit| limit > 1) {
        check!(store.max_active_gets() > 1);
    }
}

#[tokio::test]
async fn limited_cold_hot_scan_matches_independent_timestamp_ledger() {
    let query = r#"{app=~"a|b"} |= "error""#;
    check_limit_scan(
        &[
            ("a", 1, 100, 100, "error first"),
            ("a", 2, 3, 2, "error earlier"),
        ],
        &[],
        (LokiDirection::Forward, Some(1), None, None),
        query,
        &[("a", 2, "error earlier")],
        2,
    )
    .await;
    check_limit_scan(
        &[
            ("a", 1, 100, 1, "error first"),
            ("a", 90, 99, 99, "error later"),
        ],
        &[],
        (LokiDirection::Backward, Some(1), None, None),
        query,
        &[("a", 99, "error later")],
        2,
    )
    .await;
    check_limit_scan(
        &[("a", 100, 100, 100, "error cold")],
        &[("a", 2, "error hot")],
        (LokiDirection::Forward, Some(1), None, None),
        query,
        &[("a", 2, "error hot")],
        0,
    )
    .await;
    check_limit_scan(
        &[("a", 100, 100, 100, "error cold")],
        &[("a", 1, "error hot")],
        (LokiDirection::Backward, Some(1), None, None),
        query,
        &[("a", 100, "error cold")],
        1,
    )
    .await;
    for direction in [LokiDirection::Forward, LokiDirection::Backward] {
        check_limit_scan(
            &[("a", 10, 10, 10, "error cold")],
            &[("b", 10, "error hot")],
            (direction, Some(1), None, None),
            query,
            &[("a", 10, "error cold")],
            1,
        )
        .await;
        check_limit_scan(
            &[("a", 10, 10, 10, "error cold")],
            &[("a", 10, "error hot")],
            (direction, Some(1), None, None),
            query,
            &[("a", 10, "error cold")],
            1,
        )
        .await;
    }
}

#[tokio::test]
async fn limited_cold_scan_prunes_only_beyond_the_timestamp_boundary() {
    let cold = [
        ("a", 1, 9, 1, "error one"),
        ("a", 11, 19, 11, "error eleven"),
        ("a", 21, 29, 21, "error twenty-one"),
        ("a", 31, 39, 31, "error thirty-one"),
    ];
    let query = r#"{app="a"} |= "error""#;
    check_limit_scan(
        &cold,
        &[],
        (LokiDirection::Forward, Some(2), None, None),
        query,
        &[("a", 1, "error one"), ("a", 11, "error eleven")],
        2,
    )
    .await;
    check_limit_scan(
        &cold,
        &[],
        (LokiDirection::Backward, Some(2), None, None),
        query,
        &[("a", 31, "error thirty-one"), ("a", 21, "error twenty-one")],
        2,
    )
    .await;
    check_limit_scan(
        &cold,
        &[],
        (LokiDirection::Forward, Some(0), None, None),
        query,
        &[],
        0,
    )
    .await;
    check_limit_scan(
        &cold,
        &[],
        (LokiDirection::Forward, Some(1), Some(1), None),
        query,
        &[("a", 1, "error one")],
        4,
    )
    .await;
    check_limit_scan(
        &cold,
        &[],
        (LokiDirection::Forward, None, None, None),
        query,
        &[
            ("a", 1, "error one"),
            ("a", 11, "error eleven"),
            ("a", 21, "error twenty-one"),
            ("a", 31, "error thirty-one"),
        ],
        4,
    )
    .await;
    check_limit_scan(
        &cold,
        &[],
        (LokiDirection::Backward, Some(1), None, None),
        r#"{app="a"} |= "error" | distinct app"#,
        &[("a", 1, "error one")],
        4,
    )
    .await;
    check_limit_scan(
        &cold,
        &[],
        (LokiDirection::Backward, Some(1), None, Some(20)),
        query,
        &[("a", 11, "error eleven")],
        3,
    )
    .await;
}
