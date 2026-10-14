use serde_json::{Value, json};

use super::*;
use crate::{LokiStreamOptions, WalLogRecord, apply_loki_stream_options};

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
        StreamScanOptions::from_stream_options(
            LokiStreamOptions {
                direction: LokiDirection::Forward,
                limit: Some(100),
                interval: None,
            },
            None,
        ),
    )
    .await
    .unwrap();

    check!(scan.scanned_blocks.len() == 4);
    assert!(
        store.max_active_gets() > 1,
        "expected cold block reads to overlap, max_active_gets={}",
        store.max_active_gets()
    );
}

struct ColdLimitRow {
    app: &'static str,
    start: i64,
    end: i64,
    timestamp: i64,
    line: &'static str,
}

struct HotLimitRow {
    app: &'static str,
    timestamp: i64,
    line: &'static str,
}

fn cold_row(
    app: &'static str,
    start: i64,
    end: i64,
    timestamp: i64,
    line: &'static str,
) -> ColdLimitRow {
    ColdLimitRow {
        app,
        start,
        end,
        timestamp,
        line,
    }
}

fn hot_row(app: &'static str, timestamp: i64, line: &'static str) -> HotLimitRow {
    HotLimitRow {
        app,
        timestamp,
        line,
    }
}

/// One limited scan over cold blocks and hot rows, and the entries and block
/// count it must come back with.
struct LimitScanCase<'a> {
    cold: &'a [ColdLimitRow],
    hot: &'a [HotLimitRow],
    options: LokiStreamOptions,
    end_exclusive: Option<i64>,
    query: &'a str,
    expected: &'a [HotLimitRow],
    expected_scanned: usize,
}

async fn check_limit_scan(case: LimitScanCase<'_>) {
    let LimitScanCase {
        cold,
        hot,
        options,
        end_exclusive,
        query,
        expected,
        expected_scanned,
    } = case;
    let store = RecordingObjectStore::new().with_get_delay(Duration::from_millis(25));
    let prefix = ObjectPath::from("observability/logs");
    let tenant = "tenant-a";
    let mut labels = LabelIndex::default();
    let mut blocks = BlockIndex::default();
    for offset in 0..cold.len() {
        let row = &cold[offset];
        let fingerprint = labels.insert_series(
            tenant,
            krabka_blockstore::labels([krabka_blockstore::LabelPair::new("app", row.app)]),
        );
        let offset = i64::try_from(offset).unwrap();
        let block = write_log_block_to_object_store(
            &store,
            &prefix,
            &BlockKey::new(
                tenant,
                0,
                offset,
                offset,
                TimeRange::new(row.start, row.end).unwrap(),
            ),
            vec![LogRow::new(
                fingerprint,
                row.timestamp,
                row.line,
                BTreeMap::new(),
            )],
        )
        .await
        .unwrap();
        blocks.insert(block);
    }
    let hot = hot
        .iter()
        .map(|row| WalLogRecord {
            tenant: tenant.into(),
            labels: krabka_blockstore::labels([krabka_blockstore::LabelPair::new("app", row.app)]),
            timestamp_ns: row.timestamp,
            line: row.line.into(),
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
        StreamScanOptions::from_stream_options(options, end_exclusive)
            .with_block_fetch_concurrency(NonZeroUsize::new(2).unwrap()),
    )
    .await
    .unwrap();
    let actual = apply_loki_stream_options(scan.value, options, end_exclusive);
    // The response ledger is independently assigned, including every label,
    // line, stream and entry order. It does not use the scan or limit helpers.
    let mut entries: BTreeMap<&str, Vec<Value>> = BTreeMap::new();
    for row in expected {
        entries
            .entry(row.app)
            .or_default()
            .push(json!([row.timestamp.to_string(), row.line]));
    }
    let result = entries
        .keys()
        .map(|app| json!({"stream":{"app":app},"values":entries[app]}))
        .collect::<Vec<_>>();
    check!(actual == json!({"status":"success","data":{"resultType":"streams","result":result}}));
    check!(scan.scanned_blocks.len() == expected_scanned);
    if expected_scanned > 1 && options.limit.is_some_and(|limit| limit > 1) {
        check!(store.max_active_gets() > 1);
    }
}

async fn check_backward_single_result(
    cold: &[ColdLimitRow],
    query: &str,
    end_exclusive: Option<i64>,
    expected: &[HotLimitRow],
    expected_scanned: usize,
) {
    check_limit_scan(LimitScanCase {
        cold,
        hot: &[],
        options: LokiStreamOptions {
            direction: LokiDirection::Backward,
            limit: Some(1),
            interval: None,
        },
        end_exclusive,
        query,
        expected,
        expected_scanned,
    })
    .await;
}

async fn check_hot_limit_scan(direction: LokiDirection, hot_instance: &'static str) {
    check_limit_scan(LimitScanCase {
        cold: &[cold_row("a", 10, 10, 10, "error cold")],
        hot: &[hot_row(hot_instance, 10, "error hot")],
        options: LokiStreamOptions {
            direction,
            limit: Some(1),
            interval: None,
        },
        end_exclusive: None,
        query: r#"{app="a"} |= "error""#,
        expected: &[hot_row("a", 10, "error cold")],
        expected_scanned: 1,
    })
    .await;
}

#[tokio::test]
async fn limited_cold_hot_scan_matches_independent_timestamp_ledger() {
    let query = r#"{app=~"a|b"} |= "error""#;
    check_limit_scan(LimitScanCase {
        cold: &[
            cold_row("a", 1, 100, 100, "error first"),
            cold_row("a", 2, 3, 2, "error earlier"),
        ],
        hot: &[],
        options: LokiStreamOptions {
            direction: LokiDirection::Forward,
            limit: Some(1),
            interval: None,
        },
        end_exclusive: None,
        query,
        expected: &[hot_row("a", 2, "error earlier")],
        expected_scanned: 2,
    })
    .await;
    check_limit_scan(LimitScanCase {
        cold: &[
            cold_row("a", 1, 100, 1, "error first"),
            cold_row("a", 90, 99, 99, "error later"),
        ],
        hot: &[],
        options: LokiStreamOptions {
            direction: LokiDirection::Backward,
            limit: Some(1),
            interval: None,
        },
        end_exclusive: None,
        query,
        expected: &[hot_row("a", 99, "error later")],
        expected_scanned: 2,
    })
    .await;
    check_limit_scan(LimitScanCase {
        cold: &[cold_row("a", 100, 100, 100, "error cold")],
        hot: &[hot_row("a", 2, "error hot")],
        options: LokiStreamOptions {
            direction: LokiDirection::Forward,
            limit: Some(1),
            interval: None,
        },
        end_exclusive: None,
        query,
        expected: &[hot_row("a", 2, "error hot")],
        expected_scanned: 0,
    })
    .await;
    check_limit_scan(LimitScanCase {
        cold: &[cold_row("a", 100, 100, 100, "error cold")],
        hot: &[hot_row("a", 1, "error hot")],
        options: LokiStreamOptions {
            direction: LokiDirection::Backward,
            limit: Some(1),
            interval: None,
        },
        end_exclusive: None,
        query,
        expected: &[hot_row("a", 100, "error cold")],
        expected_scanned: 1,
    })
    .await;
    for direction in [LokiDirection::Forward, LokiDirection::Backward] {
        check_hot_limit_scan(direction, "b").await;
        check_hot_limit_scan(direction, "a").await;
    }
}

#[tokio::test]
async fn limited_cold_scan_prunes_only_beyond_the_timestamp_boundary() {
    let cold = [
        cold_row("a", 1, 9, 1, "error one"),
        cold_row("a", 11, 19, 11, "error eleven"),
        cold_row("a", 21, 29, 21, "error twenty-one"),
        cold_row("a", 31, 39, 31, "error thirty-one"),
    ];
    let query = r#"{app="a"} |= "error""#;
    check_limit_scan(LimitScanCase {
        cold: &cold,
        hot: &[],
        options: LokiStreamOptions {
            direction: LokiDirection::Forward,
            limit: Some(2),
            interval: None,
        },
        end_exclusive: None,
        query,
        expected: &[
            hot_row("a", 1, "error one"),
            hot_row("a", 11, "error eleven"),
        ],
        expected_scanned: 2,
    })
    .await;
    check_limit_scan(LimitScanCase {
        cold: &cold,
        hot: &[],
        options: LokiStreamOptions {
            direction: LokiDirection::Backward,
            limit: Some(2),
            interval: None,
        },
        end_exclusive: None,
        query,
        expected: &[
            hot_row("a", 31, "error thirty-one"),
            hot_row("a", 21, "error twenty-one"),
        ],
        expected_scanned: 2,
    })
    .await;
    check_limit_scan(LimitScanCase {
        cold: &cold,
        hot: &[],
        options: LokiStreamOptions {
            direction: LokiDirection::Forward,
            limit: Some(0),
            interval: None,
        },
        end_exclusive: None,
        query,
        expected: &[],
        expected_scanned: 0,
    })
    .await;
    check_limit_scan(LimitScanCase {
        cold: &cold,
        hot: &[],
        options: LokiStreamOptions {
            direction: LokiDirection::Forward,
            limit: Some(1),
            interval: Some(1),
        },
        end_exclusive: None,
        query,
        expected: &[hot_row("a", 1, "error one")],
        expected_scanned: 4,
    })
    .await;
    check_limit_scan(LimitScanCase {
        cold: &cold,
        hot: &[],
        options: LokiStreamOptions {
            direction: LokiDirection::Forward,
            limit: None,
            interval: None,
        },
        end_exclusive: None,
        query,
        expected: &[
            hot_row("a", 1, "error one"),
            hot_row("a", 11, "error eleven"),
            hot_row("a", 21, "error twenty-one"),
            hot_row("a", 31, "error thirty-one"),
        ],
        expected_scanned: 4,
    })
    .await;
    check_backward_single_result(
        &cold,
        r#"{app="a"} |= "error" | distinct app"#,
        None,
        &[hot_row("a", 1, "error one")],
        4,
    )
    .await;
    check_backward_single_result(
        &cold,
        query,
        Some(20),
        &[hot_row("a", 11, "error eleven")],
        3,
    )
    .await;
}
