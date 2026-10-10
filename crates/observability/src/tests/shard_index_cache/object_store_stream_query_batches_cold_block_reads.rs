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

type ColdLimitRow = (&'static str, i64, i64, i64, &'static str);
type HotLimitRow = (&'static str, i64, &'static str);

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
    if expected_scanned > 1 && options.limit.is_some_and(|limit| limit > 1) {
        check!(store.max_active_gets() > 1);
    }
}

#[tokio::test]
async fn limited_cold_hot_scan_matches_independent_timestamp_ledger() {
    let query = r#"{app=~"a|b"} |= "error""#;
    check_limit_scan(LimitScanCase {
        cold: &[
            ("a", 1, 100, 100, "error first"),
            ("a", 2, 3, 2, "error earlier"),
        ],
        hot: &[],
        options: LokiStreamOptions {
            direction: LokiDirection::Forward,
            limit: Some(1),
            interval: None,
        },
        end_exclusive: None,
        query,
        expected: &[("a", 2, "error earlier")],
        expected_scanned: 2,
    })
    .await;
    check_limit_scan(LimitScanCase {
        cold: &[
            ("a", 1, 100, 1, "error first"),
            ("a", 90, 99, 99, "error later"),
        ],
        hot: &[],
        options: LokiStreamOptions {
            direction: LokiDirection::Backward,
            limit: Some(1),
            interval: None,
        },
        end_exclusive: None,
        query,
        expected: &[("a", 99, "error later")],
        expected_scanned: 2,
    })
    .await;
    check_limit_scan(LimitScanCase {
        cold: &[("a", 100, 100, 100, "error cold")],
        hot: &[("a", 2, "error hot")],
        options: LokiStreamOptions {
            direction: LokiDirection::Forward,
            limit: Some(1),
            interval: None,
        },
        end_exclusive: None,
        query,
        expected: &[("a", 2, "error hot")],
        expected_scanned: 0,
    })
    .await;
    check_limit_scan(LimitScanCase {
        cold: &[("a", 100, 100, 100, "error cold")],
        hot: &[("a", 1, "error hot")],
        options: LokiStreamOptions {
            direction: LokiDirection::Backward,
            limit: Some(1),
            interval: None,
        },
        end_exclusive: None,
        query,
        expected: &[("a", 100, "error cold")],
        expected_scanned: 1,
    })
    .await;
    for direction in [LokiDirection::Forward, LokiDirection::Backward] {
        check_limit_scan(LimitScanCase {
            cold: &[("a", 10, 10, 10, "error cold")],
            hot: &[("b", 10, "error hot")],
            options: LokiStreamOptions {
                direction,
                limit: Some(1),
                interval: None,
            },
            end_exclusive: None,
            query,
            expected: &[("a", 10, "error cold")],
            expected_scanned: 1,
        })
        .await;
        check_limit_scan(LimitScanCase {
            cold: &[("a", 10, 10, 10, "error cold")],
            hot: &[("a", 10, "error hot")],
            options: LokiStreamOptions {
                direction,
                limit: Some(1),
                interval: None,
            },
            end_exclusive: None,
            query,
            expected: &[("a", 10, "error cold")],
            expected_scanned: 1,
        })
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
        expected: &[("a", 1, "error one"), ("a", 11, "error eleven")],
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
        expected: &[("a", 31, "error thirty-one"), ("a", 21, "error twenty-one")],
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
        expected: &[("a", 1, "error one")],
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
            ("a", 1, "error one"),
            ("a", 11, "error eleven"),
            ("a", 21, "error twenty-one"),
            ("a", 31, "error thirty-one"),
        ],
        expected_scanned: 4,
    })
    .await;
    check_limit_scan(LimitScanCase {
        cold: &cold,
        hot: &[],
        options: LokiStreamOptions {
            direction: LokiDirection::Backward,
            limit: Some(1),
            interval: None,
        },
        end_exclusive: None,
        query: r#"{app="a"} |= "error" | distinct app"#,
        expected: &[("a", 1, "error one")],
        expected_scanned: 4,
    })
    .await;
    check_limit_scan(LimitScanCase {
        cold: &cold,
        hot: &[],
        options: LokiStreamOptions {
            direction: LokiDirection::Backward,
            limit: Some(1),
            interval: None,
        },
        end_exclusive: Some(20),
        query,
        expected: &[("a", 11, "error eleven")],
        expected_scanned: 3,
    })
    .await;
}
