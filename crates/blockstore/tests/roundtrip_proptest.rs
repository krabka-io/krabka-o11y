//! Property tests for the block store.
//!
//! `write_then_read_preserves_all_rows` (logs, blockstore plan Task 8)
//! generates arbitrary log rows, writes them to a Parquet block, reads them
//! back, and asserts that the round-trip keeps every row, as a multiset, and
//! the descriptor's fingerprint set. The deterministic example-based
//! round-trips live in `tests/parquet.rs`. This file is the generative
//! complement.
//!
//! `equality_matcher_returns_only_matching_series` writes a block and asserts an
//! equality matcher returns exactly the rows whose fingerprint matches within
//! the time window.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use arrow::{
    array::{Int64Array, StringArray, UInt64Array},
    record_batch::RecordBatch,
};
use krabka_blockstore::{
    BlockKey, BlockStore, LabelMatcher, Labels, LogRow, MatchOp, TimeRange, read_log_block,
    write_log_block,
};
use object_store::{ObjectStore, memory::InMemory};
use proptest::prelude::*;

#[path = "support/log_schema.rs"]
mod log_schema;

/// A total order over a row's full contents, so the read-back, which is sorted
/// by `(fingerprint, timestamp)`, and the input compare as multisets even when
/// two rows share a `(fingerprint, timestamp)` pair.
#[derive(Eq, Ord, PartialEq, PartialOrd)]
struct LogRowSortKey {
    series_fingerprint: u64,
    timestamp_ns: i64,
    line: String,
    structured_metadata: BTreeMap<String, String>,
}

fn row_sort_key(row: &LogRow) -> LogRowSortKey {
    LogRowSortKey {
        series_fingerprint: row.series_fingerprint,
        timestamp_ns: row.timestamp_ns,
        line: row.line.clone(),
        structured_metadata: row.structured_metadata.clone(),
    }
}

#[derive(Clone, Debug)]
struct GeneratedLogRow {
    series_fingerprint: u64,
    timestamp_ns: i64,
    line: String,
    structured_metadata: BTreeMap<String, String>,
}

prop_compose! {
    fn arb_row()(
        series_fingerprint in any::<u64>(),
        timestamp_ns in 0_i64..1_000_000_000_000_i64,
        line in "[a-zA-Z0-9 ,.:|=_/-]{0,40}",
        structured_metadata in proptest::collection::btree_map(
            "[a-zA-Z0-9_]{1,12}", "[a-zA-Z0-9_ ]{0,20}", 0..4_usize
        ),
    ) -> GeneratedLogRow {
        GeneratedLogRow { series_fingerprint, timestamp_ns, line, structured_metadata }
    }
}

#[derive(Clone, Debug)]
struct GeneratedMatcherRow {
    is_api: bool,
    timestamp: i64,
    line: String,
}

prop_compose! {
    fn arb_matcher_row()(
        is_api in any::<bool>(),
        timestamp in 0_i64..1_000_i64,
        line in "[a-z]{1,8}",
    ) -> GeneratedMatcherRow {
        GeneratedMatcherRow { is_api, timestamp, line }
    }
}

fn arb_rows() -> impl Strategy<Value = Vec<GeneratedMatcherRow>> {
    proptest::collection::vec(arb_matcher_row(), 1..40)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn write_then_read_preserves_all_rows(raw in proptest::collection::vec(arb_row(), 1..40)) {
        let dir = tempfile::tempdir().unwrap();

        let min_ts = raw.iter().map(|row| row.timestamp_ns).min().unwrap();
        let max_ts = raw.iter().map(|row| row.timestamp_ns).max().unwrap();
        let key = BlockKey::new(
            "tenant-prop",
            0,
            0,
            i64::try_from(raw.len()).unwrap(),
            TimeRange::new(min_ts, max_ts).unwrap(),
        );

        let rows: Vec<LogRow> = raw
            .iter()
            .map(|row| LogRow::new(
                row.series_fingerprint,
                row.timestamp_ns,
                row.line.clone(),
                row.structured_metadata.clone(),
            ))
            .collect();

        let descriptor = write_log_block(dir.path(), &key, rows.clone()).unwrap();

        let expected_fingerprints: BTreeSet<u64> =
            rows.iter().map(|row| row.series_fingerprint).collect();
        prop_assert_eq!(&descriptor.fingerprints, &expected_fingerprints);

        let mut got = read_log_block(dir.path(), &key).unwrap();
        let mut want = rows;
        got.sort_by_key(row_sort_key);
        want.sort_by_key(row_sort_key);

        prop_assert_eq!(got, want);
    }

    #[test]
    #[allow(clippy::items_after_statements)]
    fn equality_matcher_returns_only_matching_series(rows in arb_rows()) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let mut api = Labels::new();
            api.insert("app", "api");
            let api_fp = api.fingerprint();
            let mut web = Labels::new();
            web.insert("app", "web");
            let web_fp = web.fingerprint();

            #[derive(Eq, Ord, PartialEq, PartialOrd)]
            struct MatcherRow {
                fingerprint: u64,
                timestamp: i64,
                line: String,
            }

            let mut sorted: Vec<MatcherRow> = rows
                .iter()
                .map(|row| {
                    MatcherRow {
                        fingerprint: if row.is_api { api_fp } else { web_fp },
                        timestamp: row.timestamp,
                        line: row.line.clone(),
                    }
                })
                .collect();
            sorted.sort();

            let expected_api_count = sorted
                .iter()
                .filter(|row| row.fingerprint == api_fp)
                .count();

            let fps = UInt64Array::from(sorted.iter().map(|row| row.fingerprint).collect::<Vec<_>>());
            let timestamps = Int64Array::from(sorted.iter().map(|row| row.timestamp).collect::<Vec<_>>());
            let lines = StringArray::from(
                sorted.iter().map(|row| row.line.as_str()).collect::<Vec<_>>(),
            );
            let batch = RecordBatch::try_new(
                log_schema::schema(),
                vec![Arc::new(fps), Arc::new(timestamps), Arc::new(lines)],
            )
            .unwrap();

            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let base = url::Url::parse("memory:///").unwrap();
            let mut block_store = BlockStore::new(store, base);
            let meta = block_store
                .writer()
                .write_block("t", "b.parquet", log_schema::schema(), &[batch])
                .await
                .unwrap();
            block_store.index_mut().add_series("t", api_fp, &api);
            block_store.index_mut().add_series("t", web_fp, &web);
            block_store.index_mut().add_block(&meta);

            let registered = block_store
                .scan_context(
                    "t",
                    &[LabelMatcher::new("app", MatchOp::Eq, "api")],
                    i64::MIN,
                    i64::MAX,
                    log_schema::schema(),
                )
                .await
                .unwrap();
            let ctx = &registered.context;
            let table = &registered.table;
            let df = ctx
                .sql(&format!(
                    "SELECT count(*) AS c FROM {table} WHERE series_fingerprint = {api_fp}"
                ))
                .await
                .unwrap();
            let out = df.collect().await.unwrap();
            let count = out[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0);

            prop_assert_eq!(usize::try_from(count).unwrap(), expected_api_count);
            Ok(())
        })?;
    }
}
