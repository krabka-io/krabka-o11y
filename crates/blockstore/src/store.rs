//! Query facade over object storage, index pruning, and `DataFusion` scans.

use std::{collections::BTreeSet, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::{
    catalog::MemTable,
    execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder},
    prelude::{Expr, ParquetReadOptions, SessionConfig, SessionContext, col, lit},
};
use futures::{StreamExt, TryStreamExt, stream};
use krabka_units::prelude::{ByteSize, ByteSizeExt};
use object_store::ObjectStore;
use tracing::instrument;
use url::Url;

use crate::{
    block::{COL_FINGERPRINT, COL_TIMESTAMP},
    error::{BlockStoreError, Result, SkippedBlock},
    index::Index,
    labels::SeriesFingerprint,
    matcher::LabelMatcher,
    reader::{
        BlockMetadataCache, DEFAULT_BLOCK_METADATA_CACHE_MAX, DEFAULT_BLOCK_READ_MAX, RowGroupMeta,
        block_metadata, read_block_row_groups_cached, row_group_metadata,
    },
    writer::BlockWriter,
};

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use arrow::{
        array::{Int64Array, LargeStringArray, StringArray, StringViewArray, UInt64Array},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    };
    use datafusion::physical_plan::ExecutionPlan;
    use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path as ObjectPath};

    use super::*;
    use crate::{
        labels::Labels,
        matcher::{LabelMatcher, MatchOp},
    };

    fn log_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(crate::COL_FINGERPRINT, DataType::UInt64, false),
            Field::new(crate::COL_TIMESTAMP, DataType::Int64, false),
            Field::new("line", DataType::Utf8, true),
        ]))
    }

    async fn seeded_store() -> (BlockStore, SchemaRef) {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = url::Url::parse("memory:///").unwrap();
        let mut bs = BlockStore::new(object_store, base);
        let schema = log_schema();

        let mut api = Labels::new();
        api.insert("app", "api");
        let fp = api.fingerprint();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![fp, fp])),
                Arc::new(Int64Array::from(vec![100_i64, 200])),
                Arc::new(StringArray::from(vec!["hello", "world"])),
            ],
        )
        .unwrap();

        let meta = bs
            .writer()
            .write_block("t", "blocks/b1.parquet", schema.clone(), &[batch])
            .await
            .unwrap();
        bs.index_mut().add_series("t", fp, &api);
        bs.index_mut().add_block(&meta);
        (bs, schema)
    }

    #[tokio::test]
    async fn from_config_inmemory_builds_usable_store() {
        use krabka_object_store::ObjectStoreConfig;

        let base = url::Url::parse("memory:///").unwrap();
        let bs = BlockStore::from_config(&ObjectStoreConfig::InMemory, base).unwrap();
        let store = bs.object_store();
        let path = ObjectPath::from("t/x");
        store
            .put(&path, object_store::PutPayload::from(b"hi".to_vec()))
            .await
            .unwrap();
        let got = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert2::assert!(&got[..] == b"hi");
    }

    #[tokio::test]
    async fn scan_returns_rows_for_matching_series() {
        let (bs, schema) = seeded_store().await;
        let matchers = [LabelMatcher::new("app", MatchOp::Eq, "api")];

        let (ctx, table) = bs
            .scan_context("t", &matchers, 0, 1_000, schema)
            .await
            .unwrap();

        let df = ctx
            .sql(&format!("SELECT line FROM {table} ORDER BY timestamp"))
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        let first = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| a.value(0))
            .or_else(|| {
                batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .map(|a| a.value(0))
            })
            .or_else(|| {
                batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .map(|a| a.value(0))
            })
            .expect("line column is utf8");
        assert2::assert!(total == 2);
        assert2::assert!(first == "hello");
    }

    /// Two series in one block, spread over four timestamps each, so a scan can
    /// narrow by series, by window, or by both.
    async fn two_series_store() -> (BlockStore, SchemaRef, Labels, Labels) {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = url::Url::parse("memory:///").unwrap();
        let mut bs = BlockStore::new(object_store, base);
        let schema = log_schema();

        let mut api = Labels::new();
        api.insert("app", "api");
        let mut web = Labels::new();
        web.insert("app", "web");

        let fps = [api.fingerprint(); 4]
            .into_iter()
            .chain([web.fingerprint(); 4])
            .collect::<Vec<_>>();
        let timestamps = [100_i64, 200, 300, 400, 100, 200, 300, 400];
        let lines = ["a1", "a2", "a3", "a4", "w1", "w2", "w3", "w4"];
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(fps)),
                Arc::new(Int64Array::from(timestamps.to_vec())),
                Arc::new(StringArray::from(lines.to_vec())),
            ],
        )
        .unwrap();

        let meta = bs
            .writer()
            .write_block("t", "blocks/two.parquet", schema.clone(), &[batch])
            .await
            .unwrap();
        bs.index_mut().add_series("t", api.fingerprint(), &api);
        bs.index_mut().add_series("t", web.fingerprint(), &web);
        bs.index_mut().add_block(&meta);
        (bs, schema, api, web)
    }

    async fn scanned_lines(bs: &BlockStore, schema: SchemaRef, app: &str) -> Vec<String> {
        let matchers = [LabelMatcher::new("app", MatchOp::Eq, app)];
        let (ctx, table) = bs
            .scan_context("t", &matchers, 200, 300, schema)
            .await
            .unwrap();
        let df = ctx
            .sql(&format!("SELECT line FROM {table} ORDER BY line"))
            .await
            .unwrap();
        df.collect()
            .await
            .unwrap()
            .iter()
            .flat_map(|batch| {
                let lines = batch.column(0).as_any().downcast_ref::<StringArray>();
                (0..batch.num_rows())
                    .map(|row| lines.expect("line column is utf8").value(row).to_string())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// The rows the deepest node of `plan` emitted, which for a scan over
    /// registered blocks is the Parquet source itself.
    fn source_output_rows(plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
        match plan.children().first() {
            Some(child) => source_output_rows(child),
            None => plan.metrics()?.output_rows(),
        }
    }

    #[tokio::test]
    async fn a_narrow_window_does_not_decode_the_whole_block() {
        // Three row groups' worth of one series, a millisecond apart, and a scan
        // asking for the first ten milliseconds of it.
        const ROWS: i64 = 200_001;

        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = url::Url::parse("memory:///").unwrap();
        let mut bs = BlockStore::new(object_store, base);
        let schema = Arc::new(Schema::new(vec![
            Field::new(crate::COL_FINGERPRINT, DataType::UInt64, false),
            Field::new(crate::COL_TIMESTAMP, DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ])) as SchemaRef;

        let mut api = Labels::new();
        api.insert("app", "api");
        let fp = api.fingerprint();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from_iter_values(std::iter::repeat_n(
                    fp,
                    usize::try_from(ROWS).unwrap(),
                ))),
                Arc::new(Int64Array::from_iter_values(0..ROWS)),
                // A payload column, so the scan has something to leave
                // undecoded. Its values alternate rather than repeat, so
                // Parquet cannot collapse the column to a single dictionary
                // entry.
                Arc::new(arrow::array::Float64Array::from_iter_values(
                    (0..ROWS).map(|row| if row % 2 == 0 { 1.0 } else { 2.0 }),
                )),
            ],
        )
        .unwrap();
        let meta = bs
            .writer()
            .write_block("t", "blocks/wide.parquet", schema.clone(), &[batch])
            .await
            .unwrap();
        bs.index_mut().add_series("t", fp, &api);
        bs.index_mut().add_block(&meta);

        let matchers = [LabelMatcher::new("app", MatchOp::Eq, "api")];
        let (ctx, table) = bs.scan_context("t", &matchers, 0, 9, schema).await.unwrap();
        let plan = ctx
            .sql(&format!("SELECT timestamp FROM {table}"))
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let batches = datafusion::physical_plan::collect(Arc::clone(&plan), ctx.task_ctx())
            .await
            .unwrap();

        let matched: usize = batches.iter().map(RecordBatch::num_rows).sum();
        let scanned = source_output_rows(&plan).expect("the source records its output rows");
        assert2::assert!(matched == 10);
        // Without a predicate on the scan, every one of the block's 200,001 rows
        // is decoded and the window is applied afterwards. With one, the source
        // hands up only the rows that answer the query: two of the three row
        // groups never load, and inside the third the reader decodes the
        // timestamp column, applies the predicate, and materializes nothing
        // else.
        assert2::assert!(scanned == matched);
    }

    #[tokio::test]
    async fn scan_reaches_the_parquet_source_with_both_predicates() {
        // The block holds eight rows; the scan asks for one series over two of
        // the four timestamps. Without the pushed-down predicate every row of
        // the block would come back and the caller would filter afterwards.
        let (bs, schema, _api, _web) = two_series_store().await;
        assert2::assert!(scanned_lines(&bs, schema.clone(), "api").await == ["a2", "a3"]);
        assert2::assert!(scanned_lines(&bs, schema, "web").await == ["w2", "w3"]);
    }

    #[test]
    fn scan_filter_bounds_time_and_series() {
        let fingerprints = BTreeSet::from([7_u64, 11]);
        assert2::assert!(
            scan_filter(100, 200, &fingerprints)
                == Some(
                    col(COL_TIMESTAMP)
                        .between(lit(100_i64), lit(200_i64))
                        .and(col(COL_FINGERPRINT).in_list(vec![lit(7_u64), lit(11_u64)], false))
                )
        );
    }

    #[test]
    fn scan_filter_degrades_a_large_fingerprint_set_to_a_range() {
        // An `IN` list of a hundred thousand literals would cost more in
        // pruning than the scan it saves, so past the cap the predicate becomes
        // the enclosing range instead.
        let fingerprints = (0..=u64::try_from(MAX_FINGERPRINT_IN_LIST).unwrap()).collect();
        assert2::assert!(
            scan_filter(i64::MIN, i64::MAX, &fingerprints)
                == Some(col(COL_FINGERPRINT).between(
                    lit(0_u64),
                    lit(u64::try_from(MAX_FINGERPRINT_IN_LIST).unwrap())
                ))
        );
    }

    #[test]
    fn scan_filter_is_none_when_nothing_is_bounded() {
        assert2::assert!(scan_filter(i64::MIN, i64::MAX, &BTreeSet::new()).is_none());
    }

    #[tokio::test]
    async fn index_returns_the_stores_own_populated_index() {
        // The accessor must hand back the store's real index, not a fresh
        // default one: the seeded `app=api` series must resolve.
        let (bs, _schema) = seeded_store().await;
        let mut api = Labels::new();
        api.insert("app", "api");
        let got = bs
            .index()
            .resolve("t", &[LabelMatcher::new("app", MatchOp::Eq, "api")])
            .unwrap();
        assert2::assert!(got == std::collections::BTreeSet::from([api.fingerprint()]));
    }

    #[tokio::test]
    async fn cloned_blockstore_shares_index_until_mutated() {
        let (bs, _schema) = seeded_store().await;
        let cloned = bs.clone();
        assert2::assert!(Arc::ptr_eq(&bs.index, &cloned.index));

        let mut mutated = cloned.clone();
        let mut web = Labels::new();
        web.insert("app", "web");
        mutated.index_mut().add_series("t", web.fingerprint(), &web);

        assert2::assert!(!Arc::ptr_eq(&bs.index, &mutated.index));
        assert2::assert!(
            bs.index()
                .resolve("t", &[LabelMatcher::new("app", MatchOp::Eq, "web")])
                .unwrap()
                == std::collections::BTreeSet::new()
        );
        assert2::assert!(
            mutated
                .index()
                .resolve("t", &[LabelMatcher::new("app", MatchOp::Eq, "web")])
                .unwrap()
                == std::collections::BTreeSet::from([web.fingerprint()])
        );
    }

    #[tokio::test]
    async fn scan_block_keys_reads_named_blocks() {
        let (bs, schema) = seeded_store().await;
        let (ctx, table) = bs
            .scan_block_keys(&["blocks/b1.parquet".to_string()], schema)
            .await
            .unwrap();
        // Table name is the fixed logical name, not a stub string.
        assert2::assert!(table == "logs");
        let df = ctx.sql(&format!("SELECT line FROM {table}")).await.unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert2::assert!(total == 2);
    }

    #[tokio::test]
    async fn scan_block_row_groups_reads_selected_groups() {
        let (bs, schema) = seeded_store().await;
        let (ctx, table) = bs
            .scan_block_row_groups("blocks/b1.parquet", &[0], schema)
            .await
            .unwrap();
        assert2::assert!(table == "logs");
        let df = ctx.sql(&format!("SELECT line FROM {table}")).await.unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert2::assert!(total == 2);
    }

    #[tokio::test]
    async fn block_read_max_reaches_metadata_row_groups_and_empty_like() {
        let (bs, schema) = seeded_store().await;
        let capped = BlockStore::new_with_block_read_max(
            bs.object_store(),
            url::Url::parse("memory:///").unwrap(),
            krabka_units::bytes(1),
        );

        assert2::assert!(
            capped
                .read_row_group_metadata("blocks/b1.parquet")
                .await
                .is_err()
        );
        assert2::assert!(
            capped
                .scan_block_row_groups("blocks/b1.parquet", &[0], schema)
                .await
                .is_err()
        );
        assert2::assert!(
            capped
                .empty_like()
                .read_row_group_metadata("blocks/b1.parquet")
                .await
                .is_err()
        );
    }

    /// An object store that counts the requests made through it, and can be
    /// told to fail the way an unreachable backend does.
    ///
    /// Counting is how the footer cache is measured: a cache without a
    /// measurement is a guess. `unreachable` is how the other half of the
    /// error boundary is tested — a store that is down is not one block's
    /// fault, and must not be skipped.
    #[derive(Debug)]
    struct TestStore {
        inner: Arc<dyn ObjectStore>,
        requests: Arc<Requests>,
        unreachable: bool,
    }

    #[derive(Debug, Default)]
    struct Requests {
        heads: AtomicUsize,
        /// Reads of a tail range: how a Parquet footer is fetched.
        suffix_gets: AtomicUsize,
        /// Reads of a bounded range or a whole object: column chunks, and the
        /// second half of a footer read the first suffix did not cover.
        other_gets: AtomicUsize,
    }

    impl Requests {
        fn snapshot(&self) -> (usize, usize, usize) {
            (
                self.heads.load(Ordering::Relaxed),
                self.suffix_gets.load(Ordering::Relaxed),
                self.other_gets.load(Ordering::Relaxed),
            )
        }
    }

    impl TestStore {
        fn counting(inner: Arc<dyn ObjectStore>) -> (Arc<dyn ObjectStore>, Arc<Requests>) {
            let requests = Arc::new(Requests::default());
            let store = Arc::new(Self {
                inner,
                requests: Arc::clone(&requests),
                unreachable: false,
            });
            (store, requests)
        }

        fn unreachable(inner: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
            Arc::new(Self {
                inner,
                requests: Arc::new(Requests::default()),
                unreachable: true,
            })
        }
    }

    /// The failure an unreachable backend raises: not `NotFound`, and not
    /// about any one block.
    fn store_is_down<T>() -> object_store::Result<T> {
        Err(object_store::Error::Generic {
            store: "TestStore",
            source: "connection reset by peer".into(),
        })
    }

    impl std::fmt::Display for TestStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "TestStore({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for TestStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            if self.unreachable {
                return store_is_down();
            }
            if options.head {
                self.requests.heads.fetch_add(1, Ordering::Relaxed);
            } else if matches!(options.range, Some(object_store::GetRange::Suffix(_))) {
                self.requests.suffix_gets.fetch_add(1, Ordering::Relaxed);
            } else {
                self.requests.other_gets.fetch_add(1, Ordering::Relaxed);
            }
            self.inner.get_opts(location, options).await
        }

        async fn get_ranges(
            &self,
            location: &ObjectPath,
            ranges: &[std::ops::Range<u64>],
        ) -> object_store::Result<Vec<bytes::Bytes>> {
            if self.unreachable {
                return store_is_down();
            }
            self.requests
                .other_gets
                .fetch_add(ranges.len(), Ordering::Relaxed);
            self.inner.get_ranges(location, ranges).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectPath>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// One block per series, both indexed, so a test can break one and still
    /// have something the scan should return.
    async fn two_block_store(object_store: Arc<dyn ObjectStore>) -> (BlockStore, Vec<String>) {
        let base = url::Url::parse("memory:///").unwrap();
        let mut bs = BlockStore::new(object_store, base);
        let schema = log_schema();

        let mut api = Labels::new();
        api.insert("app", "api");
        let mut web = Labels::new();
        web.insert("app", "web");

        for (labels, key, lines) in [
            (&api, "blocks/b1.parquet", ["a1", "a2"]),
            (&web, "blocks/b2.parquet", ["w1", "w2"]),
        ] {
            let fp = labels.fingerprint();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(UInt64Array::from(vec![fp, fp])),
                    Arc::new(Int64Array::from(vec![100_i64, 200])),
                    Arc::new(StringArray::from(lines.to_vec())),
                ],
            )
            .unwrap();
            let meta = bs
                .writer()
                .write_block("t", key, schema.clone(), &[batch])
                .await
                .unwrap();
            bs.index_mut().add_series("t", fp, labels);
            bs.index_mut().add_block(&meta);
        }

        (
            bs,
            vec![
                "blocks/b1.parquet".to_string(),
                "blocks/b2.parquet".to_string(),
            ],
        )
    }

    async fn table_lines(ctx: &SessionContext, table: &str) -> Vec<String> {
        let batches = ctx
            .sql(&format!("SELECT line FROM {table} ORDER BY line"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        batches
            .iter()
            .flat_map(|batch| {
                // A scan registered with an explicit schema hands back `Utf8`
                // and one registered without hands back `Utf8View`, so read
                // the column through a cast rather than guessing.
                let lines =
                    arrow::compute::cast(batch.column(0), &DataType::Utf8).expect("line is text");
                let lines = arrow::array::AsArray::as_string::<i32>(&lines).clone();
                (0..batch.num_rows())
                    .map(|row| lines.value(row).to_string())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn skipped_keys(report: &ScanReport) -> Vec<(String, crate::BlockSkipReason)> {
        report
            .skipped
            .iter()
            .map(|block| (block.object_key.clone(), block.reason))
            .collect()
    }

    /// The two ways a single block goes bad in ordinary operation. A missing
    /// object is not hypothetical: `Index::add_block` and `Index::save` are
    /// separate steps, and compaction swaps its outputs in without deleting
    /// the inputs, so a snapshot restored from an older generation names keys
    /// a later compactor replaced.
    #[tokio::test]
    async fn a_scan_answers_from_the_readable_blocks_and_reports_the_rest() {
        let cases: [(&str, crate::BlockSkipReason, Option<Vec<u8>>); 2] = [
            ("the object is gone", crate::BlockSkipReason::Missing, None),
            (
                "the object is not a parquet block",
                crate::BlockSkipReason::Corrupt,
                Some(b"PAR1 this is not a parquet file at all".to_vec()),
            ),
        ];

        for (case, reason, replacement) in cases {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let (bs, keys) = two_block_store(store).await;
            let broken = ObjectPath::from("blocks/b2.parquet");
            match replacement {
                None => bs.object_store().delete(&broken).await.unwrap(),
                Some(bytes) => bs
                    .object_store()
                    .put(&broken, object_store::PutPayload::from(bytes))
                    .await
                    .map(|_| ())
                    .unwrap(),
            }

            let scan = bs
                .scan_block_keys_skipping_unreadable(&keys, log_schema())
                .await
                .unwrap();

            assert2::assert!(
                table_lines(&scan.ctx, &scan.table).await == ["a1", "a2"],
                "{case}"
            );
            assert2::assert!(
                skipped_keys(&scan.report) == [("blocks/b2.parquet".to_string(), reason)],
                "{case}"
            );
            assert2::assert!(scan.report.is_partial(), "{case}");
            assert2::assert!(scan.report.registered, "{case}");
            assert2::assert!(
                scan.report.warnings()[0].contains("blocks/b2.parquet"),
                "{case}"
            );
        }
    }

    /// The same break, through the index-driven scan the metrics path uses.
    #[tokio::test]
    async fn an_indexed_scan_skips_the_block_the_index_still_names() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (bs, _keys) = two_block_store(store).await;
        bs.object_store()
            .delete(&ObjectPath::from("blocks/b2.parquet"))
            .await
            .unwrap();

        let ctx = bs.session_context();
        let report = bs
            .register_scan_table_skipping_unreadable(
                &ctx,
                ScanTableRequest {
                    table_name: TABLE_NAME,
                    tenant: "t",
                    matchers: &[LabelMatcher::new("app", MatchOp::Re, "api|web")],
                    min_ts: 0,
                    max_ts: 1_000,
                    schema: log_schema(),
                },
            )
            .await
            .unwrap();

        assert2::assert!(table_lines(&ctx, TABLE_NAME).await == ["a1", "a2"]);
        assert2::assert!(
            skipped_keys(&report)
                == [(
                    "blocks/b2.parquet".to_string(),
                    crate::BlockSkipReason::Missing
                )]
        );
    }

    /// The strict scan must fail loudly rather than answer from what is left.
    ///
    /// `DataFusion` resolves each path it is handed as a listing, so a missing
    /// object contributes no files and no error, and the scan quietly returns
    /// the surviving block's rows. Silence is the worst of the three
    /// behaviours available here: worse than failing, and worse than answering
    /// with the skip attached as a warning.
    #[tokio::test]
    async fn the_strict_scan_still_fails_on_one_unreadable_block() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (bs, keys) = two_block_store(store).await;
        bs.object_store()
            .delete(&ObjectPath::from("blocks/b2.parquet"))
            .await
            .unwrap();

        let got = bs.scan_block_keys(&keys, log_schema()).await;
        let Err(error) = got else {
            panic!("a scan over a missing block must not answer as if it were empty");
        };
        let (object_key, failure) = error.unreadable_block().expect("the error names the block");
        assert2::assert!(object_key == "blocks/b2.parquet");
        assert2::assert!(failure.is_missing());
    }

    /// A store that cannot be reached says nothing about any block in it.
    /// Skipping on that would turn an outage into a confident, quietly empty
    /// answer.
    #[tokio::test]
    async fn an_unreachable_store_is_reported_as_a_failure_not_a_skip() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (bs, keys) = two_block_store(Arc::clone(&store)).await;
        let unreachable = BlockStore::new(
            TestStore::unreachable(store),
            url::Url::parse("memory:///").unwrap(),
        );

        let got = unreachable
            .scan_block_keys_skipping_unreadable(&keys, log_schema())
            .await;
        let Err(error) = got else {
            panic!("an unreachable store must not answer");
        };
        assert2::assert!(error.skipped_block() == None);
        assert2::assert!(!error.is_block_missing());
        assert2::assert!(error.unreadable_block().map(|(key, _)| key) == Some(keys[0].as_str()));
        drop(bs);
    }

    /// The footer is two round trips and a Thrift decode per block, repeated
    /// for every query and, in a range query, for every step. The second scan
    /// of the same blocks must not pay for it again.
    #[tokio::test]
    async fn a_second_scan_does_not_re_read_the_footers() {
        let (store, requests) = TestStore::counting(Arc::new(InMemory::new()));
        let (bs, keys) = two_block_store(store).await;

        let first = bs
            .scan_block_keys(&keys, log_schema())
            .await
            .unwrap()
            .0
            .sql(&format!("SELECT line FROM {TABLE_NAME}"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let after_first = requests.snapshot();

        let second = bs
            .scan_block_keys(&keys, log_schema())
            .await
            .unwrap()
            .0
            .sql(&format!("SELECT line FROM {TABLE_NAME}"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let (heads, suffix_gets, other_gets) = requests.snapshot();
        let second_scan = (
            heads - after_first.0,
            suffix_gets - after_first.1,
            other_gets - after_first.2,
        );

        // Same answer, and the footer reads the first scan paid for are not
        // repeated. The `head`s remain: they are what validates the cached
        // footer against the object as it is now, which is what keeps a
        // rewritten block from being read through a stale footer.
        assert2::assert!(
            first.iter().map(RecordBatch::num_rows).sum::<usize>()
                == second.iter().map(RecordBatch::num_rows).sum::<usize>()
        );
        // Measured on two blocks: the first scan makes 4 tail reads and 4
        // other reads, the second makes 0 and 2 — the two that remain are the
        // column chunks the query actually wants.
        assert2::assert!(after_first.1 > 0);
        assert2::assert!(second_scan.1 == 0);
        assert2::assert!(second_scan.2 < after_first.2);
        assert2::assert!(!bs.metadata_cache().is_empty());
    }

    /// A cache key without a validator is a correctness bug waiting for a
    /// rewrite, and blocks are rewritten in place when a delete request is
    /// materialized. The second scan must see the new rows, not the footer of
    /// the bytes that used to be at that key.
    #[tokio::test]
    async fn a_rewritten_block_invalidates_its_cached_footer() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (bs, _keys) = two_block_store(Arc::clone(&store)).await;
        let keys = vec!["blocks/b1.parquet".to_string()];

        let (ctx, table) = bs.scan_block_keys(&keys, log_schema()).await.unwrap();
        assert2::assert!(table_lines(&ctx, &table).await == ["a1", "a2"]);

        let schema = log_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![7_u64, 7, 7])),
                Arc::new(Int64Array::from(vec![100_i64, 200, 300])),
                Arc::new(StringArray::from(vec!["r1", "r2", "r3"])),
            ],
        )
        .unwrap();
        bs.writer()
            .write_block("t", "blocks/b1.parquet", schema, &[batch])
            .await
            .unwrap();

        let (ctx, table) = bs.scan_block_keys(&keys, log_schema()).await.unwrap();
        assert2::assert!(table_lines(&ctx, &table).await == ["r1", "r2", "r3"]);
    }

    /// The cache is bounded, and the bound is configuration rather than a
    /// number buried in the crate.
    #[test]
    fn the_footer_cache_bound_is_configurable() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = url::Url::parse("memory:///").unwrap();
        let bs = BlockStore::new(Arc::clone(&store), base.clone());
        assert2::assert!(bs.metadata_cache().max_bytes() == DEFAULT_BLOCK_METADATA_CACHE_MAX);

        let bounded = BlockStore::new(store, base).with_metadata_cache_max(krabka_units::bytes(64));
        assert2::assert!(bounded.metadata_cache().max_bytes() == krabka_units::bytes(64));
    }

    /// Clones share the caches. A querier clones a `BlockStore` per request,
    /// and a cache that died with the clone would never be warm.
    #[tokio::test]
    async fn clones_share_the_footer_cache() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (bs, keys) = two_block_store(store).await;
        bs.scan_block_keys(&keys, log_schema()).await.unwrap();

        let clone = bs.clone();
        assert2::assert!(clone.metadata_cache().len() == bs.metadata_cache().len());
        assert2::assert!(!clone.metadata_cache().is_empty());
        assert2::assert!(!bs.empty_like().metadata_cache().is_empty());
    }

    #[tokio::test]
    async fn scan_with_no_matching_blocks_returns_empty_shape() {
        let (bs, schema) = seeded_store().await;
        let matchers = [LabelMatcher::new("app", MatchOp::Eq, "absent")];

        let (ctx, table) = bs
            .scan_context("t", &matchers, 0, 1_000, schema)
            .await
            .unwrap();
        let df = ctx.sql(&format!("SELECT line FROM {table}")).await.unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert2::assert!(total == 0);
    }
}

mod block_probe_concurrency;
mod block_scan;
mod block_store;
mod max_fingerprint_in_list;
mod probe_blocks;
mod require_blocks;
mod scan_filter;
mod scan_report;
mod scan_table_request;
mod table_name;

use block_probe_concurrency::BLOCK_PROBE_CONCURRENCY;
pub use block_scan::BlockScan;
pub use block_store::BlockStore;
use max_fingerprint_in_list::MAX_FINGERPRINT_IN_LIST;
use probe_blocks::probe_blocks;
use require_blocks::require_blocks;
use scan_filter::scan_filter;
pub use scan_report::ScanReport;
pub use scan_table_request::ScanTableRequest;
use table_name::TABLE_NAME;
