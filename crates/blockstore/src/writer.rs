//! Writes columnar blocks to object storage as Parquet.

use std::{cmp::Ordering, collections::BTreeSet, sync::Arc};

use arrow::{
    array::{
        Array, ArrayRef, DynComparator, FixedSizeBinaryArray, Int64Array, UInt32Array, UInt64Array,
        make_comparator,
    },
    compute::{
        SortColumn, SortOptions, concat_batches, lexsort_to_indices, take, take_record_batch,
    },
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use object_store::{ObjectStore, buffered::BufWriter, path::Path};
use parquet::{
    arrow::{ArrowSchemaConverter, AsyncArrowWriter},
    basic::{Compression, ZstdLevel},
    file::{metadata::SortingColumn, properties::WriterProperties},
    schema::types::{ColumnPath, SchemaDescriptor},
};
use tracing::{debug, instrument};

use crate::{
    block::{BlockMeta, COL_FINGERPRINT, COL_TIMESTAMP, validate_against},
    block_index::{BlockSchema, series_block_schema},
    compaction::BlockLevel,
    error::{BlockStoreError, Result},
    labels::SeriesFingerprint,
};

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use arrow::{
        array::{Int64Array, StringArray, UInt64Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart,
        memory::InMemory,
        path::{Path, Path as ObjectPath},
    };
    use parquet::{
        arrow::arrow_reader::ParquetRecordBatchReaderBuilder,
        file::{
            properties::ReaderProperties,
            reader::FileReader,
            serialized_reader::{ReadOptionsBuilder, SerializedFileReader},
        },
    };

    use super::*;
    use crate::{
        block_index::RequiredColumn,
        reader::read_block,
        span_schema::{SCOL_SPAN_ID, SCOL_START_NANO, SCOL_TRACE_ID, span_block_decl},
    };

    fn log_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new(crate::COL_FINGERPRINT, DataType::UInt64, false),
            Field::new(crate::COL_TIMESTAMP, DataType::Int64, false),
            Field::new("line", DataType::Utf8, true),
        ]))
    }

    fn sample_batch(schema: &Arc<Schema>) -> RecordBatch {
        let fp = UInt64Array::from(vec![10_u64, 10, 20, 20]);
        let ts = Int64Array::from(vec![100_i64, 200, 300, 400]);
        let line = StringArray::from(vec!["a", "b", "c", "d"]);
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(fp), Arc::new(ts), Arc::new(line)],
        )
        .unwrap()
    }

    #[derive(Debug)]
    struct AbortStore {
        inner: InMemory,
        aborted: Arc<AtomicBool>,
    }

    impl std::fmt::Display for AbortStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("AbortStore")
        }
    }

    #[async_trait]
    impl ObjectStore for AbortStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            Ok(Box::new(AbortUpload {
                inner: self.inner.put_multipart_opts(location, options).await?,
                aborted: Arc::clone(&self.aborted),
            }))
        }

        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
            self.inner.delete_stream(locations)
        }
    }

    #[derive(Debug)]
    struct AbortUpload {
        inner: Box<dyn MultipartUpload>,
        aborted: Arc<AtomicBool>,
    }

    #[async_trait]
    impl MultipartUpload for AbortUpload {
        fn put_part(&mut self, data: PutPayload) -> UploadPart {
            self.inner.put_part(data)
        }

        async fn complete(&mut self) -> object_store::Result<PutResult> {
            self.inner.complete().await
        }

        async fn abort(&mut self) -> object_store::Result<()> {
            self.aborted.store(true, Ordering::Relaxed);
            self.inner.abort().await
        }
    }

    #[tokio::test]
    async fn write_block_persists_object_and_returns_meta() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = BlockWriter::new(store.clone());
        let schema = log_schema();
        let batch = sample_batch(&schema);

        let meta = writer
            .write_block("tenant-a", "blocks/tenant-a/b1.parquet", schema, &[batch])
            .await
            .unwrap();

        let mut meta = meta;
        meta.fingerprints.sort_unstable();
        assert2::assert!(
            meta == BlockMeta {
                tenant: "tenant-a".to_string(),
                object_key: "blocks/tenant-a/b1.parquet".to_string(),
                min_ts: 100,
                max_ts: 400,
                row_count: 4,
                fingerprints: vec![10, 20],
                level: BlockLevel::INGESTED,
            }
        );

        let head = store.head(&Path::from("blocks/tenant-a/b1.parquet")).await;
        assert2::assert!(head.is_ok());
    }

    fn span_summary_batch() -> RecordBatch {
        use arrow::array::FixedSizeBinaryArray;

        let schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::FixedSizeBinary(16), false),
            Field::new("start_unix_nano", DataType::Int64, false),
        ]));
        let ids: Vec<[u8; 16]> = vec![[1_u8; 16], [2_u8; 16]];
        let trace_id =
            FixedSizeBinaryArray::try_from_iter(ids.iter().map(<[u8; 16]>::as_slice)).unwrap();
        let ts = Int64Array::from(vec![100_i64, 200]);
        RecordBatch::try_new(schema, vec![Arc::new(trace_id), Arc::new(ts)]).unwrap()
    }

    /// The summary of `batches`, folded one batch at a time exactly as the
    /// writer folds them.
    fn summarize(
        batches: &[RecordBatch],
        columns: &SummaryColumns,
    ) -> Result<(i64, i64, usize, Vec<SeriesFingerprint>)> {
        let mut summary = BlockSummary::new();
        for batch in batches {
            summary.push(batch, columns)?;
        }
        summary.finish()
    }

    #[test]
    fn summarize_skips_fingerprints_for_span_blocks() {
        // Span (FixedSizeBinary id) blocks never read `meta.fingerprints`, so
        // the per-row FNV pass should be skipped and the set left empty. Time
        // bounds and row count must still be summarized.
        let batch = span_summary_batch();
        let (min_ts, max_ts, row_count, fps) = summarize(
            &[batch],
            &SummaryColumns::new("trace_id", "start_unix_nano"),
        )
        .unwrap();
        assert2::assert!(min_ts == 100);
        assert2::assert!(max_ts == 200);
        assert2::assert!(row_count == 2);
        assert2::assert!(fps.is_empty());
    }

    #[test]
    fn summarize_still_fingerprints_series_blocks() {
        let schema = log_schema();
        let batch = sample_batch(&schema);
        let (_min, _max, _rows, mut fps) = summarize(&[batch], &SummaryColumns::series()).unwrap();
        fps.sort_unstable();
        assert2::assert!(fps == vec![10_u64, 20]);
    }

    /// A block summarized in pieces is the block summarized whole: the same
    /// bounds, the same row count, the same fingerprints. The streaming
    /// writer's `BlockMeta` is only trustworthy if that holds, and a bound
    /// that came out too narrow would drop the block from a query's range
    /// rather than fail anywhere visible.
    #[test]
    fn a_summary_folded_batch_by_batch_matches_one_taken_over_the_whole_block() {
        let schema = series_schema();
        let split = (0..7_i64)
            .map(|group| {
                let fp = UInt64Array::from_iter_values(
                    (0..3_u64).map(|row| u64::try_from(group).expect("a group index") * 3 + row),
                );
                let ts = Int64Array::from_iter_values((0..3_i64).map(|row| group * 3 + row - 4));
                RecordBatch::try_new(schema.clone(), vec![Arc::new(fp), Arc::new(ts)]).unwrap()
            })
            .collect::<Vec<_>>();
        let whole = arrow::compute::concat_batches(&schema, &split).unwrap();

        assert2::assert!(
            summarize(&split, &SummaryColumns::series()).unwrap()
                == summarize(std::slice::from_ref(&whole), &SummaryColumns::series()).unwrap()
        );
    }

    /// A block with no rows has no time bounds to prune by, so it is an error
    /// rather than an object nothing can query.
    #[test]
    fn a_summary_of_no_rows_at_all_is_an_error() {
        let empty: Vec<RecordBatch> = Vec::new();
        assert2::assert!(matches!(
            summarize(&empty, &SummaryColumns::series()),
            Err(BlockStoreError::InvalidBlock(message)) if message == "empty block"
        ));
    }

    #[tokio::test]
    async fn write_block_rejects_schema_without_mandatory_columns() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = BlockWriter::new(store);
        let schema = Arc::new(Schema::new(vec![Field::new("line", DataType::Utf8, true)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(vec!["x"]))])
                .unwrap();

        let err = writer.write_block("t", "k.parquet", schema, &[batch]).await;
        assert2::assert!(err.is_err());
    }

    #[tokio::test]
    async fn write_block_rejects_batch_schema_mismatch() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = BlockWriter::new(store);
        let schema = log_schema();
        let batch_schema = Arc::new(Schema::new(vec![
            Field::new(crate::COL_TIMESTAMP, DataType::Int64, false),
            Field::new(crate::COL_FINGERPRINT, DataType::UInt64, false),
            Field::new("line", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            batch_schema,
            vec![
                Arc::new(Int64Array::from(vec![100_i64])),
                Arc::new(UInt64Array::from(vec![10_u64])),
                Arc::new(StringArray::from(vec!["x"])),
            ],
        )
        .unwrap();

        let err = writer.write_block("t", "k.parquet", schema, &[batch]).await;

        assert2::assert!(
            matches!(err, Err(BlockStoreError::InvalidBlock(message)) if message.contains("schema"))
        );
    }

    /// The two mandatory columns and nothing else, for the cases that care
    /// about how a block is written rather than what it carries.
    fn series_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new(crate::COL_FINGERPRINT, DataType::UInt64, false),
            Field::new(crate::COL_TIMESTAMP, DataType::Int64, false),
        ]))
    }

    /// `rows` rows in the declared order: ten samples of each fingerprint.
    fn sorted_series_batch(rows: usize) -> RecordBatch {
        let rows = u64::try_from(rows).expect("a row count fits a u64");
        let fp = UInt64Array::from_iter_values((0..rows).map(|row| row / 10));
        let ts = Int64Array::from_iter_values(
            (0..rows).map(|row| i64::try_from(row % 10).expect("a sample index fits an i64")),
        );
        RecordBatch::try_new(series_schema(), vec![Arc::new(fp), Arc::new(ts)])
            .expect("the columns match the schema")
    }

    /// The bytes of the block at `object_key`.
    async fn block_bytes(store: &Arc<dyn ObjectStore>, object_key: &str) -> Bytes {
        store
            .get(&Path::from(object_key))
            .await
            .expect("the block was written")
            .bytes()
            .await
            .expect("the block reads back")
    }

    /// A row group's `sorting_columns` as plain tuples, which compare without
    /// depending on how the Parquet crate derives equality for its own type.
    fn sorting_of(properties: &WriterProperties) -> Option<Vec<(i32, bool, bool)>> {
        properties.sorting_columns().map(|columns| {
            columns
                .iter()
                .map(|column| (column.column_idx, column.descending, column.nulls_first))
                .collect()
        })
    }

    #[tokio::test]
    async fn write_block_cuts_row_groups_at_the_declared_size() {
        // One row past two full groups, so a writer that ignored the setting
        // (one group of everything) and one that was off by a group are both
        // distinguishable from the right answer.
        let rows = BLOCK_ROW_GROUP_ROWS * 2 + 1;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = BlockWriter::new(store.clone());

        writer
            .write_block(
                "t",
                "k.parquet",
                series_schema(),
                &[sorted_series_batch(rows)],
            )
            .await
            .unwrap();

        let bytes = block_bytes(&store, "k.parquet").await;
        let metadata = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .metadata()
            .clone();
        let group_rows = metadata
            .row_groups()
            .iter()
            .map(|group| usize::try_from(group.num_rows()).unwrap())
            .collect::<Vec<_>>();
        assert2::assert!(group_rows == vec![BLOCK_ROW_GROUP_ROWS, BLOCK_ROW_GROUP_ROWS, 1]);
    }

    #[tokio::test]
    async fn write_block_compresses_every_column_and_records_the_declared_order() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = BlockWriter::new(store.clone());
        let schema = log_schema();

        writer
            .write_block("t", "k.parquet", schema.clone(), &[sample_batch(&schema)])
            .await
            .unwrap();

        let bytes = block_bytes(&store, "k.parquet").await;
        let metadata = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .metadata()
            .clone();
        let group = &metadata.row_groups()[0];

        let zstd = parquet::basic::Compression::ZSTD(ZstdLevel::try_new(BLOCK_ZSTD_LEVEL).unwrap());
        let codecs = group
            .columns()
            .iter()
            .map(parquet::file::metadata::ColumnChunkMetaData::compression)
            .collect::<Vec<_>>();
        assert2::assert!(codecs == vec![zstd, zstd, zstd]);

        let sorting = group.sorting_columns().map(|columns| {
            columns
                .iter()
                .map(|column| (column.column_idx, column.descending, column.nulls_first))
                .collect::<Vec<_>>()
        });
        assert2::assert!(sorting == Some(vec![(0, false, true), (1, false, true)]));
    }

    /// A span-shaped block: two identity columns, one of which leads the sort
    /// key and one of which is scattered through it.
    fn span_shaped_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new(SCOL_TRACE_ID, DataType::FixedSizeBinary(16), false),
            Field::new(SCOL_SPAN_ID, DataType::FixedSizeBinary(8), false),
            Field::new(SCOL_START_NANO, DataType::Int64, false),
        ]))
    }

    fn span_shaped_batch(traces: &[u8], spans: &[u8]) -> RecordBatch {
        let trace_ids = FixedSizeBinaryArray::try_from_iter(
            traces
                .iter()
                .map(|byte| [*byte; 16])
                .collect::<Vec<_>>()
                .iter()
                .map(<[u8; 16]>::as_slice),
        )
        .unwrap();
        let span_ids = FixedSizeBinaryArray::try_from_iter(
            spans
                .iter()
                .map(|byte| [*byte; 8])
                .collect::<Vec<_>>()
                .iter()
                .map(<[u8; 8]>::as_slice),
        )
        .unwrap();
        let start =
            Int64Array::from_iter_values((0..spans.len()).map(|row| i64::try_from(row).unwrap()));
        RecordBatch::try_new(
            span_shaped_schema(),
            vec![Arc::new(trace_ids), Arc::new(span_ids), Arc::new(start)],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn write_block_blooms_the_declared_identity_column_and_nothing_else() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = BlockWriter::new(store.clone());
        let schema = span_shaped_schema();
        let decl = BlockSchema {
            required: vec![
                RequiredColumn::new(SCOL_TRACE_ID, DataType::FixedSizeBinary(16), false),
                RequiredColumn::new(SCOL_START_NANO, DataType::Int64, false),
            ],
            sort_key: vec![SCOL_TRACE_ID.to_string(), SCOL_START_NANO.to_string()],
            bloom_columns: vec![SCOL_SPAN_ID.to_string()],
        };

        writer
            .write_block_with_decl(
                "t",
                "k.parquet",
                schema,
                &[span_shaped_batch(&[1, 1, 2], &[7, 8, 9])],
                &decl,
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();

        let bytes = block_bytes(&store, "k.parquet").await;
        let options = ReadOptionsBuilder::new()
            .with_reader_properties(
                ReaderProperties::builder()
                    .set_read_bloom_filter(true)
                    .build(),
            )
            .build();
        let reader = SerializedFileReader::new_with_options(bytes, options).unwrap();
        let group = reader.get_row_group(0).unwrap();

        let bloom = group
            .get_column_bloom_filter(1)
            .expect("the span id column carries a bloom filter");
        // The span ids the batch holds, and one it does not. A bloom filter
        // may say yes to an absent value, but not to this few.
        assert2::assert!(bloom.check(&[7_u8; 8][..]));
        assert2::assert!(bloom.check(&[9_u8; 8][..]));
        assert2::assert!(!bloom.check(&[200_u8; 8][..]));

        // The trace id leads the sort key and the start is its second key;
        // row-group min/max prunes both, so neither should pay for a filter.
        assert2::assert!(group.get_column_bloom_filter(0).is_none());
        assert2::assert!(group.get_column_bloom_filter(2).is_none());
    }

    #[tokio::test]
    async fn write_block_sorts_rows_the_caller_left_out_of_declared_order() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = BlockWriter::new(store.clone());
        let schema = log_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![20_u64, 10, 20, 10])),
                Arc::new(Int64Array::from(vec![300_i64, 100, 400, 200])),
                Arc::new(StringArray::from(vec!["c", "a", "d", "b"])),
            ],
        )
        .unwrap();

        writer
            .write_block("t", "k.parquet", schema.clone(), &[batch])
            .await
            .unwrap();

        let batches = read_block(store, "k.parquet").await.unwrap();
        let written = arrow::compute::concat_batches(&schema, &batches).unwrap();
        assert2::assert!(
            written
                == RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(UInt64Array::from(vec![10_u64, 10, 20, 20])),
                        Arc::new(Int64Array::from(vec![100_i64, 200, 300, 400])),
                        Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
                    ],
                )
                .unwrap()
        );
    }

    #[tokio::test]
    async fn sorting_keeps_the_order_of_rows_the_declared_key_does_not_separate() {
        // Traces order spans within a trace by a key their declaration does
        // not name, so a sort that reshuffled equal keys would silently
        // reorder them. Two rows share a fingerprint and a timestamp here and
        // must come back in the order they were handed over.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = BlockWriter::new(store.clone());
        let schema = log_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![20_u64, 10, 10])),
                Arc::new(Int64Array::from(vec![1_i64, 5, 5])),
                Arc::new(StringArray::from(vec!["x", "first", "second"])),
            ],
        )
        .unwrap();

        writer
            .write_block("t", "k.parquet", schema.clone(), &[batch])
            .await
            .unwrap();

        let batches = read_block(store, "k.parquet").await.unwrap();
        let written = arrow::compute::concat_batches(&schema, &batches).unwrap();
        assert2::assert!(
            written
                == RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(UInt64Array::from(vec![10_u64, 10, 20])),
                        Arc::new(Int64Array::from(vec![5_i64, 5, 1])),
                        Arc::new(StringArray::from(vec!["first", "second", "x"])),
                    ],
                )
                .unwrap()
        );
    }

    #[tokio::test]
    async fn a_block_split_across_batches_is_ordered_across_the_boundary_too() {
        // Each batch is sorted on its own, but the second starts below where
        // the first ended. A check that only looked within a batch would call
        // this sorted and record a `sorting_columns` the file does not honour.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = BlockWriter::new(store.clone());
        let schema = series_schema();
        let first = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![30_u64, 40])),
                Arc::new(Int64Array::from(vec![1_i64, 2])),
            ],
        )
        .unwrap();
        let second = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![10_u64, 20])),
                Arc::new(Int64Array::from(vec![3_i64, 4])),
            ],
        )
        .unwrap();

        writer
            .write_block("t", "k.parquet", schema.clone(), &[first, second])
            .await
            .unwrap();

        let batches = read_block(store, "k.parquet").await.unwrap();
        let written = arrow::compute::concat_batches(&schema, &batches).unwrap();
        assert2::assert!(
            written
                == RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(UInt64Array::from(vec![10_u64, 20, 30, 40])),
                        Arc::new(Int64Array::from(vec![3_i64, 4, 1, 2])),
                    ],
                )
                .unwrap()
        );
    }

    /// The streaming path exists to spend less memory, not to write a
    /// different block. Handed the same rows in the same order, it must
    /// produce the same rows back and the same `BlockMeta` -- bounds, row
    /// count and fingerprints -- as the buffered path, because the index
    /// entry a compaction records is that metadata.
    #[tokio::test]
    async fn a_streamed_block_holds_what_the_buffered_one_would() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = BlockWriter::new(store.clone());
        let schema = series_schema();
        let batches = (0..4_u64)
            .map(|group| {
                let fp = UInt64Array::from_iter_values((0..5).map(|row| group * 5 + row));
                let ts = Int64Array::from_iter_values(
                    (0..5).map(|row| i64::try_from(group * 5 + row).unwrap()),
                );
                RecordBatch::try_new(schema.clone(), vec![Arc::new(fp), Arc::new(ts)]).unwrap()
            })
            .collect::<Vec<_>>();

        let buffered = writer
            .write_block("t", "buffered.parquet", schema.clone(), &batches)
            .await
            .unwrap();

        let mut block = writer
            .open_block(
                "t",
                "streamed.parquet",
                schema.clone(),
                &series_block_schema(),
                SummaryColumns::series(),
            )
            .unwrap();
        for batch in &batches {
            block.write_batch(batch).await.unwrap();
        }
        let streamed = block.finish().await.unwrap();

        assert2::assert!(
            streamed
                == BlockMeta {
                    object_key: "streamed.parquet".to_string(),
                    ..buffered
                }
        );

        let read_back = |key: &'static str| {
            let store = store.clone();
            let schema = schema.clone();
            async move {
                let batches = read_block(store, key).await.unwrap();
                arrow::compute::concat_batches(&schema, &batches).unwrap()
            }
        };
        assert2::assert!(
            read_back("streamed.parquet").await == read_back("buffered.parquet").await
        );
    }

    /// A streaming writer cannot sort rows it has already encoded, so a caller
    /// that breaks the declared order is refused rather than quietly given a
    /// block whose `sorting_columns` lies about it. The violation is across a
    /// batch boundary, which is the case a per-batch check would miss.
    #[tokio::test]
    async fn a_streaming_caller_that_breaks_the_declared_order_is_refused() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = BlockWriter::new(store.clone());
        let schema = series_schema();
        let batch = |fingerprints: Vec<u64>| {
            let ts = Int64Array::from_iter_values(fingerprints.iter().map(|_| 1_i64));
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(UInt64Array::from(fingerprints)), Arc::new(ts)],
            )
            .unwrap()
        };

        let mut block = writer
            .open_block(
                "t",
                "unsorted.parquet",
                schema.clone(),
                &series_block_schema(),
                SummaryColumns::series(),
            )
            .unwrap();
        block.write_batch(&batch(vec![30, 40])).await.unwrap();
        let rejected = block.write_batch(&batch(vec![10, 20])).await;

        assert2::assert!(
            matches!(&rejected, Err(BlockStoreError::InvalidBlock(message))
                if message.contains("out of the declared sort order"))
        );
        // Once refused, the block stays refused: a caller that ignored the
        // error must not be able to close a mis-sorted block anyway.
        assert2::assert!(block.finish().await.is_err());
        assert2::assert!(read_block(store, "unsorted.parquet").await.is_err());
    }

    /// Rows out of order *within* one batch are the same lie about
    /// `sorting_columns`, and are refused on the same terms.
    #[tokio::test]
    async fn a_streaming_caller_is_refused_for_disorder_inside_one_batch() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = BlockWriter::new(store);
        let schema = series_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![10_u64, 10])),
                Arc::new(Int64Array::from(vec![9_i64, 8])),
            ],
        )
        .unwrap();

        let mut block = writer
            .open_block(
                "t",
                "unsorted.parquet",
                schema,
                &series_block_schema(),
                SummaryColumns::series(),
            )
            .unwrap();

        assert2::assert!(block.write_batch(&batch).await.is_err());
    }

    #[tokio::test]
    async fn a_failed_streaming_write_aborts_its_multipart_upload() {
        let aborted = Arc::new(AtomicBool::new(false));
        let store: Arc<dyn ObjectStore> = Arc::new(AbortStore {
            inner: InMemory::new(),
            aborted: Arc::clone(&aborted),
        });
        let schema = series_schema();
        let decl = series_block_schema();
        let object_writer = BufWriter::with_capacity(store, Path::from("failed.parquet"), 1);
        let parquet_writer = AsyncArrowWriter::try_new(
            object_writer,
            schema.clone(),
            Some(block_writer_properties(&schema, &decl).unwrap()),
        )
        .unwrap();
        let mut block = BlockStreamWriter::new(
            parquet_writer,
            "t",
            "failed.parquet",
            schema.clone(),
            SummaryColumns::series(),
            Some(SortKeyCheck::new(&decl.sort_key)),
            &decl.sort_key,
        );
        let fingerprints = UInt64Array::from_iter_values(0..BLOCK_ROW_GROUP_ROWS as u64);
        let timestamps = Int64Array::from_iter_values((0..BLOCK_ROW_GROUP_ROWS).map(|_| 1));
        let full_group =
            RecordBatch::try_new(schema, vec![Arc::new(fingerprints), Arc::new(timestamps)])
                .unwrap();
        block.write_batch(&full_group).await.unwrap();

        assert2::assert!(
            block
                .write_batch(&sample_batch(&log_schema()))
                .await
                .is_err()
        );
        assert2::assert!(aborted.load(Ordering::Relaxed));
    }

    /// A stream that never carried a row leaves no object behind: an empty
    /// block has no time bounds to prune by, and an index entry pointing at
    /// one would send every query that overlaps it to a file with nothing in.
    #[tokio::test]
    async fn a_stream_with_no_rows_writes_no_block() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = BlockWriter::new(store.clone());
        let schema = series_schema();
        let empty = RecordBatch::new_empty(schema.clone());

        let mut block = writer
            .open_block(
                "t",
                "empty.parquet",
                schema,
                &series_block_schema(),
                SummaryColumns::series(),
            )
            .unwrap();
        block.write_batch(&empty).await.unwrap();

        assert2::assert!(
            matches!(block.finish().await, Err(BlockStoreError::InvalidBlock(message))
                if message == "empty block")
        );
        assert2::assert!(store.head(&Path::from("empty.parquet")).await.is_err());
    }

    #[test]
    fn sorting_columns_index_parquet_leaves_rather_than_arrow_fields() {
        // One nested Arrow field ahead of the sort key contributes two
        // Parquet leaves, so the fingerprint's column chunk is the third and
        // not the second. Numbering the sort key by Arrow field index would
        // name the wrong columns here.
        let nested = DataType::Struct(
            vec![
                Field::new("name", DataType::Utf8, true),
                Field::new("at", DataType::Int64, true),
            ]
            .into(),
        );
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("events", nested, true),
            Field::new(crate::COL_FINGERPRINT, DataType::UInt64, false),
            Field::new(crate::COL_TIMESTAMP, DataType::Int64, false),
        ]));

        let properties = block_writer_properties(&schema, &series_block_schema()).unwrap();

        assert2::assert!(sorting_of(&properties) == Some(vec![(2, false, true), (3, false, true)]));
    }

    #[test]
    fn properties_reject_a_declaration_naming_a_column_the_schema_lacks() {
        let schema: SchemaRef = series_schema();
        let decl = BlockSchema {
            required: vec![RequiredColumn::new(
                crate::COL_FINGERPRINT,
                DataType::UInt64,
                false,
            )],
            sort_key: vec![crate::COL_FINGERPRINT.to_string()],
            bloom_columns: vec!["not_a_column".to_string()],
        };

        let err = block_writer_properties(&schema, &decl);

        assert2::assert!(
            matches!(err, Err(BlockStoreError::InvalidBlock(message)) if message.contains("not_a_column"))
        );
    }

    #[test]
    fn the_span_declaration_blooms_the_span_id_alone() {
        // `span_id` is not part of the span sort key, so it reaches the
        // writer only through the declaration's bloom list, and the trace id
        // that leads the sort key must not also be paying for a filter.
        let schema = crate::span_schema::span_block_schema();
        let decl = span_block_decl();

        let properties = block_writer_properties(&schema, &decl).unwrap();

        assert2::assert!(decl.bloom_columns == vec![SCOL_SPAN_ID]);
        let trace_id = schema.index_of(SCOL_TRACE_ID).unwrap();
        let start = schema.index_of(SCOL_START_NANO).unwrap();
        assert2::assert!(
            sorting_of(&properties)
                == Some(vec![
                    (i32::try_from(trace_id).unwrap(), false, true),
                    (i32::try_from(start).unwrap(), false, true),
                ])
        );
    }
}

mod block_row_group_rows;
mod block_stream_writer;
mod block_summary;
mod block_writer;
mod block_writer_properties;
mod block_zstd_level;
mod is_sorted_by_key;
mod key_columns;
mod sort_batches_by_key;
mod sort_key_check;
mod sort_key_options;
mod summary_columns;
mod validate_batch_schema;
mod validate_batch_schemas;

pub use block_row_group_rows::BLOCK_ROW_GROUP_ROWS;
pub use block_stream_writer::BlockStreamWriter;
use block_summary::BlockSummary;
pub use block_writer::BlockWriter;
pub use block_writer_properties::block_writer_properties;
pub use block_zstd_level::BLOCK_ZSTD_LEVEL;
use is_sorted_by_key::is_sorted_by_key;
use key_columns::key_columns;
use sort_batches_by_key::sort_batches_by_key;
use sort_key_check::SortKeyCheck;
pub(crate) use sort_key_options::SORT_KEY_OPTIONS;
pub use summary_columns::SummaryColumns;
use validate_batch_schema::validate_batch_schema;
use validate_batch_schemas::validate_batch_schemas;
